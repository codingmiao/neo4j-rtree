/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wowtools.neo4j.rtree;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.operation.predicate.RectangleIntersects;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.wowtools.neo4j.rtree.spatial.RTreeIndex;
import org.wowtools.neo4j.rtree.util.GeometryBbox;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

/**
 * 利用rtree索引进行空间查询
 *
 * @author liuyu
 * @date 2020/5/27
 */
public class RtreeQuery {

    /**
     * 查询结果访问器，RtreeQuery类中的每个查询方法，在查到一条node时都会访问一次vist
     */
    @FunctionalInterface
    public interface NodeVisitor {
        /**
         * @param node     访问到的node
         * @param geometry 访问到node的geometry，由node的空间字段wkb转换而来，因为空间过滤时必须要转换一次，所以直接回传避免重复转换
         */
        void vist(Node node, Geometry geometry);
    }

    /**
     * 空间查询过滤器
     */
    public interface SpatialFilter {
        /**
         * 判断传入的geometry是否满足过滤器的要求
         *
         * @param geometry g
         * @return 是否满足过滤器的要求
         */
        boolean accept(Geometry geometry);

        /**
         * 返回空间bbox范围
         *
         * @return [xmin, ymin, xmax, ymax]
         */
        double[] getBbox();
    }

    /**
     * bbox空间过滤
     */
    public static class BboxSpatialFilter implements SpatialFilter {
        protected final RectangleIntersects bboxRectangleIntersects;
        protected final double[] bbox;

        /**
         * @param bbox [xmin,ymin,xmax,ymax]
         */
        public BboxSpatialFilter(double[] bbox) {
            this.bbox = bbox;
            GeometryFactory gf = new GeometryFactory();
            Coordinate c0 = new Coordinate(bbox[0], bbox[1]);
            Polygon bboxPolygon = gf.createPolygon(new Coordinate[]{
                    c0,
                    new Coordinate(bbox[2], bbox[1]),
                    new Coordinate(bbox[2], bbox[3]),
                    new Coordinate(bbox[0], bbox[3]),
                    c0
            });
            bboxRectangleIntersects = new RectangleIntersects(bboxPolygon);
        }

        @Override
        public boolean accept(Geometry geometry) {
            return bboxRectangleIntersects.intersects(geometry);
        }

        @Override
        public double[] getBbox() {
            return bbox;
        }
    }


    /**
     * geometry相交过滤
     */
    public static class GeometryIntersectsSpatialFilter implements SpatialFilter {
        protected final double[] bbox;
        protected final Geometry geometry;

        /**
         * @param geometry geometry
         */
        public GeometryIntersectsSpatialFilter(Geometry geometry) {
            GeometryBbox.Bbox bboxObj = GeometryBbox.getBbox(geometry);
            bbox = new double[]{bboxObj.xmin, bboxObj.xmax, bboxObj.ymin, bboxObj.ymax};
            this.geometry = geometry;
        }

        @Override
        public boolean accept(Geometry geometry) {
            return this.geometry.intersects(geometry);
        }

        @Override
        public double[] getBbox() {
            return bbox;
        }
    }

    /**
     * 查询与输入bbox相交的node
     *
     * @param tx         tx
     * @param rTreeIndex 索引
     * @param bbox       [xmin,ymin,xmax,ymax]
     * @param visitor    结果访问器
     */
    public static void queryByBbox(Transaction tx, RTreeIndex rTreeIndex, double[] bbox, NodeVisitor visitor) {
        BboxSpatialFilter filter = new BboxSpatialFilter(bbox);
        queryBySpatialFilter(tx, rTreeIndex, filter, visitor);
    }

    /**
     * 查询与输入geometry相交的node
     *
     * @param tx         tx
     * @param rTreeIndex 索引
     * @param geometry   geometry
     * @param visitor    结果访问器
     */
    public static void queryByGeometryIntersects(Transaction tx, RTreeIndex rTreeIndex, Geometry geometry, NodeVisitor visitor) {
        GeometryIntersectsSpatialFilter filter = new GeometryIntersectsSpatialFilter(geometry);
        queryBySpatialFilter(tx, rTreeIndex, filter, visitor);
    }

    /**
     * 查询满足输入空间过滤器的node
     *
     * @param tx            tx
     * @param rTreeIndex    索引
     * @param spatialFilter 空间查询过滤器
     * @param visitor       结果访问器
     */
    public static void queryBySpatialFilter(Transaction tx, RTreeIndex rTreeIndex, SpatialFilter spatialFilter, NodeVisitor visitor) {
        double[] bbox = spatialFilter.getBbox();
        WKBReader wkbReader = new WKBReader();
        Node rtreeNode = rTreeIndex.getIndexRoot(tx);
        Deque<Node> stack = new ArrayDeque<>();//辅助遍历的栈
        stack.push(rtreeNode);
        while (!stack.isEmpty()) {
            rtreeNode = stack.pop();
            //判断当前节点的bbox是否与输入bbox相交
            double[] nodeBbox = (double[]) rtreeNode.getProperty(Constant.RtreeProperty.bbox);

            if (!bboxIntersect(bbox, nodeBbox)) {
                continue;
            }
            if (rtreeNode.hasRelationship(Direction.OUTGOING, Constant.Relationship.RTREE_CHILD)) {
                //若有下级索引节点,下级索引节点入栈
                for (Relationship relationship : rtreeNode.getRelationships(Direction.OUTGOING, Constant.Relationship.RTREE_CHILD)) {
                    Node child = relationship.getEndNode();
                    stack.push(child);
                }
            } else if (rtreeNode.hasRelationship(Direction.OUTGOING, Constant.Relationship.RTREE_REFERENCE)) {
                //若有下级对象节点，返回结果
                for (Relationship relationship : rtreeNode.getRelationships(Direction.OUTGOING, Constant.Relationship.RTREE_REFERENCE)) {
                    Node objNode = relationship.getEndNode();
                    Geometry geometry;
                    byte[] wkb = (byte[]) objNode.getProperty(rTreeIndex.getGeometryFieldName());
                    try {
                        geometry = wkbReader.read(wkb);
                    } catch (ParseException e) {
                        throw new RuntimeException("parse wkb error", e);
                    }
                    if (spatialFilter.accept(geometry)) {
                        visitor.vist(objNode, geometry);
                    }
                }
            }

        }
    }

    /**
     * 判断两个矩形是否相交
     * https://blog.csdn.net/szfhy/article/details/49740191
     *
     * @param bbox1 [xmin,ymin,xmax,ymax]1
     * @param bbox2 [xmin,ymin,xmax,ymax]2
     * @return 是否相交
     */
    public static boolean bboxIntersect(double[] bbox1, double[] bbox2) {
        double x01 = bbox1[0], y01 = bbox1[1], x02 = bbox1[2], y02 = bbox1[3];
        double x11 = bbox2[0], y11 = bbox2[1], x12 = bbox2[2], y12 = bbox2[3];
        double zx = Math.abs(x01 + x02 - x11 - x12);
        double x = Math.abs(x01 - x02) + Math.abs(x11 - x12);
        double zy = Math.abs(y01 + y02 - y11 - y12);
        double y = Math.abs(y01 - y02) + Math.abs(y11 - y12);
        return (zx <= x && zy <= y);
    }


    /**
     * 判断点是否与bbox相交
     *
     * @param bbox [xmin,ymin,xmax,ymax]1
     * @param x    x
     * @param y    y
     * @return 是否相交
     */
    private static boolean pointInBbox(double[] bbox, double x, double y) {
        return bbox[0] <= x && x <= bbox[2]
                && bbox[1] <= y && y <= bbox[3];
    }

    /**
     * 最邻近搜索。查询与输入点最近的设备，最多返回n个
     *
     * @param tx         tx
     * @param rTreeIndex 索引
     * @param x          输入点x坐标
     * @param y          输入点y坐标
     * @param n          最大返回node数
     * @param visitor    结果访问器
     */
    public static void queryNearestN(Transaction tx, RTreeIndex rTreeIndex, double x, double y, int n, NodeVisitor visitor) {
        Node rtreeNode = rTreeIndex.getIndexRoot(tx);
        //判断rtree根节点的bbox是否与输入xy相交
        double[] nodeBbox = (double[]) rtreeNode.getProperty(Constant.RtreeProperty.bbox);
        if (!pointInBbox(nodeBbox, x, y)) {
            return;
        }
        /** 1、找到输入点所属的叶子索引node **/
        Node childNode = rtreeNode;
        do {
            rtreeNode = childNode;
            //若有下级索引节点,下级索引节点pointInBbox过滤入栈
            for (Relationship relationship : rtreeNode.getRelationships(Direction.OUTGOING, Constant.Relationship.RTREE_CHILD)) {
                Node child = relationship.getEndNode();
                nodeBbox = (double[]) child.getProperty(Constant.RtreeProperty.bbox);
                if (pointInBbox(nodeBbox, x, y)) {
                    childNode = child;
                    break;//点只会与一个bbox相交，所以可以直接break
                }
            }
        } while (childNode != rtreeNode);
        /** 2、从叶子索引node开始反向找上级索引节点并求距离 **/
        WKBReader wkbReader = new WKBReader();
        List<Node> possibleNodes = new LinkedList<>();//可能是最邻近的点
    }

}
