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

import static org.wowtools.neo4j.rtree.util.BboxIntersectUtil.bbox2Geometry;
import static org.wowtools.neo4j.rtree.util.BboxIntersectUtil.bboxIntersect;

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
            bbox = new double[]{bboxObj.xmin, bboxObj.ymin, bboxObj.xmax, bboxObj.ymax};
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
     * 查询与输入geometry相交的node，
     * <p>
     * 对于一些狭长的geometry，例如很长的一条线，bbox过滤几乎无效，此时用此方法效率更高
     *
     * @param tx         tx
     * @param rTreeIndex 索引
     * @param geometry   geometry
     * @param visitor    结果访问器
     */
    public static void queryByStripGeometryIntersects(Transaction tx, RTreeIndex rTreeIndex, Geometry geometry, NodeVisitor visitor) {
        final GeometryFactory gf = new GeometryFactory();
        traverseRtree(tx, rTreeIndex,
                (rtreeNode, nodeBbox) -> {
                    Geometry nodeGeo = bbox2Geometry(nodeBbox, gf);
                    return nodeGeo.intersects(geometry);
                },
                (objNode, objGeo) -> {
                    if (geometry.intersects(objGeo)) {
                        visitor.vist(objNode, objGeo);
                    }
                }
        );
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
        traverseRtree(tx, rTreeIndex,
                (rtreeNode, nodeBbox) -> bboxIntersect(bbox, nodeBbox),
                (objNode, geometry) -> {
                    if (spatialFilter.accept(geometry)) {
                        visitor.vist(objNode, geometry);
                    }
                }
        );

    }

    /**
     * 树节点访问器
     */
    @FunctionalInterface
    private interface RtreeNodeVisitor {
        /**
         * 访问树索引节点
         *
         * @param rtreeNode rtreeNode
         * @param nodeBbox  nodeBbox
         * @return 为false时跳过此节点的子节点
         */
        boolean vist(Node rtreeNode, double[] nodeBbox);
    }

    private static void traverseRtree(Transaction tx, RTreeIndex rTreeIndex, RtreeNodeVisitor rtreeNodeVisitor, NodeVisitor visitor) {
        WKBReader wkbReader = new WKBReader();
        Node rtreeNode = rTreeIndex.getIndexRoot(tx);
        Deque<Node> stack = new ArrayDeque<>();//辅助遍历的栈
        stack.push(rtreeNode);
        while (!stack.isEmpty()) {
            rtreeNode = stack.pop();
            //判断当前节点的bbox是否与输入bbox相交
            double[] nodeBbox = (double[]) rtreeNode.getProperty(Constant.RtreeProperty.bbox);
            if (!rtreeNodeVisitor.vist(rtreeNode, nodeBbox)) {
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
                    visitor.vist(objNode, geometry);
                }
            }

        }
    }


}
