/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
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
import org.neo4j.graphdb.Transaction;
import org.wowtools.neo4j.rtree.spatial.RTreeIndex;

import java.util.ArrayDeque;
import java.util.Deque;

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
         * @param geometry 访问到的geometry
         */
        void vist(Node node, Geometry geometry);
    }

    /**
     * 查询与输入bbox相交的node
     *
     * @param tx         tx
     * @param rTreeIndex 索引
     * @param bbox       [xmin,ymin,xmax,ymax]
     * @param visitor 结果访问器
     */
    public static void queryByBbox(Transaction tx, RTreeIndex rTreeIndex, double[] bbox, NodeVisitor visitor) {
        RectangleIntersects bboxRectangleIntersects;
        {
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
                rtreeNode.getRelationships(Direction.OUTGOING, Constant.Relationship.RTREE_CHILD).forEach((relationship) -> {
                    Node child = relationship.getEndNode();
                    stack.push(child);
                });
            } else if (rtreeNode.hasRelationship(Direction.OUTGOING, Constant.Relationship.RTREE_REFERENCE)) {
                //若有下级对象节点，返回结果
                rtreeNode.getRelationships(Direction.OUTGOING, Constant.Relationship.RTREE_REFERENCE).forEach((relationship) -> {
                    Node objNode = relationship.getEndNode();
                    org.locationtech.jts.geom.Geometry geometry;
                    byte[] wkb = (byte[]) objNode.getProperty(rTreeIndex.getGeometryFieldName());
                    try {
                        geometry = wkbReader.read(wkb);
                    } catch (ParseException e) {
                        throw new RuntimeException("parse wkb error", e);
                    }
                    if (bboxRectangleIntersects.intersects(geometry)) {
                        visitor.vist(objNode, geometry);
                    }
                });
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

}
