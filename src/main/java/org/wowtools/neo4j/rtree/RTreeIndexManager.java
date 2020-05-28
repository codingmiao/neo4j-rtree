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
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.neo4j.graphdb.*;
import org.wowtools.neo4j.rtree.spatial.Envelope;
import org.wowtools.neo4j.rtree.spatial.EnvelopeDecoder;
import org.wowtools.neo4j.rtree.spatial.RTreeIndex;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * 空间索引管理
 *
 * @author liuyu
 * @date 2020/5/27
 */
public class RTreeIndexManager {

    /**
     * 新建空间索引
     *
     * @param database          db
     * @param indexName         索引名(唯一)
     * @param geometryFieldName node中的geometry字段名
     * @param maxNodeReferences 每个树节点上最多挂几个节点
     * @return
     */
    public static synchronized RTreeIndex createIndex(GraphDatabaseService database, String indexName, String geometryFieldName, int maxNodeReferences) {
        //判断索引名是否唯一
        try (Transaction tx = database.beginTx()) {
            tx.findNodes(Constant.RtreeLabel.ReferenceNode, Constant.RtreeProperty.indexName, indexName).forEachRemaining((node) -> {
                throw new RuntimeException("index name(" + indexName + ") has been taken");
            });
        }
        return new RTreeIndex(indexName, geometryFieldName, database, new MyEnvelopeDecoder(), maxNodeReferences, true);
    }

    /**
     * 获取索引
     *
     * @param database  db
     * @param indexName 索引名
     * @return
     */
    public static synchronized RTreeIndex getIndex(GraphDatabaseService database, String indexName) {
        try (Transaction tx = database.beginTx()) {
            Node rootNode = tx.findNode(Constant.RtreeLabel.ReferenceNode, Constant.RtreeProperty.indexName, indexName);
            if (null == rootNode) {
                throw new RuntimeException("index (" + indexName + ") is nonexistent");
            }
            String geometryFieldName = (String) rootNode.getProperty(Constant.RtreeProperty.geometryFieldName);
            int maxNodeReferences = (int) rootNode.getProperty(Constant.RtreeProperty.maxNodeReferences);
            return new RTreeIndex(indexName, geometryFieldName, database, new MyEnvelopeDecoder(), maxNodeReferences, false);
        }
    }

    /**
     * 获取指定名称的空间索引，若没有，则新建
     *
     * @param database          db
     * @param indexName         索引名(唯一)
     * @param geometryFieldName node中的geometry字段名，若已有索引，可能会和输入值不一致
     * @param maxNodeReferences 每个树节点上最多挂几个节点，若已有索引，可能会和输入值不一致
     * @return
     */
    public static synchronized RTreeIndex getOrCreateIndex(GraphDatabaseService database, String indexName, String geometryFieldName, int maxNodeReferences) {
        Node rootNode;
        try (Transaction tx = database.beginTx()) {
            rootNode = tx.findNode(Constant.RtreeLabel.ReferenceNode, Constant.RtreeProperty.indexName, indexName);
            if (null != rootNode) {
                geometryFieldName = (String) rootNode.getProperty(Constant.RtreeProperty.geometryFieldName);
                maxNodeReferences = (int) rootNode.getProperty(Constant.RtreeProperty.maxNodeReferences);
            }
        }
        if (null == rootNode) {
            return createIndex(database, indexName, geometryFieldName, maxNodeReferences);
        } else {
            return new RTreeIndex(indexName, geometryFieldName, database, new MyEnvelopeDecoder(), maxNodeReferences, false);
        }

    }

    /**
     * 删除索引
     *
     * @param database
     * @param indexName
     */
    public static synchronized void dropIndex(GraphDatabaseService database, String indexName) {
        try (Transaction tx = database.beginTx()) {
            Node rootNode = tx.findNode(Constant.RtreeLabel.ReferenceNode, Constant.RtreeProperty.indexName, indexName);
            if (null == rootNode) {
                return;
            }
            //删掉METADATA
            Relationship relation = rootNode.getSingleRelationship(Constant.Relationship.RTREE_METADATA, Direction.OUTGOING);
            relation.delete();
            relation.getEndNode().delete();
            //删掉索引节点
            relation = rootNode.getSingleRelationship(Constant.Relationship.RTREE_ROOT, Direction.OUTGOING);
            relation.delete();
            Node indexNode = relation.getEndNode();
            Deque<Node> stack = new ArrayDeque<>();//辅助遍历的栈
            stack.push(indexNode);
            while (!stack.isEmpty()) {
                indexNode = stack.pop();
                indexNode.getRelationships(Direction.OUTGOING, Constant.Relationship.RTREE_REFERENCE).forEach(relationship -> {
                    relationship.delete();
                });
                indexNode.getRelationships(Direction.OUTGOING, Constant.Relationship.RTREE_CHILD).forEach(relationship -> {
                    stack.push(relationship.getEndNode());
                    relationship.delete();
                });
            }
            //删掉rootNode
            rootNode.delete();
            tx.commit();
        }
    }


    private static final class MyEnvelopeDecoder implements EnvelopeDecoder {
        @Override
        public Envelope decodeEnvelope(Object o) {
            byte[] wkb = (byte[]) o;
            Geometry geometry;
            try {
                geometry = new WKBReader().read(wkb);
            } catch (ParseException e) {
                throw new RuntimeException("pase wkb error ", e);
            }
            Geometry bound = geometry.getEnvelope();
            Coordinate[] coords = bound.getCoordinates();
            double xmin, xmax, ymin, ymax;
            if (coords.length > 1) {
                xmin = Double.MAX_VALUE;
                ymin = Double.MAX_VALUE;
                xmax = Double.MIN_VALUE;
                ymax = Double.MIN_VALUE;
                for (Coordinate coordinate : coords) {
                    double x = coordinate.x;
                    double y = coordinate.y;
                    if (x < xmin) {
                        xmin = x;
                    }
                    if (y < ymin) {
                        ymin = y;
                    }
                    if (x > xmax) {
                        xmax = x;
                    }
                    if (y > ymax) {
                        ymax = y;
                    }
                }
            } else {
                Coordinate coord = geometry.getCoordinate();
                xmin = coord.x;
                ymin = coord.y;
                xmax = coord.x;
                ymax = coord.y;
            }

            Envelope envelope = new Envelope(xmin, xmax, ymin, ymax);
            return envelope;
        }
    }
}
