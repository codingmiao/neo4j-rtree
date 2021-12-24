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
package org.wowtools.neo4j.rtree.geometry2dold;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.neo4j.graphdb.*;
import org.wowtools.neo4j.rtree.geometry2dold.spatial.Envelope;
import org.wowtools.neo4j.rtree.geometry2dold.spatial.EnvelopeDecoder;
import org.wowtools.neo4j.rtree.geometry2dold.spatial.RTreeIndex;
import org.wowtools.neo4j.rtree.geometry2dold.util.GeometryBbox;

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
     * @param geometryCacheSize geometry缓存数量，由于对外wkb缓存转到堆内并转geometry比较耗时，故这里加了一个lru的缓存以直接获取geometry
     * @return RTreeIndex
     */
    public static synchronized RTreeIndex createIndex(GraphDatabaseService database, String indexName, String geometryFieldName, int maxNodeReferences, int geometryCacheSize) {
        //判断索引名是否唯一
        try (Transaction tx = database.beginTx()) {
            tx.findNodes(Constant.RtreeLabel.ReferenceNode, Constant.RtreeProperty.indexName, indexName).forEachRemaining((node) -> {
                throw new RuntimeException("index name(" + indexName + ") has been taken");
            });
        }
        return new RTreeIndex(indexName, geometryFieldName, database, new MyEnvelopeDecoder(geometryFieldName), maxNodeReferences, geometryCacheSize, true);
    }

    /**
     * 获取索引
     *
     * @param database          db
     * @param indexName         索引名
     * @param geometryCacheSize geometry缓存数量，由于对外wkb缓存转到堆内并转geometry比较耗时，故这里加了一个lru的缓存以直接获取geometry，可以和构造索引时不一致
     * @return RTreeIndex
     */
    public static synchronized RTreeIndex getIndex(GraphDatabaseService database, String indexName, int geometryCacheSize) {
        try (Transaction tx = database.beginTx()) {
            Node rootNode = tx.findNode(Constant.RtreeLabel.ReferenceNode, Constant.RtreeProperty.indexName, indexName);
            if (null == rootNode) {
                throw new RuntimeException("index (" + indexName + ") is nonexistent");
            }
            String geometryFieldName = (String) rootNode.getProperty(Constant.RtreeProperty.geometryFieldName);
            int maxNodeReferences = (int) rootNode.getProperty(Constant.RtreeProperty.maxNodeReferences);
            return new RTreeIndex(indexName, geometryFieldName, database, new MyEnvelopeDecoder(geometryFieldName), maxNodeReferences, geometryCacheSize, false);
        }
    }

    /**
     * 获取指定名称的空间索引，若没有，则新建
     *
     * @param database          db
     * @param indexName         索引名(唯一)
     * @param geometryFieldName node中的geometry字段名，若已有索引，可能会和输入值不一致
     * @param maxNodeReferences 每个树节点上最多挂几个节点，若已有索引，可能会和输入值不一致
     * @param geometryCacheSize geometry缓存数量，由于对外wkb缓存转到堆内并转geometry比较耗时，故这里加了一个lru的缓存以直接获取geometry，可以和构造索引时不一致
     * @return RTreeIndex
     */
    public static synchronized RTreeIndex getOrCreateIndex(GraphDatabaseService database, String indexName, String geometryFieldName, int maxNodeReferences, int geometryCacheSize) {
        Node rootNode;
        try (Transaction tx = database.beginTx()) {
            rootNode = tx.findNode(Constant.RtreeLabel.ReferenceNode, Constant.RtreeProperty.indexName, indexName);
            if (null != rootNode) {
                geometryFieldName = (String) rootNode.getProperty(Constant.RtreeProperty.geometryFieldName);
                maxNodeReferences = (int) rootNode.getProperty(Constant.RtreeProperty.maxNodeReferences);
            }
        }
        if (null == rootNode) {
            return createIndex(database, indexName, geometryFieldName, maxNodeReferences, geometryCacheSize);
        } else {
            return new RTreeIndex(indexName, geometryFieldName, database, new MyEnvelopeDecoder(geometryFieldName), maxNodeReferences, geometryCacheSize, false);
        }

    }

    /**
     * 删除索引
     *
     * @param database  db
     * @param indexName index name
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
        private final String geometryFieldName;

        public MyEnvelopeDecoder(String geometryFieldName) {
            this.geometryFieldName = geometryFieldName;
        }

        @Override
        public Envelope decodeEnvelope(Node node) {
            byte[] wkb = (byte[]) node.getProperty(geometryFieldName);
            Geometry geometry;
            try {
                geometry = new WKBReader().read(wkb);
            } catch (ParseException e) {
                throw new RuntimeException("pase wkb error ", e);
            }
            GeometryBbox.Bbox bbox = GeometryBbox.getBbox(geometry);

            Envelope envelope = new Envelope(bbox.xmin, bbox.xmax, bbox.ymin, bbox.ymax);
            return envelope;
        }
    }
}
