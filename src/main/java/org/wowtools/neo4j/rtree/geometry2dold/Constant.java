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
package org.wowtools.neo4j.rtree.geometry2dold;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

/**
 * 常量
 *
 * @author liuyu
 * @date 2020/5/27
 */
public class Constant {

    public static final class RtreeLabel {
        public static final Label ReferenceNode = Label.label("ReferenceNode");
    }

    public static final class RtreeProperty {
        public static final String indexName = "indexName";
        public static final String geometryFieldName = "geometryFieldName";
        public static final String maxNodeReferences = "maxNodeReferences";
        public static final String bbox = "bbox";
    }

    public static final class Relationship {
        /**
         * 根节点到METADATA的关系
         */
        public static final RelationshipType RTREE_METADATA = RelationshipType.withName("RTREE_METADATA");
        /**
         * 根节点到索引节点的关系
         */
        public static final RelationshipType RTREE_ROOT = RelationshipType.withName("RTREE_ROOT");
        /**
         * 索引节点到索引节点的关系
         */
        public static final RelationshipType RTREE_CHILD = RelationshipType.withName("RTREE_CHILD");
        /**
         * 索引节点到对象节点的关系
         */
        public static final RelationshipType RTREE_REFERENCE = RelationshipType.withName("RTREE_REFERENCE");

    }


}
