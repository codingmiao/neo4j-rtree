/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * <p>
 * This file is part of Neo4j Spatial.
 * <p>
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.wowtools.neo4j.rtree.internal.define;

import org.neo4j.graphdb.RelationshipType;


/**
 * 关系
 */
public enum Relationships implements RelationshipType {

    /**
     * 树的描述信息指向树的根节点
     */
    RTREE_METADATA_TO_ROOT,

    /**
     * 树的父节点指向子节点
     */
    RTREE_PARENT_TO_CHILD,

    /**
     * 树的叶子节点到具体数据节点
     */
    RTREE_LEAF_TO_DATA
}
