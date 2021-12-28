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
package org.wowtools.neo4j.rtree.geometry2dold.spatial;

import org.neo4j.graphdb.Node;

import java.util.Iterator;

public class SearchResults implements Iterable<Node> {
    private Iterable<Node> traverser;
    private int count = -1;

    public SearchResults(Iterable<Node> traverser) {
        this.traverser = traverser;
    }

    @Override
    public Iterator<Node> iterator() {
        return traverser.iterator();
    }

    public int count() {
        if (count < 0) {
            count = 0;
            for (@SuppressWarnings("unused")
                    Node node : this) {
                count++;
            }
        }
        return count;
    }
}
