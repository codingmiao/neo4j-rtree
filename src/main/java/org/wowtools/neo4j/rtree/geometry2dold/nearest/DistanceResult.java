package org.wowtools.neo4j.rtree.geometry2dold.nearest;

import org.locationtech.jts.geom.Geometry;
import org.neo4j.graphdb.Node;

/**
 * 距离查询结果
 *
 * @author liuyu
 * @date 2020/6/12
 */
public class DistanceResult {
    private final Node t;
    private final double dist;
    private final Geometry geometry;

    public DistanceResult(Node t, double dist, Geometry geometry) {
        this.t = t;
        this.dist = dist;
        this.geometry = geometry;
    }

    public Node getNode() {
        return t;
    }

    public double getDist() {
        return dist;
    }

    public Geometry getGeometry() {
        return geometry;
    }

    @Override
    public String toString() {
        return "DistanceResult{" +
                "node=" + t.getId() +
                ", dist=" + dist +
                ", geometry=" + geometry.toText() +
                '}';
    }
}
