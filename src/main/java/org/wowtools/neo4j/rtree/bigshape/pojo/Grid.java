package org.wowtools.neo4j.rtree.bigshape.pojo;

import org.locationtech.jts.geom.Geometry;

/**
 * 切割开的一个网格格子
 *
 * @author liuyu
 * @date 2020/11/23
 */
public class Grid {
    private final Geometry geometry;
    private final boolean coverCompletely;

    public Grid(Geometry geometry, boolean coverCompletely) {
        this.geometry = geometry;
        this.coverCompletely = coverCompletely;
    }

    public Geometry getGeometry() {
        return geometry;
    }

    public boolean isCoverCompletely() {
        return coverCompletely;
    }
}
