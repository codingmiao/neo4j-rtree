package org.wowtools.neo4j.rtree.bigshape.pojo;

import org.locationtech.jts.geom.Geometry;

/**
 * 切割开的一个网格格子
 *
 * @author liuyu
 * @date 2020/11/23
 */
public class Grid {
    /**
     * 格子与输入geometry的相交部分
     */
    private final Geometry geometry;
    /**
     * 格子是否完全被输入geometry覆盖
     */
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
