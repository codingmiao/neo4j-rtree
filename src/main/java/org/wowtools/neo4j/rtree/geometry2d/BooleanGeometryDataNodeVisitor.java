package org.wowtools.neo4j.rtree.geometry2d;

import org.locationtech.jts.geom.Geometry;

/**
 * geometry数据节点访问器，返回true时，终止接下来的遍历
 *
 * @author liuyu
 * @date 2021/12/24
 */
@FunctionalInterface
public interface BooleanGeometryDataNodeVisitor {

    /**
     * 访问到数据节点时触发此方法
     *
     * @param nodeId 数据节点neo4j id
     * @param geometry 数据节点的geometry，由于空间计算时要使用一次，这里就直接把geometry返回了，避免重复的wkb转geometry
     * @return 返回true时，终止接下来的遍历
     */
    boolean visit(long nodeId, Geometry geometry);
}
