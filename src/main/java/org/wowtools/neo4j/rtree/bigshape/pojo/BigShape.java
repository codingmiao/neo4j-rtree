package org.wowtools.neo4j.rtree.bigshape.pojo;

import org.wowtools.neo4j.rtree.spatial.RTreeIndex;

/**
 * 大shape对象
 * @author liuyu
 * @date 2020/11/23
 */
public class BigShape {
    private final RTreeIndex rTreeIndex;

    public BigShape(RTreeIndex rTreeIndex) {
        this.rTreeIndex = rTreeIndex;
    }
}
