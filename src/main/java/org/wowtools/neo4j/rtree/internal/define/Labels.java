package org.wowtools.neo4j.rtree.internal.define;

import org.neo4j.graphdb.Label;

/**
 * 标签
 *
 * @author liuyu
 * @date 2021/12/18
 */
public class Labels {
    /**
     * 树的描述信息
     */
    public static final Label METADATA = Label.label("RTREE_METADATA");

    /**
     * 树的非叶子节点
     */
    public static final Label RTREE_BRANCH = Label.label("RTREE_BRANCH");

    /**
     * 树的叶子节点
     */
    public static final Label RTREE_LEAF = Label.label("RTREE_LEAF");

    /**
     * 树的数据节点 记录了数据节点的节点id和bbox，仅用于快速删除或修改数据
     */
    public static final Label RTREE_ENTITY = Label.label("RTREE_ENTITY");
}
