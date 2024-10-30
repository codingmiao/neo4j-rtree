package org.wowtools.neo4j.rtree.util;

/**
 * 数据节点访问器，返回true时，终止接下来的遍历
 *
 * @author liuyu
 * @date 2021/12/24
 */
@FunctionalInterface
public interface BooleanDataNodeVisitor {

    /**
     * 访问到数据节点时触发此方法
     *
     * @param nodeId 数据节点neo4j id
     * @return 返回true时，终止接下来的遍历
     */
    boolean visit(String nodeId);
}
