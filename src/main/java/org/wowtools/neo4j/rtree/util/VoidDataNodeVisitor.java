package org.wowtools.neo4j.rtree.util;

/**
 * 数据节点访问器
 *
 * @author liuyu
 * @date 2021/12/24
 */
@FunctionalInterface
public interface VoidDataNodeVisitor {

    void visit(long nodeId);
}
