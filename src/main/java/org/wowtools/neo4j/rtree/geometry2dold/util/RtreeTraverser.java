package org.wowtools.neo4j.rtree.geometry2dold.util;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.wowtools.neo4j.rtree.geometry2dold.Constant;
import org.wowtools.neo4j.rtree.geometry2dold.spatial.RTreeIndex;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * rtree遍历器
 * @author liuyu
 * @date 2020/11/25
 */
public class RtreeTraverser {


    /**
     * 索引点访问器，决定哪些索引节点符合条件
     */
    @FunctionalInterface
    public interface IndexNodeVisitor {
        /**
         * 访问树索引节点
         *
         * @param rtreeNode rtreeNode
         * @param nodeBbox  nodeBbox
         * @return 为false时跳过此节点的子节点
         */
        boolean vist(Node rtreeNode, double[] nodeBbox);
    }

    /**
     * 对象节点访问器
     */
    @FunctionalInterface
    public interface ObjNodeVisitor {
        /**
         * @param objNode     访问到的node
         */
        void vist(Node objNode);
    }

    /**
     * 遍历rtree
     * @param tx tx
     * @param rTreeIndex index
     * @param rtreeNodeVisitor 索引节点访问器，决定哪些节点符合条件
     * @param objNodeVisitor 结果节点访问器
     */
    public static void traverse(Transaction tx, RTreeIndex rTreeIndex, IndexNodeVisitor rtreeNodeVisitor, ObjNodeVisitor objNodeVisitor) {
        Node rtreeNode = rTreeIndex.getIndexRoot(tx);
        Deque<Node> stack = new ArrayDeque<>();//辅助遍历的栈
        stack.push(rtreeNode);
        while (!stack.isEmpty()) {
            rtreeNode = stack.pop();
            //判断当前节点的bbox是否与输入bbox相交
            double[] nodeBbox = (double[]) rtreeNode.getProperty(Constant.RtreeProperty.bbox);
            if (!rtreeNodeVisitor.vist(rtreeNode, nodeBbox)) {
                continue;
            }

            if (rtreeNode.hasRelationship(Direction.OUTGOING, Constant.Relationship.RTREE_CHILD)) {
                //若有下级索引节点,下级索引节点入栈
                for (Relationship relationship : rtreeNode.getRelationships(Direction.OUTGOING, Constant.Relationship.RTREE_CHILD)) {
                    Node child = relationship.getEndNode();
                    stack.push(child);
                }
            } else if (rtreeNode.hasRelationship(Direction.OUTGOING, Constant.Relationship.RTREE_REFERENCE)) {
                //若有下级对象节点，返回结果
                for (Relationship relationship : rtreeNode.getRelationships(Direction.OUTGOING, Constant.Relationship.RTREE_REFERENCE)) {
                    Node objNode = relationship.getEndNode();
                    objNodeVisitor.vist(objNode);
                }
            }

        }
    }

}
