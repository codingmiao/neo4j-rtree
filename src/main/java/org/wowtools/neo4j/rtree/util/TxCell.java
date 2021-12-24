package org.wowtools.neo4j.rtree.util;

import org.neo4j.graphdb.*;
import org.wowtools.neo4j.rtree.internal.define.Labels;
import org.wowtools.neo4j.rtree.internal.define.Relationships;
import org.wowtools.neo4j.rtree.internal.edit.CacheNode;
import org.wowtools.neo4j.rtree.internal.edit.NodeOfAxialSplitLeaf;
import org.wowtools.neo4j.rtree.internal.edit.NodeOfBranch;
import org.wowtools.neo4j.rtree.internal.edit.RectBuilder;
import org.wowtools.neo4j.rtree.pojo.RectNd;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 事务外壳，用于在构建rtree时获取事务
 * 注意，此对象非线程安全
 *
 * @author liuyu
 * @date 2021/12/17
 */
public class TxCell{

    private Transaction tx;

    private final int limit;
    private final GraphDatabaseService graphDb;

    private int num;

    private final RectBuilder builder = new RectNd.Builder();

    private final Map<Long, CacheNode> cacheNodeMap = new HashMap<>();

    public CacheNode getNode(long nodeId) {
        CacheNode cacheNode = cacheNodeMap.get(nodeId);
        if (null != cacheNode) {
            return cacheNode;
        }
        Node noeNode = getTx().getNodeById(nodeId);
        String labelName = noeNode.getLabels().iterator().next().name();
        if (labelName.equals(Labels.RTREE_BRANCH.name())) {
            cacheNode = new CacheNode(nodeId, this, CacheNode.NodeType.Branch);
        } else if (labelName.equals(Labels.RTREE_LEAF.name())) {
            cacheNode = new CacheNode(nodeId, this, CacheNode.NodeType.Branch);
        } else {
            throw new RuntimeException("label错误: " + labelName + " id: " + noeNode);
        }
        cacheNodeMap.put(nodeId, cacheNode);
        return cacheNode;
    }


    public CacheNode newNode(Label label) {
        Node node = tx.createNode(label);
        CacheNode.NodeType nodeType;
        if (label == Labels.RTREE_BRANCH) {
            nodeType = CacheNode.NodeType.Branch;
        } else if (label == Labels.RTREE_LEAF) {
            nodeType = CacheNode.NodeType.Leaf;
        } else {
            throw new RuntimeException("未知label " + label.name());
        }
        CacheNode cacheNode = new CacheNode(node.getId(), this, nodeType);
        cacheNodeMap.put(node.getId(), cacheNode);
        return cacheNode;
    }

    public org.wowtools.neo4j.rtree.internal.edit.Node getNodeFromNeo4j(long nid) {
        Node node = getTx().getNodeById(nid);
        String labelName = node.getLabels().iterator().next().name();
        if (labelName.equals(Labels.RTREE_BRANCH.name())) {
            return NodeOfBranch.getFromNeo(getBuilder(), nid, this);
        } else if (labelName.equals(Labels.RTREE_LEAF.name())) {
            return NodeOfAxialSplitLeaf.getFromNeo(getBuilder(), nid, this);
        } else {
            throw new RuntimeException("未知标签 " + labelName);
        }
    }

    public TxCell(int limit, GraphDatabaseService graphDb) {
        this.limit = limit;
        this.graphDb = graphDb;
        tx = newTx();
    }

    public RectBuilder getBuilder() {
        return builder;
    }

    protected Transaction _newTx() {
        return graphDb.beginTx();
    }

    public Transaction newTx() {
        tx = _newTx();
        return tx;
    }

    public Transaction getTx() {
        return tx;
    }

    public void addChange() {
        num++;
    }

    public void addChange(int n) {
        num += n;
    }

    public boolean limitCommit() {
        if (num >= limit) {
            commit();
            newTx();
            return true;
        }
        return false;
    }

    public void commit() {
        cacheNodeMap.forEach((nid, cacheNode) -> {
            cacheNode.commit();
        });
        neoGc();
        tx.commit();
        num = 0;
        cacheNodeMap.forEach((nid, cacheNode) -> {
            cacheNode.clearCache();
        });
        cacheNodeMap.clear();
    }

    /**
     * 做一次“gc”操作，把cacheNodeMap中没有引用的node及其子节点从数据库删除掉
     */
    private void neoGc() {
        cacheNodeMap.forEach((id, n) -> {
            Node node;
            try {
                node = tx.getNodeById(id);
            } catch (NotFoundException e) {
                //node已在前面的遍历中被删除，跳过
                return;
            }
            Node thisRoot = node;
            ArrayDeque<Long> stack = new ArrayDeque<>();
            stack.push(node.getId());
            boolean findRoot = false;
            do {
                try {
                    node = tx.getNodeById(stack.pop());
                } catch (NotFoundException e) {
                    continue;
                }
                //检查是否有关系 RTREE_METADATA_TO_ROOT
                Iterator<Relationship> iterator = node.getRelationships(Direction.INCOMING, Relationships.RTREE_METADATA_TO_ROOT).iterator();
                if (iterator.hasNext()) {
                    findRoot = true;
                    break;
                }
                //将父节点丢进栈继续
                for (Relationship relationship : node.getRelationships(Direction.INCOMING, Relationships.RTREE_PARENT_TO_CHILD)) {
                    Node parent = relationship.getStartNode();
                    thisRoot = parent;
                    stack.push(parent.getId());
                }
            } while (!stack.isEmpty());
            //未找到根节点，则删除所有本次遍历过的设备
            if (!findRoot) {
                stack = new ArrayDeque<>();
                stack.push(thisRoot.getId());
                do {
                    try {
                        node = tx.getNodeById(stack.pop());
                    } catch (NotFoundException e) {
                        continue;
                    }
                    //遍历子节点
                    for (Relationship relationship : node.getRelationships(Direction.OUTGOING, Relationships.RTREE_PARENT_TO_CHILD)) {
                        Node parent = relationship.getStartNode();
                        thisRoot = parent;
                        stack.push(parent.getId());
                    }
                    //删除父节点及关系
                    for (Relationship r : node.getRelationships()) {
                        r.delete();
                    }
                    node.delete();
                } while (!stack.isEmpty());
            }
        });
    }

    public void close() {
        tx.close();
    }
}
