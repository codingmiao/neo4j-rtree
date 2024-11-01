package org.wowtools.neo4j.rtree.internal.edit;

import org.neo4j.graphdb.*;
import org.wowtools.neo4j.rtree.internal.define.Labels;
import org.wowtools.neo4j.rtree.internal.define.Relationships;
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
public class TxCell {

    private Transaction tx;

    private final int limit;
    private final int mMin;
    private final int mMax;
    private final TxBuilder txBuilder;

    private int num;

    private final RectBuilder builder = new RectNd.Builder();

    private final Map<String, CacheNode> cacheNodeMap = new HashMap<>();

    private final Map<String, String> nodeParentMap = new HashMap<>();

    public CacheNode getNode(String nodeId) {
        CacheNode cacheNode = cacheNodeMap.get(nodeId);
        if (null != cacheNode) {
            return cacheNode;
        }
        org.neo4j.graphdb.Node noeNode = getTx().getNodeByElementId(nodeId);
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
        org.neo4j.graphdb.Node node = tx.createNode(label);
        CacheNode.NodeType nodeType;
        if (label == Labels.RTREE_BRANCH) {
            nodeType = CacheNode.NodeType.Branch;
        } else if (label == Labels.RTREE_LEAF) {
            nodeType = CacheNode.NodeType.Leaf;
        } else {
            throw new RuntimeException("未知label " + label.name());
        }
        CacheNode cacheNode = new CacheNode(node.getElementId(), this, nodeType);
        cacheNodeMap.put(node.getElementId(), cacheNode);
        return cacheNode;
    }

    public void setNodeParent(String nodeId, String parentNodeId) {
        nodeParentMap.put(nodeId, parentNodeId);
    }

    public org.wowtools.neo4j.rtree.internal.edit.Node getNodeFromNeo4j(String nid) {
        org.neo4j.graphdb.Node node = getTx().getNodeByElementId(nid);
        String labelName = node.getLabels().iterator().next().name();
        if (labelName.equals(Labels.RTREE_BRANCH.name())) {
            return NodeOfBranch.getFromNeo(getBuilder(), nid, this);
        } else if (labelName.equals(Labels.RTREE_LEAF.name())) {
            return NodeOfAxialSplitLeaf.getFromNeo(getBuilder(), nid, this);
        } else {
            throw new RuntimeException("未知标签 " + labelName);
        }
    }

    public TxCell(int limit, int mMin, int mMax, TxBuilder txBuilder) {
        this.limit = limit;
        this.mMin = mMin;
        this.mMax = mMax;
        this.txBuilder = txBuilder;
        tx = newTx();
    }

    public RectBuilder getBuilder() {
        return builder;
    }

    protected Transaction _newTx() {
        return txBuilder.beginTx();
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
        //各cacheNode属性提交
        cacheNodeMap.forEach((nid, cacheNode) -> {
            cacheNode.commit();
        });
        //RTREE_PARENT_TO_CHILD关系变更
        nodeParentMap.forEach((nid, parentNid) -> {
            if (null == parentNid) {//parentNid为空，删除旧关系
                ResourceIterable<Relationship> relationships = tx.getNodeByElementId(nid).getRelationships(Direction.INCOMING, Relationships.RTREE_PARENT_TO_CHILD);
                for (Relationship relationship : relationships) {
                    relationship.delete();
                }
                relationships.close();
            } else {//parentNid非空，修改关系
                boolean hasRelationship = false;//新的关系是否已存在
                ResourceIterable<Relationship> relationships = tx.getNodeByElementId(nid).getRelationships(Direction.INCOMING, Relationships.RTREE_PARENT_TO_CHILD);
                for (Relationship relationship : relationships) {
                    if (hasRelationship) {
                        relationship.delete();
                    } else {
                        if (parentNid.equals(relationship.getElementId())) {
                            hasRelationship = true;
                        } else {
                            relationship.delete();
                        }
                    }
                }
                relationships.close();
                if (!hasRelationship) {
                    tx.getNodeByElementId(parentNid).createRelationshipTo(tx.getNodeByElementId(nid), Relationships.RTREE_PARENT_TO_CHILD);
                }
            }
        });
        neoGc();//gc
        tx.commit();//提交neo4j事务
        //清理内存中的对象
        num = 0;
        cacheNodeMap.forEach((nid, cacheNode) -> {
            cacheNode.clearCache();
        });
        cacheNodeMap.clear();
        nodeParentMap.clear();
    }

    /**
     * 做一次“gc”操作，把cacheNodeMap中没有引用的node及其子节点从数据库删除掉
     */
    private void neoGc() {
        cacheNodeMap.forEach((id, n) -> {
            org.neo4j.graphdb.Node node;
            try {
                node = tx.getNodeByElementId(id);
            } catch (NotFoundException e) {
                //node已在前面的遍历中被删除，跳过
                return;
            }
            org.neo4j.graphdb.Node thisRoot = node;
            ArrayDeque<String> stack = new ArrayDeque<>();
            stack.push(node.getElementId());
            boolean findRoot = false;
            do {
                try {
                    node = tx.getNodeByElementId(stack.pop());
                } catch (NotFoundException e) {
                    continue;
                }
                //检查是否有关系 RTREE_METADATA_TO_ROOT
                ResourceIterable<Relationship> relationships = node.getRelationships(Direction.INCOMING, Relationships.RTREE_METADATA_TO_ROOT);
                Iterator<Relationship> iterator = relationships.iterator();
                if (iterator.hasNext()) {
                    findRoot = true;
                    break;
                }
                relationships.close();
                //将父节点丢进栈继续
                relationships = node.getRelationships(Direction.INCOMING, Relationships.RTREE_PARENT_TO_CHILD);
                for (Relationship relationship : relationships) {
                    org.neo4j.graphdb.Node parent = relationship.getStartNode();
                    thisRoot = parent;
                    stack.push(parent.getElementId());
                }
                relationships.close();
            } while (!stack.isEmpty());
            //未找到根节点，则删除所有本次遍历过的设备
            if (!findRoot) {
                stack = new ArrayDeque<>();
                stack.push(thisRoot.getElementId());
                do {
                    try {
                        node = tx.getNodeByElementId(stack.pop());
                    } catch (NotFoundException e) {
                        continue;
                    }
                    //遍历子节点
                    ResourceIterable<Relationship> relationships = node.getRelationships(Direction.OUTGOING, Relationships.RTREE_PARENT_TO_CHILD);
                    for (Relationship relationship : relationships) {
                        org.neo4j.graphdb.Node parent = relationship.getStartNode();
                        thisRoot = parent;
                        stack.push(parent.getElementId());
                    }
                    relationships.close();
                    //删除父节点及关系
                    relationships = node.getRelationships();
                    for (Relationship r : relationships) {
                        r.delete();
                    }
                    relationships.close();
                    node.delete();
                } while (!stack.isEmpty());
            }
        });
    }

    public void close() {
        tx.close();
    }

    public int getmMin() {
        return mMin;
    }

    public int getmMax() {
        return mMax;
    }
}
