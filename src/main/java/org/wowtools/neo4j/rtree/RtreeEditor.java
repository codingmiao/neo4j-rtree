package org.wowtools.neo4j.rtree;

import org.neo4j.graphdb.*;
import org.wowtools.neo4j.rtree.internal.RtreeLock;
import org.wowtools.neo4j.rtree.internal.define.Labels;
import org.wowtools.neo4j.rtree.internal.define.Relationships;
import org.wowtools.neo4j.rtree.internal.edit.RTree;
import org.wowtools.neo4j.rtree.pojo.RectNd;
import org.wowtools.neo4j.rtree.internal.edit.TxCell;
import org.wowtools.neo4j.rtree.util.VoidDataNodeVisitor;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.locks.Lock;

/**
 * rtree编辑器，此对象实例化时，会启动一个事务，并在索引上加写锁，所以务必在结束时调用close方法
 * 此对象非线程安全
 *
 * @author liuyu
 * @date 2021/12/24
 */
public class RtreeEditor implements AutoCloseable {

    private final RTree rTree;
    private final Lock writeLock;
    private final TxCell txCell;

    private RtreeEditor(RTree rTree, String name, TxCell txCell) {
        this.rTree = rTree;
        writeLock = RtreeLock.getUseReadWriteLock(name).writeLock();
        this.txCell = txCell;
        writeLock.lock();
    }

    /**
     * 获取索引
     *
     * @param graphdb     neo4j db
     * @param commitLimit 操作达到多少个顶点时执行提交操作
     * @param name        索引名
     * @return RtreeEditor
     */
    public static RtreeEditor get(GraphDatabaseService graphdb, int commitLimit, String name) {
        long metadataNodeId;
        int mMin;
        int mMax;
        synchronized (RtreeLock.getCreateIndexLock()) {
            try (Transaction tx = graphdb.beginTx()) {
                Node metadataNode = tx.findNode(Labels.METADATA, "name", name);
                if (null == metadataNode) {
                    throw new RuntimeException("索引 " + name + " 不存在");
                }
                metadataNodeId = metadataNode.getId();
                Map<String, Object> properties = metadataNode.getProperties("mMin", "mMax");
                mMin = (int) properties.get("mMin");
                mMax = (int) properties.get("mMax");
            }

        }

        TxCell txCell = new TxCell(commitLimit, mMin, mMax, graphdb);
        RTree rTree = new RTree(new RectNd.Builder(), metadataNodeId, mMin, mMax, txCell);
        RtreeEditor rtreeEditor = new RtreeEditor(rTree, name, txCell);
        return rtreeEditor;
    }

    /**
     * 新建索引
     *
     * @param graphdb     neo4j db
     * @param commitLimit 操作达到多少个顶点时执行提交操作
     * @param name        索引名
     * @param mMin        索引中每个节点最小子节点数
     * @param mMax        索引中每个节点最大子节点数
     * @return RtreeEditor
     */
    public static RtreeEditor create(GraphDatabaseService graphdb, int commitLimit, String name, int mMin, int mMax) {
        TxCell txCell = new TxCell(commitLimit, mMin, mMax, graphdb);
        Node metadataNode;
        synchronized (RtreeLock.getCreateIndexLock()) {
            metadataNode = txCell.getTx().findNode(Labels.METADATA, "name", name);
            if (null != metadataNode) {
                txCell.close();
                throw new RuntimeException("索引 " + name + " 已存在");
            }
            metadataNode = txCell.getTx().createNode(Labels.METADATA);
        }
        metadataNode.setProperty("mMin", mMin);
        metadataNode.setProperty("mMax", mMax);
        metadataNode.setProperty("name", name);

        RTree rTree = new RTree(new RectNd.Builder(), metadataNode.getId(), mMin, mMax, txCell);
        RtreeEditor rtreeEditor = new RtreeEditor(rTree, name, txCell);
        return rtreeEditor;
    }

    /**
     * 若指定名称的索引存在，获取索引，若不存在，则新建一个
     *
     * @param graphdb     neo4j db
     * @param commitLimit 操作达到多少个顶点时执行提交操作
     * @param name        索引名
     * @param mMin        索引中每个节点最小子节点数，如索引已存在则使用现有值，此输入值失效
     * @param mMax        索引中每个节点最大子节点数，如索引已存在则使用现有值，此输入值失效
     * @return RtreeEditor
     */
    public static RtreeEditor getOrCreate(GraphDatabaseService graphdb, int commitLimit, String name, int mMin, int mMax) {
        TxCell txCell = new TxCell(commitLimit, mMin, mMax, graphdb);
        Node metadataNode;
        boolean exist;
        synchronized (RtreeLock.getCreateIndexLock()) {
            metadataNode = txCell.getTx().findNode(Labels.METADATA, "name", name);
            if (null == metadataNode) {
                metadataNode = txCell.getTx().createNode(Labels.METADATA);
                exist = false;
            } else {
                exist = true;
            }
        }

        if (exist) {
            RTree rTree = new RTree(new RectNd.Builder(), metadataNode.getId(), mMin, mMax, txCell);
            RtreeEditor rtreeEditor = new RtreeEditor(rTree, name, txCell);
            return rtreeEditor;
        } else {
            metadataNode.setProperty("mMin", mMin);
            metadataNode.setProperty("mMax", mMax);
            metadataNode.setProperty("name", name);

            RTree rTree = new RTree(new RectNd.Builder(), metadataNode.getId(), mMin, mMax, txCell);
            RtreeEditor rtreeEditor = new RtreeEditor(rTree, name, txCell);
            return rtreeEditor;
        }

    }

    /**
     * 删除索引
     *
     * @param graphdb         neo4j db
     * @param name            索引名
     * @param dataNodeVisitor 数据节点访问器，具体实现遇到数据节点该如何处置（例如将数据节点删除）
     */
    public static void drop(GraphDatabaseService graphdb, String name, VoidDataNodeVisitor dataNodeVisitor) {
        //删掉METADATA
        long rootId;
        int mMax;
        try (Transaction tx = graphdb.beginTx()) {
            synchronized (RtreeLock.getCreateIndexLock()) {
                Node metadataNode = tx.findNode(Labels.METADATA, "name", name);
                if (null == metadataNode) {
                    throw new RuntimeException("索引 " + name + " 不存在");
                }
                Relationship r = metadataNode.getRelationships(Relationships.RTREE_METADATA_TO_ROOT).iterator().next();
                rootId = r.getEndNodeId();
                r.delete();
                mMax = (int) metadataNode.getProperty("mMax");
                metadataNode.delete();
                tx.commit();
            }
        }
        //删掉树上的节点
        String[] keys = new String[mMax];
        for (int i = 0; i < mMax; i++) {
            keys[i] = "entryDataId" + i;
        }
        try (Transaction tx = graphdb.beginTx()) {
            Node node = tx.getNodeById(rootId);
            ArrayDeque<Node> stack = new ArrayDeque<>();
            stack.push(node);
            do {
                node = stack.pop();
                String label = node.getLabels().iterator().next().name();
                if (label.equals(Labels.RTREE_BRANCH.name())) {
                    for (Relationship relationship : node.getRelationships(Direction.OUTGOING, Relationships.RTREE_PARENT_TO_CHILD)) {
                        Node child = relationship.getEndNode();
                        relationship.delete();
                        stack.push(child);
                    }
                } else if (label.equals(Labels.RTREE_LEAF.name())) {
                    Map<String, Object> entryDataIds = node.getProperties(keys);
                    entryDataIds.forEach((key, id) -> {
                        dataNodeVisitor.visit((long) id);
                    });
                }
                node.delete();
            } while (!stack.isEmpty());
            tx.commit();
        }
    }

    /**
     * 向索引中添加数据
     *
     * @param t 数据的外接矩形
     */
    public void add(final RectNd t) {
        rTree.add(t);
        txCell.addChange();
        txCell.limitCommit();
    }

    /**
     * 从索引中移除数据，注意不会删除数据节点，如需删除或其它操作应在自身业务代码中实现
     *
     * @param t 被移除的节点，min、max、dataNodeId必须与现有节点一致
     */
    public void remove(final RectNd t) {
        rTree.remove(t);
        txCell.addChange();
//        txCell.getTx().getNodeById(t.getDataNodeId()).delete(); //由外部自行决定处理是否将其删除
        txCell.limitCommit();
    }

    /**
     * 修改现有数据
     *
     * @param told 现有节点，min、max、dataNodeId必须与现有节点一致
     * @param tnew 新节点，dataNodeId必须与现有节点一致
     */
    public void update(final RectNd told, final RectNd tnew) {
        rTree.update(told, tnew);
        txCell.addChange();
        txCell.limitCommit();
    }

    @Override
    public void close() {
        txCell.commit();
        writeLock.unlock();
    }

    public RTree getrTree() {
        return rTree;
    }

    public TxCell getTxCell() {
        return txCell;
    }
}
