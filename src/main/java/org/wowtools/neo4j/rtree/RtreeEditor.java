package org.wowtools.neo4j.rtree;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.Schema;
import org.wowtools.neo4j.rtree.internal.RtreeLock;
import org.wowtools.neo4j.rtree.internal.define.Labels;
import org.wowtools.neo4j.rtree.internal.define.PropertyNames;
import org.wowtools.neo4j.rtree.internal.define.Relationships;
import org.wowtools.neo4j.rtree.internal.edit.GraphDbTxBuilder;
import org.wowtools.neo4j.rtree.internal.edit.RTree;
import org.wowtools.neo4j.rtree.internal.edit.TxBuilder;
import org.wowtools.neo4j.rtree.internal.edit.TxCell;
import org.wowtools.neo4j.rtree.pojo.RectNd;
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

    private static final String entityNodeIdIndexName = "index_entity_node_id";

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
     * 获取RtreeEditor
     *
     * @param txBuilder   neo4j 事务构建接口
     * @param commitLimit 操作达到多少个顶点时执行提交操作
     * @param name        索引名
     * @return RtreeEditor
     */
    public static RtreeEditor get(TxBuilder txBuilder, int commitLimit, String name) {
        String metadataNodeId;
        int mMin;
        int mMax;
        synchronized (RtreeLock.getCreateIndexLock()) {
            try (Transaction tx = txBuilder.beginTx()) {
                Node metadataNode = tx.findNode(Labels.METADATA, "name", name);
                if (null == metadataNode) {
                    throw new RuntimeException("索引 " + name + " 不存在");
                }
                metadataNodeId = metadataNode.getElementId();
                Map<String, Object> properties = metadataNode.getProperties("mMin", "mMax");
                mMin = (int) properties.get("mMin");
                mMax = (int) properties.get("mMax");
            }

        }

        TxCell txCell = new TxCell(commitLimit, mMin, mMax, txBuilder);
        RTree rTree = new RTree(new RectNd.Builder(), mMin, mMax, txCell, metadataNodeId);
        RtreeEditor rtreeEditor = new RtreeEditor(rTree, name, txCell);
        return rtreeEditor;
    }

    /**
     * 获取RtreeEditor
     *
     * @param graphdb     neo4j db
     * @param commitLimit 操作达到多少个顶点时执行提交操作
     * @param name        索引名
     * @return RtreeEditor
     */
    public static RtreeEditor get(GraphDatabaseService graphdb, int commitLimit, String name) {
        TxBuilder txBuilder = new GraphDbTxBuilder(graphdb);
        return get(txBuilder, commitLimit, name);
    }

    private static void createIndexIfNotExist(TxBuilder txBuilder) {
        try (Transaction tx = txBuilder.beginTx()){
            Schema schema = tx.schema();
            try {
                schema.getIndexByName(entityNodeIdIndexName);
            } catch (Exception e) {
                schema.indexFor(Labels.RTREE_ENTITY)
                        .on(PropertyNames.entryDataId)
                        .withName(entityNodeIdIndexName)
                        .create();
            }
        }
    }

    /**
     * 新建索引
     *
     * @param txBuilder   txBuilder
     * @param commitLimit 操作达到多少个顶点时执行提交操作
     * @param name        索引名
     * @param mMin        索引中每个节点最小子节点数
     * @param mMax        索引中每个节点最大子节点数
     * @return RtreeEditor
     */
    public static RtreeEditor create(TxBuilder txBuilder, int commitLimit, String name, int mMin, int mMax) {
        createIndexIfNotExist(txBuilder);
        TxCell txCell = new TxCell(commitLimit, mMin, mMax, txBuilder);
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

        RTree rTree = new RTree(new RectNd.Builder(), mMin, mMax, txCell, metadataNode.getElementId());
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
        TxBuilder txBuilder = new GraphDbTxBuilder(graphdb);
        return create(txBuilder, commitLimit, name, mMin, mMax);
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
        TxBuilder txBuilder = new GraphDbTxBuilder(graphdb);
        return getOrCreate(txBuilder, commitLimit, name, mMin, mMax);
    }

    /**
     * 若指定名称的索引存在，获取索引，若不存在，则新建一个
     *
     * @param txBuilder   txBuilder
     * @param commitLimit 操作达到多少个顶点时执行提交操作
     * @param name        索引名
     * @param mMin        索引中每个节点最小子节点数，如索引已存在则使用现有值，此输入值失效
     * @param mMax        索引中每个节点最大子节点数，如索引已存在则使用现有值，此输入值失效
     * @return RtreeEditor
     */
    public static RtreeEditor getOrCreate(TxBuilder txBuilder, int commitLimit, String name, int mMin, int mMax) {
        createIndexIfNotExist(txBuilder);
        TxCell txCell = new TxCell(commitLimit, mMin, mMax, txBuilder);
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
            RTree rTree = new RTree(new RectNd.Builder(), mMin, mMax, txCell, metadataNode.getElementId());
            RtreeEditor rtreeEditor = new RtreeEditor(rTree, name, txCell);
            return rtreeEditor;
        } else {
            metadataNode.setProperty("mMin", mMin);
            metadataNode.setProperty("mMax", mMax);
            metadataNode.setProperty("name", name);

            RTree rTree = new RTree(new RectNd.Builder(), mMin, mMax, txCell, metadataNode.getElementId());
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
        TxBuilder txBuilder = new GraphDbTxBuilder(graphdb);
        drop(txBuilder, name, dataNodeVisitor);
    }

    /**
     * 删除索引
     *
     * @param txBuilder       txBuilder
     * @param name            索引名
     * @param dataNodeVisitor 数据节点访问器，具体实现遇到数据节点该如何处置（例如将数据节点删除）
     */
    public static void drop(TxBuilder txBuilder, String name, VoidDataNodeVisitor dataNodeVisitor) {
        createIndexIfNotExist(txBuilder);

        //删掉METADATA
        String rootId;
        int mMax;
        try (Transaction tx = txBuilder.beginTx()) {
            synchronized (RtreeLock.getCreateIndexLock()) {
                Node metadataNode = tx.findNode(Labels.METADATA, "name", name);
                if (null == metadataNode) {
                    throw new RuntimeException("索引 " + name + " 不存在");
                }
                Relationship r = metadataNode.getRelationships(Relationships.RTREE_METADATA_TO_ROOT).iterator().next();
                rootId = r.getEndNode().getElementId();
                r.delete();
                mMax = (int) metadataNode.getProperty("mMax");
                metadataNode.delete();
                tx.commit();
            }
        }
        //删掉树上的节点
        String[] keys = new String[mMax];
        for (int i = 0; i < mMax; i++) {
            keys[i] = PropertyNames.entryDataId + i;
        }
        try (Transaction tx = txBuilder.beginTx()) {
            Node node = tx.getNodeByElementId(rootId);
            ArrayDeque<Node> stack = new ArrayDeque<>();
            stack.push(node);
            do {
                node = stack.pop();
                String label = node.getLabels().iterator().next().name();
                if (label.equals(Labels.RTREE_BRANCH.name())) {
                    ResourceIterable<Relationship> relationships = node.getRelationships(Direction.OUTGOING, Relationships.RTREE_PARENT_TO_CHILD);
                    for (Relationship relationship : relationships) {
                        Node child = relationship.getEndNode();
                        relationship.delete();
                        stack.push(child);
                    }
                    relationships.close();
                } else if (label.equals(Labels.RTREE_LEAF.name())) {
                    Map<String, Object> entryDataIds = node.getProperties(keys);
                    entryDataIds.forEach((key, id) -> {
                        dataNodeVisitor.visit((String) id);
                    });
                    ResourceIterable<Relationship> relationships = node.getRelationships(Direction.OUTGOING, Relationships.RTREE_LEAF_TO_ENTITY);
                    for (Relationship relationship : relationships) {
                        relationship.getEndNode().delete();
                        relationship.delete();
                    }
                    relationships.close();
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
