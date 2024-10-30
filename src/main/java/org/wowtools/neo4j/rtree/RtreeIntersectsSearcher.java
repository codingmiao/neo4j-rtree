package org.wowtools.neo4j.rtree;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.wowtools.neo4j.rtree.internal.RtreeLock;
import org.wowtools.neo4j.rtree.internal.define.Labels;
import org.wowtools.neo4j.rtree.internal.define.Relationships;
import org.wowtools.neo4j.rtree.pojo.PointNd;
import org.wowtools.neo4j.rtree.pojo.RectNd;
import org.wowtools.neo4j.rtree.util.BooleanDataNodeVisitor;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.Lock;

/**
 * 相交关系查询器
 *
 * @author liuyu
 * @date 2021/12/24
 */
public class RtreeIntersectsSearcher {


    private final String metadataNodeId;
    private final Lock readLock;


    private RtreeIntersectsSearcher(String metadataNodeId, Lock readLock) {
        this.metadataNodeId = metadataNodeId;
        this.readLock = readLock;
    }

    /**
     * 获取查询器
     *
     * @param tx   事务 此事务需要在外部手动关闭
     * @param name 索引名
     * @return RtreeIntersectsSearcher
     */
    public static RtreeIntersectsSearcher get(Transaction tx, String name) {
        Node metadataNode = tx.findNode(Labels.METADATA, "name", name);
        if (null == metadataNode) {
            throw new RuntimeException("索引 " + name + " 不存在");
        }
        String metadataNodeId = metadataNode.getElementId();
        Lock readLock = RtreeLock.getUseReadWriteLock(name).readLock();
        RtreeIntersectsSearcher rtreeIntersectsSearcher = new RtreeIntersectsSearcher(metadataNodeId, readLock);
        return rtreeIntersectsSearcher;
    }


    /**
     * 相交查询
     *
     * @param bbox    查询的bbox范围
     * @param tx      事务 此事务需要在外部手动关闭
     * @param visitor 结果访问器
     */
    public void intersects(RectNd bbox, Transaction tx, BooleanDataNodeVisitor visitor) {
        readLock.lock();
        try {
            Node metadataNode = tx.getNodeByElementId(metadataNodeId);
            Iterator<Relationship> iterator = metadataNode.getRelationships(Relationships.RTREE_METADATA_TO_ROOT).iterator();
            if (!iterator.hasNext()) {
                return;
            }
            Node node = iterator.next().getEndNode();
            ArrayDeque<Node> stack = new ArrayDeque<>();
            stack.push(node);
            do {
                node = stack.pop();
                //判断当前节点是否与bbox相交
                Map<String, Object> mbrProperties = node.getProperties("mbrMax", "mbrMin");
                PointNd min = new PointNd((double[]) mbrProperties.get("mbrMin"));
                PointNd max = new PointNd((double[]) mbrProperties.get("mbrMax"));
                RectNd nodeMbr = new RectNd(min, max);
                if (!bbox.intersects(nodeMbr)) {
                    continue;
                }
                //子节点
                String label = node.getLabels().iterator().next().name();
                if (label.equals(Labels.RTREE_BRANCH.name())) {
                    for (Relationship relationship : node.getRelationships(Direction.OUTGOING, Relationships.RTREE_PARENT_TO_CHILD)) {
                        Node child = relationship.getEndNode();
                        stack.push(child);
                    }
                } else if (label.equals(Labels.RTREE_LEAF.name())) {
                    Map<String, Object> properties = node.getAllProperties();
                    int size = (int) properties.get("size");
                    for (int i = 0; i < size; i++) {
                        double[] rMin = (double[]) properties.get("entryMin" + i);
                        double[] rMax = (double[]) properties.get("entryMax" + i);
                        RectNd dataMbr = new RectNd(rMin, rMax);
                        if (bbox.intersects(dataMbr)) {
                            if (visitor.visit((String) properties.get("entryDataId" + i))) {
                                return;
                            }
                        }
                    }
                }
            } while (!stack.isEmpty());
        } finally {
            readLock.unlock();
        }

    }

    public String getMetadataNodeId() {
        return metadataNodeId;
    }
}
