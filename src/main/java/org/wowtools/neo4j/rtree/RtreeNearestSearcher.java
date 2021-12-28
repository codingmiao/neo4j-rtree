package org.wowtools.neo4j.rtree;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.wowtools.neo4j.rtree.internal.RtreeLock;
import org.wowtools.neo4j.rtree.internal.define.Labels;
import org.wowtools.neo4j.rtree.internal.define.Relationships;
import org.wowtools.neo4j.rtree.internal.nearest.DistanceResult;
import org.wowtools.neo4j.rtree.util.NearestNeighbour;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;

/**
 * 最邻近搜索器
 *
 * @author liuyu
 * @date 2021/12/27
 */
public class RtreeNearestSearcher {

    private final long metadataNodeId;
    private final Lock readLock;


    private RtreeNearestSearcher(long metadataNodeId, Lock readLock) {
        this.metadataNodeId = metadataNodeId;
        this.readLock = readLock;
    }


    /**
     * 获取查询器
     *
     * @param tx   事务 此事务需要在外部手动关闭
     * @param name 索引名
     * @return
     */
    public static RtreeNearestSearcher get(Transaction tx, String name) {
        Node metadataNode = tx.findNode(Labels.METADATA, "name", name);
        if (null == metadataNode) {
            throw new RuntimeException("索引 " + name + " 不存在");
        }
        long metadataNodeId = metadataNode.getId();
        Lock readLock = RtreeLock.getUseReadWriteLock(name).readLock();
        RtreeNearestSearcher searcher = new RtreeNearestSearcher(metadataNodeId, readLock);
        return searcher;
    }


    /**
     * 最邻近查询。查询距离输入点pointNd的距离最近的点
     *
     * @param nearestNeighbour 最邻近查询函数
     * @param tx               事务 此事务需要在外部手动关闭
     * @return
     */
    public List<DistanceResult> nearest(NearestNeighbour nearestNeighbour, Transaction tx) {
        readLock.lock();
        try {
            Node metadataNode = tx.getNodeById(metadataNodeId);
            Iterator<Relationship> iterator = metadataNode.getRelationships(Relationships.RTREE_METADATA_TO_ROOT).iterator();
            if (!iterator.hasNext()) {
                return List.of();
            }
            Node root = iterator.next().getEndNode();
            List<DistanceResult> res = nearestNeighbour.find(root);
            return res;
        } finally {
            readLock.unlock();
        }
    }

}
