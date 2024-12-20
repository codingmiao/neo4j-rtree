package org.wowtools.neo4j.rtree.util;

/**
 * @author liuyu
 * @date 2020/6/12
 */

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.wowtools.neo4j.rtree.internal.define.Labels;
import org.wowtools.neo4j.rtree.internal.define.PropertyNames;
import org.wowtools.neo4j.rtree.internal.define.Relationships;
import org.wowtools.neo4j.rtree.internal.nearest.MinDist;
import org.wowtools.neo4j.rtree.internal.nearest.MinDistComparator;
import org.wowtools.neo4j.rtree.pojo.PointNd;

import java.util.*;

/**
 * 最邻近搜索查询条件，计算距离目标点最近的几个点
 */
public abstract class NearestNeighbour<T extends DistanceResult> {

    private final DistanceResultNodeFilter filter;
    private final int maxHits;
    private final PointNd pointNd;

    public static final DistanceResultNodeFilter alwaysTrue = dr -> true;


    /**
     * @param filter  过滤器 为null则不过滤
     * @param maxHits 最大返回条数
     * @param pointNd 目标点
     */
    public NearestNeighbour(DistanceResultNodeFilter filter, int maxHits, PointNd pointNd) {
        this.pointNd = pointNd;
        this.filter = filter;
        this.maxHits = maxHits;
    }

    /**
     * @param maxHits 最大返回条数
     * @param pointNd 目标点
     */
    public NearestNeighbour(int maxHits, PointNd pointNd) {
        this.pointNd = pointNd;
        this.filter = alwaysTrue;
        this.maxHits = maxHits;
    }

    /**
     * 新建一个DistanceResult对象，包含了距离和dataNodeId
     *
     * @param pointNd    查询的点
     * @param dataNodeId dataNodeId
     * @return DistanceResult对象
     */
    public abstract T createDistanceResult(PointNd pointNd, String dataNodeId);

    /**
     * @return the nearest neighbour
     */
    public List<T> find(Node root) {
        List<T> ret =
                new ArrayList<>(maxHits);
        MinDistComparator nc =
                new MinDistComparator(pointNd);
        PriorityQueue<Node> queue = new PriorityQueue<Node>(20, nc);
        queue.add(root);
        while (!queue.isEmpty()) {
            Node n = queue.remove();
            if (n.hasLabel(Labels.RTREE_BRANCH)) {
                nnExpandInternal(n, ret, maxHits, queue);
            } else {
                nnExpandLeaf(n, filter, ret, maxHits);
            }
        }
        return ret;
    }


    //访问索引上的非叶子节点
    private void nnExpandInternal(Node node,
                                  List<T> drs,
                                  int maxHits,
                                  PriorityQueue<Node> queue) {

        for (Relationship relationship : node.getRelationships(Direction.OUTGOING, Relationships.RTREE_PARENT_TO_CHILD)) {
            Node n = relationship.getEndNode();
            Map<String, Object> properties = n.getProperties("mbrMax", "mbrMin");
            double[] mins = (double[]) properties.get("mbrMin");
            double[] maxs = (double[]) properties.get("mbrMax");
            double minDist = MinDist.get(mins, maxs, pointNd);
            int t = drs.size();
            // drs is sorted so we can check only the last entry
            if (t < maxHits || minDist <= drs.get(t - 1).getDist()) {
                queue.add(n);
            }
        }
    }

    //访问索引上的叶子节点
    private void nnExpandLeaf(
            Node node,
            DistanceResultNodeFilter filter,
            List<T> drs,
            int maxHits) {
        int size = (int) node.getProperty(PropertyNames.size);
        String[] keys = new String[size];
        for (int i = 0; i < size; i++) {
            keys[i] = PropertyNames.entryDataId + i;
        }
        Map<String, Object> properties = node.getProperties(keys);
        properties.forEach((k, v) -> {
            String dataNodeId = (String) v;
            T dr = createDistanceResult(pointNd, dataNodeId);
            double dist = dr.getDist();
            if (filter.accept(dr)) {
                int n = drs.size();
                if (n < maxHits || dist < drs.get(n - 1).getDist()) {
                    add(drs, dr, maxHits);
                }
            }
        });

    }

    private void add(List<T> drs,
                     T dr,
                     int maxHits) {
        int n = drs.size();
        if (n == maxHits)
            drs.remove(n - 1);
        int pos = Collections.binarySearch(drs, dr, comp);
        if (pos < 0) {
            // binarySearch return -(pos + 1) for new entries
            pos = -(pos + 1);
        }
        drs.add(pos, dr);
    }

    private static final Comparator<DistanceResult> comp =
            new Comparator<DistanceResult>() {
                public int compare(DistanceResult d1, DistanceResult d2) {
                    return Double.compare(d1.getDist(), d2.getDist());
                }
            };

}
