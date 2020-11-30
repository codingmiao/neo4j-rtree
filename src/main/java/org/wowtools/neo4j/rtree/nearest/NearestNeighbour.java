package org.wowtools.neo4j.rtree.nearest;

/**
 * @author liuyu
 * @date 2020/6/12
 */

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKBReader;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.wowtools.neo4j.rtree.Constant;
import org.wowtools.neo4j.rtree.RtreeNearestQuery;
import org.wowtools.neo4j.rtree.spatial.RTreeIndex;
import org.wowtools.neo4j.rtree.util.Singleton;

import java.util.*;

public class NearestNeighbour {

    private final RtreeNearestQuery.NodeFilter filter;
    private final int maxHits;
    private final Node root;
    private final double x;
    private final double y;
    private final Point point;
    private final RTreeIndex rTreeIndex;
    private final WKBReader wkbReader = new WKBReader();

    public NearestNeighbour(RtreeNearestQuery.NodeFilter filter, int maxHits, Node root, double x, double y, RTreeIndex rTreeIndex) {
        this.filter = filter;
        this.maxHits = maxHits;
        this.root = root;
        this.x = x;
        this.y = y;
        this.rTreeIndex = rTreeIndex;
        point = Singleton.geometryFactory.createPoint(new Coordinate(x, y));
    }

    /**
     * @return the nearest neighbour
     */
    public List<DistanceResult> find() {
        List<DistanceResult> ret =
                new ArrayList<>(maxHits);
        MinDistComparator nc =
                new MinDistComparator(x, y);
        PriorityQueue<Node> queue = new PriorityQueue<Node>(20, nc);
        queue.add(root);
        while (!queue.isEmpty()) {
            Node n = queue.remove();
            if (n.hasRelationship(Direction.OUTGOING, Constant.Relationship.RTREE_CHILD)) {
                nnExpandInternal(n, ret, maxHits, queue, nc);
            } else if (n.hasRelationship(Direction.OUTGOING, Constant.Relationship.RTREE_REFERENCE)) {
                nnExpandLeaf(n, filter, ret, maxHits);
            }
        }
        return ret;
    }


    /**
     * 访问索引上的非叶子节点
     *
     * @param node
     * @param drs
     * @param maxHits
     * @param queue
     * @param mdc
     */
    private void nnExpandInternal(Node node,
                                  List<DistanceResult> drs,
                                  int maxHits,
                                  PriorityQueue<Node> queue,
                                  MinDistComparator mdc) {
        double[] bbox = (double[]) node.getProperty(Constant.RtreeProperty.bbox);
        for (Relationship relationship : node.getRelationships(Direction.OUTGOING, Constant.Relationship.RTREE_CHILD)) {
            Node n = relationship.getEndNode();
            double minDist = MinDist.get(bbox, x, y);
            int t = drs.size();
            // drs is sorted so we can check only the last entry
            if (t < maxHits || minDist <= drs.get(t - 1).getDist())
                queue.add(n);
        }
    }

    /**
     * 访问索引上的叶子节点
     *
     * @param node
     * @param filter
     * @param drs
     * @param maxHits
     */
    private void nnExpandLeaf(
            Node node,
            RtreeNearestQuery.NodeFilter filter,
            List<DistanceResult> drs,
            int maxHits) {
        for (Relationship relationship : node.getRelationships(Direction.OUTGOING, Constant.Relationship.RTREE_REFERENCE)) {
            Node objNode = relationship.getEndNode();
            Geometry geometry = rTreeIndex.getObjNodeGeometry(objNode, wkbReader);
            if (filter.accept(objNode, geometry)) {
                double dist = geometry.distance(point);
                int n = drs.size();
                if (n < maxHits || dist < drs.get(n - 1).getDist()) {
                    add(drs, new DistanceResult(objNode, dist, geometry), maxHits);
                }
            }
        }
    }

    private void add(List<DistanceResult> drs,
                     DistanceResult dr,
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
