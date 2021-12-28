package org.wowtools.neo4j.rtree.internal.nearest;

import org.neo4j.graphdb.Node;
import org.wowtools.neo4j.rtree.pojo.PointNd;

import java.util.Comparator;
import java.util.Map;

/**
 * A comparator that uses the MINDIST metrics to sort Nodes
 *
 * @author liuyu
 * @date 2020/6/12
 */
public class MinDistComparator implements Comparator<Node> {
    private final PointNd pointNd;

    public MinDistComparator(PointNd pointNd) {
        this.pointNd = pointNd;
    }

    @Override
    public int compare(Node n1, Node n2) {
        Map<String, Object> properties = n1.getProperties("mbrMax", "mbrMin");
        double[] mins1 = (double[]) properties.get("mbrMin");
        double[] maxs1 = (double[]) properties.get("mbrMax");

        properties = n2.getProperties("mbrMax", "mbrMin");
        double[] mins2 = (double[]) properties.get("mbrMin");
        double[] maxs2 = (double[]) properties.get("mbrMax");

        return Double.compare(MinDist.get(mins1, maxs1, pointNd),
                MinDist.get(mins2, maxs2, pointNd));
    }

    public PointNd getPointNd() {
        return pointNd;
    }
}
