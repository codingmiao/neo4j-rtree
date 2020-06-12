package org.wowtools.neo4j.rtree.nearest;

import org.neo4j.graphdb.Node;
import org.wowtools.neo4j.rtree.Constant;

import java.util.Comparator;

/**
 * A comparator that uses the MINDIST metrics to sort Nodes
 *
 * @author liuyu
 * @date 2020/6/12
 */
public class MinDistComparator implements Comparator<Node> {
    public final double x;
    public final double y;

    public MinDistComparator(double x, double y) {
        this.x = x;
        this.y = y;
    }


    @Override
    public int compare(Node n1, Node n2) {
        double[] bbox1 = (double[]) n1.getProperty(Constant.RtreeProperty.bbox);
        double[] bbox2 = (double[]) n2.getProperty(Constant.RtreeProperty.bbox);
        return Double.compare(MinDist.get(bbox1, x, y),
                MinDist.get(bbox2, x, y));
    }
}
