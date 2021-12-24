package org.wowtools.neo4j.rtree.pojo;

import org.wowtools.neo4j.rtree.internal.edit.HyperPoint;

/**
 * n维点
 * @author liuyu
 * @date 2021/12/17
 */
public class PointNd implements HyperPoint {
    private double[] xs;

    public PointNd(double[] xs) {
        this.xs = xs;
    }

    @Override
    public int getNDim() {
        return xs.length;
    }

    @Override
    public Double getCoord(int d) {
        if (d < xs.length) {
            return xs[d];
        } else {
            throw new IllegalArgumentException("Invalid dimension");
        }
    }

    @Override
    public double distance(HyperPoint p) {
        double d = 0;
        for (int i = 0; i < p.getNDim(); i++) {
            double d1 = getCoord(i);
            double d2 = p.getCoord(i);
            double sub = d1 - d2;
            sub = sub * sub;
            d += sub;
        }
        return Math.sqrt(d);
    }

    @Override
    public double distance(HyperPoint p, int d) {
        double d1 = getCoord(d);
        double d2 = p.getCoord(d);
        return Math.abs(d1 - d2);
    }

    public double[] getXs() {
        return xs;
    }
}
