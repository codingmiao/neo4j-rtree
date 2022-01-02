package org.wowtools.neo4j.rtree.internal.nearest;

import org.wowtools.neo4j.rtree.pojo.PointNd;

/**
 * Class that can calculate the MINDIST between a point and a rectangle
 */
public class MinDist {
    /**
     * Do not instantiate
     */
    private MinDist() {
        // empty
    }

    /**
     * 距离的平方
     * Calculate the MINDIST between the given MBRND and the given point
     *
     * @param mins    the min bounding box to use
     * @param maxs    the max bounding box to use
     * @param pointNd the point
     * @return the squared distance
     */
    public static double get(double[] mins, double[] maxs, PointNd pointNd) {
        double[] xs = pointNd.getXs();
        double res = 0;
        for (int i = 0; i < xs.length; i++) {
            res += dd(xs[i], mins[i], maxs[i]);
        }
        return res;
    }

    /**
     * 某个维度上距离的平方
     *
     * @param o
     * @param min
     * @param max
     * @return
     */
    private static double dd(double o, double min, double max) {
        double rv = r(o, min, max);
        double dr = o - rv;
        return dr * dr;
    }

    private static double r(double x, double min, double max) {
        double r = x;
        if (x < min)
            r = min;
        if (x > max)
            r = max;
        return r;
    }
}
