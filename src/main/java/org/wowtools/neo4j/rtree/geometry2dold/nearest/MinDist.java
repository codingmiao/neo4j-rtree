package org.wowtools.neo4j.rtree.geometry2dold.nearest;

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
     * @param bbox the bounding box to use
     * @param x    the point x
     * @param y    the point y
     * @return the squared distance
     */
    public static double get(double[] bbox, double x, double y) {
        double res =
                dd(x, bbox[0], bbox[2]) +
                        dd(y, bbox[1], bbox[3]);
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
