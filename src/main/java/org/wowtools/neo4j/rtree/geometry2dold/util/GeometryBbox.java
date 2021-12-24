package org.wowtools.neo4j.rtree.geometry2dold.util;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.wowtools.neo4j.rtree.geometry2dold.spatial.Envelope;

/**
 * 获取geometry的bbox
 *
 * @author liuyu
 * @date 2020/5/31
 */
public class GeometryBbox {

    /**
     * bbox
     */
    public static final class Bbox {
        public final double xmin, xmax, ymin, ymax;

        private Bbox(double xmin, double xmax, double ymin, double ymax) {
            this.xmin = xmin;
            this.xmax = xmax;
            this.ymin = ymin;
            this.ymax = ymax;
        }

        public double[] toDoubleArray() {
            return new double[]{xmin, ymin, xmax, ymax};
        }

        public Envelope toEnvelope() {
            return new Envelope(xmin, xmax, ymin, ymax);
        }

    }

    /**
     * 获取bbox
     *
     * @param geometry geo
     * @return bbox
     */
    public static Bbox getBbox(Geometry geometry) {
        Geometry bound = geometry.getEnvelope();
        Coordinate[] coords = bound.getCoordinates();
        double xmin, xmax, ymin, ymax;
        if (coords.length > 1) {
            xmin = Double.MAX_VALUE;
            ymin = Double.MAX_VALUE;
            xmax = Double.MIN_VALUE;
            ymax = Double.MIN_VALUE;
            for (Coordinate coordinate : coords) {
                double x = coordinate.x;
                double y = coordinate.y;
                if (x < xmin) {
                    xmin = x;
                }
                if (y < ymin) {
                    ymin = y;
                }
                if (x > xmax) {
                    xmax = x;
                }
                if (y > ymax) {
                    ymax = y;
                }
            }
        } else {
            Coordinate coord = geometry.getCoordinate();
            xmin = coord.x;
            ymin = coord.y;
            xmax = coord.x;
            ymax = coord.y;
        }
        Bbox bbox = new Bbox(xmin, xmax, ymin, ymax);
        return bbox;
    }
}
