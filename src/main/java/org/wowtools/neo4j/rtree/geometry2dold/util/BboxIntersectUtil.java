package org.wowtools.neo4j.rtree.geometry2dold.util;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;

/**
 * bbox相交判断的工具类
 *
 * @author liuyu
 * @date 2020/7/9
 */
public class BboxIntersectUtil {

    /**
     * 判断两个矩形是否相交
     * https://blog.csdn.net/szfhy/article/details/49740191
     *
     * @param bbox1 [xmin,ymin,xmax,ymax]1
     * @param bbox2 [xmin,ymin,xmax,ymax]2
     * @return 是否相交
     */
    public static boolean bboxIntersect(double[] bbox1, double[] bbox2) {
        double x01 = bbox1[0], y01 = bbox1[1], x02 = bbox1[2], y02 = bbox1[3];
        double x11 = bbox2[0], y11 = bbox2[1], x12 = bbox2[2], y12 = bbox2[3];
        double zx = Math.abs(x01 + x02 - x11 - x12);
        double x = Math.abs(x01 - x02) + Math.abs(x11 - x12);
        double zy = Math.abs(y01 + y02 - y11 - y12);
        double y = Math.abs(y01 - y02) + Math.abs(y11 - y12);
        return (zx <= x && zy <= y);
    }


    /**
     * 判断点是否与bbox相交
     *
     * @param bbox [xmin,ymin,xmax,ymax]1
     * @param x    x
     * @param y    y
     * @return 是否相交
     */
    public static boolean pointInBbox(double[] bbox, double x, double y) {
        return bbox[0] <= x && x <= bbox[2]
                && bbox[1] <= y && y <= bbox[3];
    }

    /**
     * bbo转为geometry
     *
     * @param bbox bbox
     * @return geometry
     */
    public static Geometry bbox2Geometry(double[] bbox) {
        Coordinate[] shell = new Coordinate[5];
        double xmin = bbox[0], ymin = bbox[1], xmax = bbox[2], ymax = bbox[3];
        shell[0] = new Coordinate(xmin, ymin);
        shell[1] = new Coordinate(xmax, ymin);
        shell[2] = new Coordinate(xmax, ymax);
        shell[3] = new Coordinate(xmin, ymax);
        shell[4] = shell[0];
        Polygon bboxGeo = Singleton.geometryFactory.createPolygon(shell);
        return bboxGeo;
    }

}
