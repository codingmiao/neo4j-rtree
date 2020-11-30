package org.wowtools.neo4j.rtree.bigshape.util;

import org.locationtech.jts.geom.*;
import org.wowtools.neo4j.rtree.bigshape.pojo.Grid;
import org.wowtools.neo4j.rtree.util.GeometryBbox;
import org.wowtools.neo4j.rtree.util.Singleton;

import java.util.LinkedList;
import java.util.List;

/**
 * 使用网格切割复杂geometry
 *
 * @author liuyu
 * @date 2020/11/23
 */
public class GridCuter {

    /**
     * 按geometry的bbox分隔为若干行列的网格，并范围网格每个格子与geometry的交集
     *
     * @param geometry
     * @param row
     * @param column
     * @return 相交的网格
     */
    public static List<Grid> cut(Geometry geometry, int row, int column) {
        //会有一些拓扑错误，而处理拓扑比较花时间，所以try尝试一下
        try {
            return _cut(geometry, row, column);
        } catch (TopologyException e) {
            try {
                geometry = SelfIntersectingDispose.validate(geometry);//处理掉自相交
                return _cut(geometry, row, column);
            } catch (TopologyException e1) {
                geometry = geometry.buffer(0);
                geometry = SelfIntersectingDispose.validate(geometry);//处理掉自相交
                return _cut(geometry, row, column);
            }
        }
    }

    private static List<Grid> _cut(Geometry geometry, int row, int column) {
        GeometryBbox.Bbox bbox = GeometryBbox.getBbox(geometry);
        double width = (bbox.xmax - bbox.xmin) / column;
        double height = (bbox.ymax - bbox.ymin) / row;
        double xmin = bbox.xmin, ymin, xmax = xmin + width, ymax;
        Coordinate c0, c1, c2, c3;//网格的四个顶点,左下角起逆时针
        List<Grid> res = new LinkedList<>();
        //按从左下角逐行上移、逐列右移的方式遍历所有格子并进行相交分析
        do {
            ymin = bbox.ymin;
            ymax = ymin + height;
            c0 = new Coordinate(xmin, ymin);
            c1 = new Coordinate(xmax, ymin);
            c2 = new Coordinate(xmax, ymax);
            c3 = new Coordinate(xmin, ymax);
            do {
                Polygon bboxGeometry = Singleton.geometryFactory.createPolygon(new Coordinate[]{
                        c0, c1, c2, c3, c0
                });
                Geometry intersectionGeometry = geometry.intersection(bboxGeometry);
                if (!intersectionGeometry.isEmpty()) {
                    //判断相交结果是否就是bbox本身
                    boolean eq = false;
                    if (intersectionGeometry instanceof Polygon) {
                        Polygon polygon = (Polygon) intersectionGeometry;
                        if (polygon.getExteriorRing().getNumPoints() == 5) {
                            eq = intersectionGeometry.equalsTopo(bboxGeometry);
                        }
                    }
                    res.add(new Grid(intersectionGeometry, eq));
                }
//                res.add(new Grid(bboxGeometry, true));//测试代码
                /*
                 * 上移一格
                 *          c2 c3
                 * c3 c2 -> c0 c1
                 * c0 c1
                 * */
                ymin = ymax;
                ymax += height;
                c0 = c3;
                c1 = c2;
                c2 = new Coordinate(xmax, ymax);
                c3 = new Coordinate(xmin, ymax);
            } while (ymax <= bbox.ymax);
            xmin = xmax;
            xmax += width;
            /*
             * 右移一格
             * c3 c2
             * c0 c1
             *    |
             *    c3 c2
             *    c0 c1
             * */
            c0 = c1;
            c3 = c2;
            c1 = new Coordinate(xmax, ymin);
            c2 = new Coordinate(xmax, ymax);
        } while (xmax <= bbox.xmax);
        return res;
    }

}
