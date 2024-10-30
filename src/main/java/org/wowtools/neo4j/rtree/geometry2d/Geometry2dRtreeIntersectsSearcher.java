package org.wowtools.neo4j.rtree.geometry2d;

import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.operation.predicate.RectangleIntersects;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.wowtools.neo4j.rtree.RtreeIntersectsSearcher;
import org.wowtools.neo4j.rtree.pojo.RectNd;
import org.wowtools.neo4j.rtree.util.BooleanDataNodeVisitor;

/**
 * 二维geometry 相交关系查询器
 *
 * @author liuyu
 * @date 2021/12/28
 */
public class Geometry2dRtreeIntersectsSearcher {

    private final String geometryName;
    private final RtreeIntersectsSearcher rtreeIntersectsSearcher;

    private Geometry2dRtreeIntersectsSearcher(String geometryName, RtreeIntersectsSearcher rtreeIntersectsSearcher) {
        this.geometryName = geometryName;
        this.rtreeIntersectsSearcher = rtreeIntersectsSearcher;
    }


    /**
     * 获取查询器
     *
     * @param tx   事务 此事务需要在外部手动关闭
     * @param name 索引名
     * @return Geometry2dRtreeIntersectsSearcher
     */
    public static Geometry2dRtreeIntersectsSearcher get(Transaction tx, String name) {
        RtreeIntersectsSearcher rtreeIntersectsSearcher = RtreeIntersectsSearcher.get(tx, name);
        String geometryName = (String) tx.getNodeByElementId(rtreeIntersectsSearcher.getMetadataNodeId()).getProperty("geometryName");
        return new Geometry2dRtreeIntersectsSearcher(geometryName, rtreeIntersectsSearcher);
    }

    /**
     * 相交查询
     *
     * @param bbox    查询的bbox范围
     * @param tx      事务 此事务需要在外部手动关闭
     * @param visitor 结果访问器
     */
    public void intersects(RectNd bbox, Transaction tx, BooleanGeometryDataNodeVisitor visitor) {
        double[] max = bbox.getMaxXs();
        double[] min = bbox.getMinXs();
        double xmin = min[0], ymin = min[1], xmax = max[0], ymax = max[1];
        Coordinate[] coords = new Coordinate[]{
                new Coordinate(xmin, ymin),
                new Coordinate(xmax, ymin),
                new Coordinate(xmax, ymax),
                new Coordinate(xmin, ymax),
                null
        };
        coords[4] = coords[0];
        Geometry geometry = new GeometryFactory().createPolygon(coords);
        GeometryBooleanDataNodeVisitor geoIntersects = new GeometryBooleanDataNodeVisitor(geometry, tx, visitor, geometryName);
        rtreeIntersectsSearcher.intersects(bbox, tx, geoIntersects);
    }

    /**
     * 相交查询
     *
     * @param geometry 检查是否与此geometry相交
     * @param tx       事务 此事务需要在外部手动关闭
     * @param visitor  结果访问器
     */
    public void intersects(Geometry geometry, Transaction tx, BooleanGeometryDataNodeVisitor visitor) {
        RectNd bbox = GeometryBbox.getBbox(geometry).toRect2d();
        GeometryBooleanDataNodeVisitor geoIntersects = new GeometryBooleanDataNodeVisitor(geometry, tx, visitor, geometryName);
        rtreeIntersectsSearcher.intersects(bbox, tx, geoIntersects);
    }

    private static final class GeometryBooleanDataNodeVisitor implements BooleanDataNodeVisitor {
        private final Geometry geometry;
        private final Transaction tx;
        private final BooleanGeometryDataNodeVisitor visitor;
        private final WKBReader wkbReader = new WKBReader();
        private final String geometryName;
        private final boolean isRectangle;
        private final boolean isGeometryCollection;

        public GeometryBooleanDataNodeVisitor(Geometry geometry, Transaction tx, BooleanGeometryDataNodeVisitor visitor, String geometryName) {
            this.geometry = geometry;
            this.tx = tx;
            this.visitor = visitor;
            this.geometryName = geometryName;
            isRectangle = geometry.isRectangle();

            isGeometryCollection = geometry instanceof GeometryCollection;

        }

        @Override
        public boolean visit(String nodeId) {
            Geometry nodeGeometry;
            try {
                Node node = null;
                try {
                    node = tx.getNodeByElementId(nodeId);
                } catch (NotFoundException e) {
                }
                if (null == node) {
                    return false;
                }
                byte[] wkb = (byte[]) node.getProperty(geometryName,null);
                if (null == wkb) {
                    return false;
                }
                nodeGeometry = wkbReader.read(wkb);
            } catch (Exception e) {
                throw new RuntimeException("解析node的geometry数据出错 ,节点id " + nodeId + " ,字段名" + geometryName, e);
            }
            if (intersects(nodeGeometry)) {
                return visitor.visit(nodeId, nodeGeometry);
            }
            return false;
        }

        /**
         * 针对此场景优化了Geometry.intersects方法 getEnvelopeInternal的相交判断，因为这个已经在rtree里判断过了
         * 一些可以在初始化时判断的东西也提前做掉了，减少intersects时的计算量
         *
         * @param g 被判断的geometry
         * @return 是否相交
         * @see Geometry
         */
        private boolean intersects(Geometry g) {

            // optimization for rectangle arguments
            if (isRectangle) {
                return RectangleIntersects.intersects((Polygon) geometry, g);
            }
            if (g.isRectangle()) {
                return RectangleIntersects.intersects((Polygon) g, geometry);
            }
            if (isGeometryCollection || g instanceof GeometryCollection) {
                for (int i = 0; i < geometry.getNumGeometries(); i++) {
                    for (int j = 0; j < g.getNumGeometries(); j++) {
                        if (geometry.getGeometryN(i).intersects(g.getGeometryN(j))) {
                            return true;
                        }
                    }
                }
                return false;
            }
            // general case
            return geometry.relate(g).isIntersects();
        }
    }
}
