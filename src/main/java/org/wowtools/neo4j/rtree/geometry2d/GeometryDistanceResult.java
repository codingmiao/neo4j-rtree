package org.wowtools.neo4j.rtree.geometry2d;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.wowtools.neo4j.rtree.pojo.PointNd;
import org.wowtools.neo4j.rtree.util.DistanceResult;

/**
 * @author liuyu
 * @date 2021/12/31
 */
public class GeometryDistanceResult extends DistanceResult {

    private final Geometry geometry;

    public GeometryDistanceResult(double dist, long dataNodeId, Geometry geometry) {
        super(dist, dataNodeId);
        this.geometry = geometry;
    }

    public static GeometryDistanceResult newInstance(Transaction tx, String geometryName, WKBReader wkbReader, PointNd pointNd, long dataNodeId) {
        Node node = tx.getNodeById(dataNodeId);
        byte[] wkb = (byte[]) node.getProperty(geometryName);
        Geometry geometry;
        try {
            geometry = wkbReader.read(wkb);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        double[] coord = pointNd.getXs();
        Point point = Constant.geometryFactory.createPoint(new Coordinate(coord[0], coord[1]));
        double dist = geometry.distance(point);

        return new GeometryDistanceResult(dist, dataNodeId, geometry);
    }

    public Geometry getGeometry() {
        return geometry;
    }
}
