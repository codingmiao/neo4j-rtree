package org.wowtools.neo4j.rtree.bigshape.intersects;

import junit.framework.TestCase;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.io.WKTReader;
import org.neo4j.graphdb.Transaction;
import org.wowtools.common.utils.ResourcesReader;
import org.wowtools.neo4j.rtree.Neo4jDbManager;
import org.wowtools.neo4j.rtree.bigshape.BigShapeManager;
import org.wowtools.neo4j.rtree.bigshape.pojo.BigShape;
import org.wowtools.neo4j.rtree.bigshape.util.GridCuterTest1;
import org.wowtools.neo4j.rtree.bigshape.util.SelfIntersectingDispose;
import org.wowtools.neo4j.rtree.util.GeometryBbox;

import java.util.Random;

public class PolygonIntersectsTest extends TestCase {
    private static final int testNum = 200;
    private static final String indexId = "1";

    @Test
    public void test() throws Exception {
        /*
        放了两个测试用的wkt,wkt1.txt和wkt2.txt
        wkt1复杂，用来测性能，性能比jts直接计算要快几百倍
        wkt2简单，用来快速验证正确性，由于wkt2太简单了，切割后性能反而不如jts直接计算
        * */
        String wkt = ResourcesReader.readStr(GridCuterTest1.class, "wkt2.txt");
        WKTReader wktReader = new WKTReader();
        Geometry geometry = wktReader.read(wkt);
        testBuild(geometry);
        geometry = geometry.buffer(0);
        geometry = SelfIntersectingDispose.validate(geometry);
        testQuery(geometry);
        testSpeed(geometry);
//        Scanner sin = new Scanner(System.in);
//        sin.next();
    }

    private void testBuild(Geometry geometry) throws Exception {
        BigShapeManager.build(Neo4jDbManager.getGraphDb(), indexId, geometry, 10, 10);
        System.out.println("build success");
    }

    private void testQuery(Geometry geometry) throws Exception {
        BigShape bigShape = BigShapeManager.get(Neo4jDbManager.getGraphDb(), indexId);
        GeometryFactory geometryFactory = new GeometryFactory();
        GeometryBbox.Bbox bbox = GeometryBbox.getBbox(geometry);
        Random random = new Random(233);
        try (Transaction tx = Neo4jDbManager.getGraphDb().beginTx()) {
            for (int i = 0; i < testNum; i++) {
                Coordinate c0 = new Coordinate(
                        bbox.xmin + random.nextFloat() * (bbox.xmax - bbox.xmin),
                        bbox.ymin + random.nextFloat() * (bbox.ymax - bbox.ymin)
                );
                Geometry geo = geometryFactory.createPoint(c0).buffer(random.nextFloat() * 0.05 * (bbox.xmax - bbox.xmin));
                boolean r1 = bigShape.intersects(tx, geo);
                boolean r2 = geometry.intersects(geo);
                System.out.println(r2 + "\t" + geo.toText());
                assertEquals(r2, r1);
            }
        }

    }

    private void testQuery1(Geometry geometry) throws Exception {
        BigShape bigShape = BigShapeManager.get(Neo4jDbManager.getGraphDb(), indexId);
        Geometry s1 = new WKTReader().read("POINT (866.8184900283813 132.99309968948364)");
        try (Transaction tx = Neo4jDbManager.getGraphDb().beginTx()) {
            System.out.println(bigShape.intersects(tx, s1));
        }
    }

    private void testSpeed(Geometry geometry) {
        BigShape bigShape = BigShapeManager.get(Neo4jDbManager.getGraphDb(), indexId);
        GeometryFactory geometryFactory = new GeometryFactory();
        GeometryBbox.Bbox bbox = GeometryBbox.getBbox(geometry);
        Random random = new Random(233);
        Geometry[] geos = new Geometry[testNum];
        for (int i = 0; i < testNum; i++) {
            Coordinate c0 = new Coordinate(
                    bbox.xmin + random.nextFloat() * (bbox.xmax - bbox.xmin),
                    bbox.ymin + random.nextFloat() * (bbox.ymax - bbox.ymin)
            );
            Geometry geo = geometryFactory.createPoint(c0).buffer(random.nextFloat() * 0.05 * (bbox.xmax - bbox.xmin));
            geos[i] = geo;
        }
        long t;
        t = System.currentTimeMillis();
        for (Geometry geo : geos) {
            geometry.intersects(geo);
        }
        System.out.println("jts cost:" + (System.currentTimeMillis() - t));
        Transaction tx = Neo4jDbManager.getGraphDb().beginTx();
        t = System.currentTimeMillis();
        for (Geometry geo : geos) {
            bigShape.intersects(tx, geo);
        }
        System.out.println("bigshape cost:" + (System.currentTimeMillis() - t));
        tx.close();
    }

}
