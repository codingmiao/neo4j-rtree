package org.wowtools.neo4j.rtree.geometry2dold;

import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.wowtools.neo4j.rtree.geometry2dold.spatial.RTreeIndex;
import org.wowtools.neo4j.rtree.geometry2dold.util.Singleton;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * 狭长型geometry相交查询测试
 *
 * @author liuyu
 * @date 2020/7/21
 */
public class StripGeometryIntersectsTest {

    final String geometryFileName = "geo";
    final String wktFileName = "wkt";
    final int geoNum = 100000;
    final Label testLabel = Label.label("TestNode");

    @Test
    public void test() {
        GraphDatabaseService db = Neo4jDbManager.getGraphDb();
        RTreeIndex rTreeIndex = RTreeIndexManager.createIndex(db, "polygonIdx", geometryFileName, 64,1024);

        Geometry inputGeo;
        try {
            //构造一条线，横跨整个范围[0,0,100,100]
            inputGeo = new WKTReader().read(
                    "LINESTRING (0 1, 6 7, 10 4, 19 11, 15 16, 23 18, 33 12, 35 22, 29 40, 37 40, 46 46, 46 52, 55 53, 58 63, 64 60, 69 52, 71 65, 69 77, 86 81, 82 89, 92 92, 96 96, 100 100)");
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        HashSet<String> intersectWkt = new HashSet<>();
        //随机写入测试数据(一堆范围内的三角形)
        WKBWriter wkbWriter = new WKBWriter();
        Random random = new Random(233);
        try (Transaction tx = db.beginTx()) {
            List<Node> sidxList = new LinkedList<>();
            for (int i = 0; i < geoNum; i++) {
                Node node = tx.createNode(testLabel);//新建节点
                Coordinate[] coords = new Coordinate[4];
                coords[0] = new Coordinate(random.nextDouble() * 100, random.nextDouble() * 100);
                coords[1] = new Coordinate(coords[0].x + random.nextDouble(), coords[0].y);
                coords[2] = new Coordinate(coords[1].x + random.nextDouble() - 0.5, coords[1].y + random.nextDouble());
                coords[3] = coords[0];
                Polygon geo = Singleton.geometryFactory.createPolygon(coords);
                byte[] wkb = wkbWriter.write(geo);//转为wkb
                node.setProperty(geometryFileName, wkb);//设置空间字段值,必须为wkb格式
                node.setProperty(wktFileName, geo.toText());//设置其他值(可选)
                node.setProperty("idx", i);
                sidxList.add(node);//把node放到一个list里，后续把list加入索引
                if (inputGeo.intersects(geo)) {
                    intersectWkt.add(geo.toText());
                }
            }
            rTreeIndex.add(sidxList, tx);//加入索引
            tx.commit();
        }
        System.out.println("数据构建完成,相交数:" + intersectWkt.size());

        HashSet<String> testIntersectWkt = new HashSet<>();

        //查询测试(预热)
        testIntersectWkt.addAll(intersectWkt);
        try (Transaction tx = db.beginTx()) {
            RtreeQuery.queryByGeometryIntersects(tx, rTreeIndex, inputGeo, (node, geometry) -> {
                testIntersectWkt.remove(node.getProperty(wktFileName));
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (String wkt : testIntersectWkt) {
            System.out.println(wkt);
        }
        assertEquals(0, testIntersectWkt.size());
        System.out.println("预热完成");

        long t;
        //查询测试(queryByStripGeometryIntersects)
        testIntersectWkt.clear();
        testIntersectWkt.addAll(intersectWkt);
        t = System.currentTimeMillis();
        try (Transaction tx = db.beginTx()) {
            RtreeQuery.queryByStripGeometryIntersects(tx, rTreeIndex, inputGeo, (node, geometry) -> {
                testIntersectWkt.remove(node.getProperty(wktFileName));
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (String wkt : testIntersectWkt) {
            System.out.println(wkt);
        }
        assertEquals(0, testIntersectWkt.size());
        System.out.println("queryByStripGeometryIntersects完成,cost:" + (System.currentTimeMillis() - t));

        //查询测试(queryByGeometryIntersects)
        testIntersectWkt.clear();
        testIntersectWkt.addAll(intersectWkt);
        t = System.currentTimeMillis();
        try (Transaction tx = db.beginTx()) {
            RtreeQuery.queryByGeometryIntersects(tx, rTreeIndex, inputGeo, (node, geometry) -> {
                testIntersectWkt.remove(node.getProperty(wktFileName));
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (String wkt : testIntersectWkt) {
            System.out.println(wkt);
        }
        assertEquals(0, testIntersectWkt.size());
        System.out.println("queryByGeometryIntersects完成,cost:" + (System.currentTimeMillis() - t));
        Neo4jDbManager.getServer().stop();

    }
}
