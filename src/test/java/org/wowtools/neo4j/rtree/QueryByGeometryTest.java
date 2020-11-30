package org.wowtools.neo4j.rtree;

import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.operation.predicate.RectangleIntersects;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.wowtools.neo4j.rtree.spatial.RTreeIndex;
import org.wowtools.neo4j.rtree.util.Singleton;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class QueryByGeometryTest {

    final String geometryFileName = "geo";
    final String wktFileName = "wkt";
    final int geoNum = 10000;
    final Label testLabel = Label.label("TestNode");

    private void testPoint(GraphDatabaseService db, RTreeIndex rTreeIndex) {
        HashSet<String> intersectWkt = new HashSet<>();
        Geometry inputGeo;
        try {
            inputGeo = new WKTReader().read("POLYGON ((11 24, 22 28, 29 15, 11 24))");
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        //随机写入测试数据
        WKBWriter wkbWriter = new WKBWriter();
        Random random = new Random(233);
        try (Transaction tx = db.beginTx()) {
            List<Node> sidxList = new LinkedList<>();
            for (int i = 0; i < geoNum; i++) {
                Node node = tx.createNode(testLabel);//新建节点
                Point geo = Singleton.geometryFactory.createPoint(new Coordinate(random.nextDouble() * 100, random.nextDouble() * 100));//构建一个geometry,POINT(x y)
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

        //查询测试
        try (Transaction tx = db.beginTx()) {
            rTreeIndex = RTreeIndexManager.getIndex(db,"pointIdx",1024);
            RtreeQuery.queryByGeometryIntersects(tx, rTreeIndex, inputGeo, (node, geometry) -> {
                intersectWkt.remove(node.getProperty(wktFileName));
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (String wkt : intersectWkt) {
            System.out.println(wkt);
        }
        assertEquals(0, intersectWkt.size());
        System.out.println("查询完成");

    }

    private void testLine(GraphDatabaseService db, RTreeIndex rTreeIndex) {
        Geometry inputGeo;
        try {
            inputGeo = new WKTReader().read("POLYGON ((11 24, 22 28, 29 15, 11 24))");
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        HashSet<String> intersectWkt = new HashSet<>();

        //随机写入测试数据
        WKBWriter wkbWriter = new WKBWriter();
        Random random = new Random(233);
        try (Transaction tx = db.beginTx()) {
            List<Node> sidxList = new LinkedList<>();
            for (int i = 0; i < geoNum; i++) {
                Node node = tx.createNode(testLabel);//新建节点
                int lineNum = random.nextInt(5);
                if (lineNum < 2) {
                    lineNum = 2;
                }
                Coordinate[] coords = new Coordinate[lineNum];
                for (int i1 = 0; i1 < lineNum; i1++) {
                    coords[i1] = new Coordinate(random.nextDouble() * 100, random.nextDouble() * 100);
                }
                LineString geo = Singleton.geometryFactory.createLineString(coords);
                byte[] wkb = wkbWriter.write(geo);//转为wkb
                node.setProperty(geometryFileName, wkb);//设置空间字段值,必须为wkb格式
                node.setProperty(wktFileName, geo.toText());//设置其他值(可选)
                node.setProperty("idx", i);
                sidxList.add(node);//把node放到一个list里，后续把list加入索引
//                rTreeIndex.add(node,tx);
                if (inputGeo.intersects(geo)) {
                    intersectWkt.add(geo.toText());
                }
            }
            rTreeIndex.add(sidxList, tx);//加入索引
            tx.commit();
        }
        System.out.println("数据构建完成,相交数:" + intersectWkt.size());

        //查询测试
        try (Transaction tx = db.beginTx()) {
            RtreeQuery.queryByGeometryIntersects(tx, rTreeIndex, inputGeo, (node, geometry) -> {
                intersectWkt.remove(node.getProperty(wktFileName));
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (String wkt : intersectWkt) {
            System.out.println(wkt);
        }
//        assertEquals(0, intersectWkt.size());
        System.out.println("查询完成");
    }

    private void testPolygon(GraphDatabaseService db, RTreeIndex rTreeIndex) {
        Geometry inputGeo;
        try {
            inputGeo = new WKTReader().read("POLYGON ((11 24, 22 28, 29 15, 11 24))");
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        HashSet<String> intersectWkt = new HashSet<>();
        //随机写入测试数据
        WKBWriter wkbWriter = new WKBWriter();
        Random random = new Random(233);
        try (Transaction tx = db.beginTx()) {
            List<Node> sidxList = new LinkedList<>();
            for (int i = 0; i < geoNum; i++) {
                Node node = tx.createNode(testLabel);//新建节点
                int lineNum = random.nextInt(10);
                if (lineNum < 4) {
                    lineNum = 4;
                }
                Coordinate[] coords = new Coordinate[lineNum];
                for (int i1 = 0; i1 < lineNum -1 ; i1++) {
                    coords[i1] = new Coordinate(random.nextDouble() * 100, random.nextDouble() * 100);
                }
                coords[lineNum-1] = coords[0];
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

        //查询测试
        try (Transaction tx = db.beginTx()) {
            RtreeQuery.queryByGeometryIntersects(tx, rTreeIndex, inputGeo, (node, geometry) -> {
                intersectWkt.remove(node.getProperty(wktFileName));
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (String wkt : intersectWkt) {
            System.out.println(wkt);
        }
        assertEquals(0, intersectWkt.size());
        System.out.println("查询完成");
    }

    @Test
    public void queryByBbox() {
        GraphDatabaseService db = Neo4jDbManager.getGraphDb();
        RTreeIndex index;
        //在[0,0,100,100]中随机生成点线并测试
        index = RTreeIndexManager.createIndex(db, "pointIdx", geometryFileName, 64,1024);
        testPoint(db, index);
        index = RTreeIndexManager.createIndex(db, "lineIdx", geometryFileName, 64,1024);
        testLine(db, index);
        index = RTreeIndexManager.createIndex(db, "polygonIdx", geometryFileName, 64,1024);
        testPolygon(db, index);
//        Scanner sin = new Scanner(System.in);
//        sin.next();
    }
}
