package org.wowtools.neo4j.rtree;

import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.operation.predicate.RectangleIntersects;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.wowtools.neo4j.rtree.nearest.DistanceResult;
import org.wowtools.neo4j.rtree.spatial.RTreeIndex;
import org.wowtools.neo4j.rtree.util.Singleton;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class QueryNearestTest {

    final String geometryFileName = "geo";
    final String wktFileName = "wkt";
    final int geoNum = 10000;
    final Label testLabel = Label.label("TestNode");

    private void testPoint(GraphDatabaseService db, RTreeIndex rTreeIndex) {

        //随机写入测试数据
        WKBWriter wkbWriter = new WKBWriter();

        try (Transaction tx = db.beginTx()) {
            List<Node> sidxList = new LinkedList<>();
            for (int x = 0; x < 100; x++) {
                for (int y = 0; y < 100; y++) {
                    Node node = tx.createNode(testLabel);//新建节点
                    Point geo = Singleton.geometryFactory.createPoint(new Coordinate(x, y));//构建一个geometry,POINT(x y)
                    byte[] wkb = wkbWriter.write(geo);//转为wkb
                    node.setProperty(geometryFileName, wkb);//设置空间字段值,必须为wkb格式
                    node.setProperty(wktFileName, geo.toText());//设置其他值(可选)
                    node.setProperty("idx", x + "," + y);
                    sidxList.add(node);
                }
            }
            rTreeIndex.add(sidxList, tx);//加入索引
            tx.commit();
        }

        //查询测试,全范围
        try (Transaction tx = db.beginTx()) {
            List<DistanceResult> res = RtreeNearestQuery.queryNearestN(tx, rTreeIndex, 10.2, 13.2, 4, (node, geometry) -> true);
            Assert.assertEquals(4, res.size());
            Assert.assertArrayEquals(new Object[]{
                    "10,13",
                    "10,14",
                    "11,13",
                    "11,14"
            }, new Object[]{
                    res.get(0).getNode().getProperty("idx"),
                    res.get(1).getNode().getProperty("idx"),
                    res.get(2).getNode().getProperty("idx"),
                    res.get(3).getNode().getProperty("idx")
            });
        }
        //查询测试,条件过滤
        try (Transaction tx = db.beginTx()) {
            List<DistanceResult> res = RtreeNearestQuery.queryNearestN(tx, rTreeIndex, 10.2, 13.2, 4, (node, geometry) -> geometry.getCoordinate().x < 10);
            Assert.assertEquals(4, res.size());
            Assert.assertArrayEquals(new Object[]{
                    "9,13",
                    "9,14",
                    "9,12",
                    "9,15"
            }, new Object[]{
                    res.get(0).getNode().getProperty("idx"),
                    res.get(1).getNode().getProperty("idx"),
                    res.get(2).getNode().getProperty("idx"),
                    res.get(3).getNode().getProperty("idx")
            });
        }
        System.out.println("查询完成");

    }


    @Test
    public void queryNearest() {
        GraphDatabaseService db = Neo4jDbManager.getGraphDb();
        RTreeIndex index;
        index = RTreeIndexManager.createIndex(db, "pointIdx", geometryFileName, 64, 1024);
        testPoint(db, index);
//        Scanner sin = new Scanner(System.in);
//        sin.next();
    }
}
