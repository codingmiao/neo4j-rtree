package org.wowtools.neo4j.rtree.geometry2d;


import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.wowtools.neo4j.rtree.Neo4jDbManager;
import org.wowtools.neo4j.rtree.RtreeNearestSearcherTest;
import org.wowtools.neo4j.rtree.util.TxCell;

import java.util.*;

public class Geometry2dRtreeNearestSearcherTest {
    private Neo4jDbManager neo4jDbManager;

    @Before
    public void before(){
        neo4jDbManager = new Neo4jDbManager();
    }

    @After
    public void after(){
        neo4jDbManager.afterTest();
    }
    @Test
    public void test() throws Exception {
        System.out.println("start");
        int num = 1234;//测试数据量 4
        String indexName = "testIndex";
        String geometryName = "geo";
        Random r = new Random(233);
        GeometryFactory geometryFactory = new GeometryFactory();
        WKBWriter wkbWriter = new WKBWriter();

        DataNodeCell[] dataNodeCells = new DataNodeCell[num];
        System.out.println("init success");
        // add
        try (Geometry2dRtreeEditor rtreeEditor = Geometry2dRtreeEditor.create(neo4jDbManager.getGraphDb(), 2000, indexName, 2, 8, geometryName)) {
            TxCell txCell = rtreeEditor.getTxCell();
            for (int i = 0; i < num; i++) {
                double x = r.nextDouble();
                double y = r.nextDouble();
                Geometry geometry = geometryFactory.createPoint(new Coordinate(x, y));
                geometry = geometry.buffer(r.nextDouble() * 0.00001);
                byte[] wkb = wkbWriter.write(geometry);
                Node dataNode = txCell.getTx().createNode();
                dataNode.setProperty(geometryName, wkb);
                DataNodeCell dataNodeCell = new DataNodeCell(dataNode.getId(), geometry);
                dataNodeCells[i] = dataNodeCell;
                rtreeEditor.add(dataNode.getId(), geometry);
            }
        }
        // end add

        double x = 0.5, y = 0.5;
        int hitNum = 10;
        List<GeometryDistanceResult> distanceResults;
        try (Transaction tx = neo4jDbManager.getGraphDb().beginTx()) {
            Geometry2dRtreeNearestSearcher geometry2dRtreeNearestSearcher = Geometry2dRtreeNearestSearcher.get(tx, indexName);
            distanceResults = geometry2dRtreeNearestSearcher.nearest(null, hitNum, x, y, tx);
        }
        Point point = new GeometryFactory().createPoint(new Coordinate(x, y));
        Arrays.sort(dataNodeCells, Comparator.comparingDouble(c -> c.geometry.distance(point)));
        for (int i = 0; i < hitNum; i++) {
            Assert.assertEquals(dataNodeCells[i].dataNodeId, distanceResults.get(i).getDataNodeId());
        }
    }

    private static final class DataNodeCell {
        private final long dataNodeId;
        private final Geometry geometry;

        public DataNodeCell(long dataNodeId, Geometry geometry) {
            this.dataNodeId = dataNodeId;
            this.geometry = geometry;
        }
    }
}
