package org.wowtools.neo4j.rtree.geometry2d;


import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.wowtools.neo4j.rtree.Neo4jDbManager;
import org.wowtools.neo4j.rtree.internal.edit.TxCell;

import java.util.ArrayList;
import java.util.Random;

public class Geometry2dRtreeEditorTest {
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
        Geometry inputGeometry = new WKTReader().read("POLYGON ((0.20 0.40, 0.30 0.90, 0.57 0.75, 0.55 0.55, 0.20 0.40))");
        int num = 1234;//测试数据量 4
        String indexName = "testIndex";
        String geometryName = "geo";
        int resNum = 0;//手算出来的相交数
        Random r = new Random(233);
        GeometryFactory geometryFactory = new GeometryFactory();
        WKBWriter wkbWriter = new WKBWriter();
        WKBReader wkbReader = new WKBReader();

        DataNodeCell[] dataNodeCells = new DataNodeCell[num];
        ArrayList<DataNodeCell> intersectDataNodeCells = new ArrayList<>();
        System.out.println("init success");
        // add
        try (Geometry2dRtreeEditor rtreeEditor = Geometry2dRtreeEditor.create(neo4jDbManager.getGraphDb(), 2000, indexName, 2, 8, geometryName)) {
            TxCell txCell = rtreeEditor.getTxCell();
            for (int i = 0; i < num; i++) {
                double x = r.nextDouble();
                double y = r.nextDouble();
                Geometry geometry = geometryFactory.createPoint(new Coordinate(x, y));
                geometry = geometry.buffer(r.nextDouble() * 0.1);
                byte[] wkb = wkbWriter.write(geometry);
                Node dataNode = txCell.getTx().createNode();
                dataNode.setProperty(geometryName, wkb);
                DataNodeCell dataNodeCell = new DataNodeCell(dataNode.getElementId(), geometry);
                if (inputGeometry.intersects(geometry)) {
                    intersectDataNodeCells.add(dataNodeCell);
                    resNum++;
                }
                dataNodeCells[i] = dataNodeCell;
                rtreeEditor.add(dataNode.getElementId());
            }
        }
        MyVisitor myVisitor = new MyVisitor();
        try (Transaction tx = neo4jDbManager.getGraphDb().beginTx()) {
            Geometry2dRtreeIntersectsSearcher searcher = Geometry2dRtreeIntersectsSearcher.get(tx, indexName);
            searcher.intersects(inputGeometry, tx, myVisitor);
        }
        System.out.println("add " + myVisitor.num);
        Assert.assertEquals(resNum, myVisitor.num);
        // end add

        // remove
        int removeNum = (int) (resNum * 0.4);//移除40%的数据
        resNum -= removeNum;
        try (Geometry2dRtreeEditor rtreeEditor = Geometry2dRtreeEditor.get(neo4jDbManager.getGraphDb(), 2000, indexName)) {
            for (int i = 0; i < removeNum; i++) {
                DataNodeCell dataNodeCell = intersectDataNodeCells.get(i);
                rtreeEditor.remove(dataNodeCell.dataNodeId);
            }
        }
        myVisitor = new MyVisitor();
        try (Transaction tx = neo4jDbManager.getGraphDb().beginTx()) {
            Geometry2dRtreeIntersectsSearcher searcher = Geometry2dRtreeIntersectsSearcher.get(tx, indexName);
            searcher.intersects(inputGeometry, tx, myVisitor);
        }
        System.out.println("remove " + myVisitor.num);
        Assert.assertEquals(resNum, myVisitor.num);
        // end remove

        // update
        int updateStart = removeNum + 1;
        try (Geometry2dRtreeEditor rtreeEditor = Geometry2dRtreeEditor.get(neo4jDbManager.getGraphDb(), 2000, indexName)) {
            TxCell txCell = rtreeEditor.getTxCell();
            for (int i = updateStart; i < intersectDataNodeCells.size(); i++) {
                DataNodeCell intersectDataNodeCell = intersectDataNodeCells.get(i);

                Node dataNode = txCell.getTx().getNodeByElementId(intersectDataNodeCell.dataNodeId);

                byte[] oldWkb = (byte[]) dataNode.getProperty(geometryName);
                Geometry oldGeometry = wkbReader.read(oldWkb);

                double x = r.nextDouble();
                double y = r.nextDouble();
                Geometry newGeometry = geometryFactory.createPoint(new Coordinate(x, y));
                newGeometry = newGeometry.buffer(r.nextDouble() * 0.1);
                byte[] wkb = wkbWriter.write(newGeometry);
                dataNode.setProperty(geometryName, wkb);

                if (!inputGeometry.intersects(newGeometry)) {
                    resNum--;
                }

                rtreeEditor.update(intersectDataNodeCell.dataNodeId, oldGeometry);
            }
        }

        myVisitor = new MyVisitor();
        try (Transaction tx = neo4jDbManager.getGraphDb().beginTx()) {
            Geometry2dRtreeIntersectsSearcher searcher = Geometry2dRtreeIntersectsSearcher.get(tx, indexName);
            searcher.intersects(inputGeometry, tx, myVisitor);
        }
        System.out.println("update " + myVisitor.num);
        Assert.assertEquals(resNum, myVisitor.num);
        // end update

    }


    private static final class MyVisitor implements BooleanGeometryDataNodeVisitor {
        int num;

        @Override
        public boolean visit(String nodeId, Geometry geometry) {
            num++;
            return false;
        }
    }

    private static final class DataNodeCell {
        private final String dataNodeId;
        private final Geometry geometry;

        public DataNodeCell(String dataNodeId, Geometry geometry) {
            this.dataNodeId = dataNodeId;
            this.geometry = geometry;
        }
    }
}
