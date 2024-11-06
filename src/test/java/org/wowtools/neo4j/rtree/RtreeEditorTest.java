package org.wowtools.neo4j.rtree;


import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.neo4j.graphdb.Transaction;
import org.wowtools.neo4j.rtree.internal.define.Labels;
import org.wowtools.neo4j.rtree.pojo.PointNd;
import org.wowtools.neo4j.rtree.pojo.RectNd;
import org.wowtools.neo4j.rtree.util.BooleanDataNodeVisitor;

import java.util.ArrayList;
import java.util.Random;

public class RtreeEditorTest {
    private Neo4jDbManager neo4jDbManager;

    @Before
    public void before() {
        neo4jDbManager = new Neo4jDbManager();
    }

    @After
    public void after() {
        neo4jDbManager.afterTest();
    }

    @Test
    public void test() {
        //构造测试数据
        System.out.println("init");
        GeometryFactory gf = new GeometryFactory();
        double x0 = 0, x1 = 0.5, y0 = 0, y1 = 0.5;//查询范围
        int num = 12345;//测试数据量
        String indexName = "testIndex";
        int resNum = 0;//手算出来的相交数
        Random r = new Random(233);
        Polygon bbox = gf.createPolygon(new Coordinate[]{
                new Coordinate(x0, y0),
                new Coordinate(x1, y0),
                new Coordinate(x1, y1),
                new Coordinate(x0, y1),
                new Coordinate(x0, y0)
        });

        RectNd[] rectNds = new RectNd[num];
        ArrayList<RectNd> intersectRectNds = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            double xmin = r.nextDouble();
            double xmax = xmin + r.nextDouble() * 0.1;
            double ymin = r.nextDouble();
            double ymax = ymin + r.nextDouble() * 0.1;


            RectNd rect2d = new RectNd(new PointNd(new double[]{xmin, ymin}), new PointNd(new double[]{xmax, ymax}));
            rect2d.setDataNodeId(String.valueOf(i));
            rectNds[i] = rect2d;

            Polygon bbox1 = gf.createPolygon(new Coordinate[]{
                    new Coordinate(xmin, ymin),
                    new Coordinate(xmax, ymin),
                    new Coordinate(xmax, ymax),
                    new Coordinate(xmin, ymax),
                    new Coordinate(xmin, ymin),
            });
            if (bbox.intersects(bbox1)) {
                resNum++;
                intersectRectNds.add(rect2d);
            }

        }

        // add
        long t = System.currentTimeMillis();
        try (RtreeEditor rtreeEditor = RtreeEditor.create(neo4jDbManager.getGraphDb(), 2000, indexName, 2, 8)) {
            for (int i = 0; i < num; i++) {
                rtreeEditor.add(rectNds[i]);
            }
        }
        long t1 = System.currentTimeMillis() - t;
        MyVisitor myVisitor = new MyVisitor();
        try (Transaction tx = neo4jDbManager.getGraphDb().beginTx()) {
            RtreeIntersectsSearcher searcher = RtreeIntersectsSearcher.get(tx, indexName);
            PointNd p0 = new PointNd(new double[]{x0, y0});
            PointNd p1 = new PointNd(new double[]{x1, y1});
            searcher.intersects(new RectNd(p0, p1), tx, myVisitor);
        }
        System.out.println("add " + myVisitor.num + ",cost " + t1);
        Assert.assertEquals(resNum, myVisitor.num);
        // end add

        // remove
        int removeNum = (int) (resNum * 0.4);//移除40%的数据
        resNum -= removeNum;
        t = System.currentTimeMillis();
        try (RtreeEditor rtreeEditor = RtreeEditor.get(neo4jDbManager.getGraphDb(), 2000, indexName)) {
            for (int i = 0; i < removeNum; i++) {
                RectNd rn = intersectRectNds.get(i);
                rtreeEditor.remove(rn);
            }
        }
        t1 = System.currentTimeMillis() - t;
        myVisitor = new MyVisitor();
        try (Transaction tx = neo4jDbManager.getGraphDb().beginTx()) {
            RtreeIntersectsSearcher searcher = RtreeIntersectsSearcher.get(tx, indexName);
            PointNd p0 = new PointNd(new double[]{x0, y0});
            PointNd p1 = new PointNd(new double[]{x1, y1});
            searcher.intersects(new RectNd(p0, p1), tx, myVisitor);
        }
        System.out.println("remove " + myVisitor.num + ",cost " + t1);
        Assert.assertEquals(resNum, myVisitor.num);
        // end remove

        // update
        int updateStart = removeNum + 1;
        t = System.currentTimeMillis();
        try (RtreeEditor rtreeEditor = RtreeEditor.get(neo4jDbManager.getGraphDb(), 2000, indexName)) {
            for (int i = updateStart; i < intersectRectNds.size(); i++) {
                RectNd told = intersectRectNds.get(i);
                double xmin = r.nextDouble();
                double xmax = xmin + r.nextDouble() * 0.1;
                double ymin = r.nextDouble();
                double ymax = ymin + r.nextDouble() * 0.1;

                RectNd rect2d = new RectNd(new PointNd(new double[]{xmin, ymin}), new PointNd(new double[]{xmax, ymax}));
                rect2d.setDataNodeId(told.getDataNodeId());

                Polygon bbox1 = gf.createPolygon(new Coordinate[]{
                        new Coordinate(xmin, ymin),
                        new Coordinate(xmax, ymin),
                        new Coordinate(xmax, ymax),
                        new Coordinate(xmin, ymax),
                        new Coordinate(xmin, ymin),
                });
                if (!bbox.intersects(bbox1)) {
                    resNum--;
                }
                rtreeEditor.update(told, rect2d);
            }
        }
        t1 = System.currentTimeMillis() - t;
        myVisitor = new MyVisitor();
        try (Transaction tx = neo4jDbManager.getGraphDb().beginTx()) {
            RtreeIntersectsSearcher searcher = RtreeIntersectsSearcher.get(tx, indexName);
            PointNd p0 = new PointNd(new double[]{x0, y0});
            PointNd p1 = new PointNd(new double[]{x1, y1});
            searcher.intersects(new RectNd(p0, p1), tx, myVisitor);
        }
        System.out.println("update " + myVisitor.num + ",cost " + t1);
        Assert.assertEquals(resNum, myVisitor.num);
        // end update


//        drop
        try (Transaction tx = neo4jDbManager.getGraphDb().beginTx()) {
            RtreeEditor.drop(neo4jDbManager.getGraphDb(), indexName, (nodeId) -> {
//                tx.getNodeById(nodeId).delete();
            });
        }
        try (Transaction tx = neo4jDbManager.getGraphDb().beginTx()) {
            tx.findNodes(Labels.METADATA).forEachRemaining(node -> {
                throw new RuntimeException("drop未清理干净 METADATA");
            });
            tx.findNodes(Labels.RTREE_BRANCH).forEachRemaining(node -> {
                throw new RuntimeException("drop未清理干净 RTREE_BRANCH");
            });
            tx.findNodes(Labels.RTREE_LEAF).forEachRemaining(node -> {
                throw new RuntimeException("drop未清理干净 RTREE_LEAF");
            });
            tx.findNodes(Labels.RTREE_ENTITY).forEachRemaining(node -> {
                throw new RuntimeException("drop未清理干净 RTREE_ENTITY");
            });
        }
//        drop end

//        java.util.Scanner sin = new java.util.Scanner(System.in);
//        sin.next();
    }


    private static final class MyVisitor implements BooleanDataNodeVisitor {
        int num;

        @Override
        public boolean visit(String nodeId) {
            num++;
            return false;
        }
    }

}
