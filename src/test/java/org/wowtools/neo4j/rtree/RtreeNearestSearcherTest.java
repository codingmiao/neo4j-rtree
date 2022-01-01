package org.wowtools.neo4j.rtree;


import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.wowtools.neo4j.rtree.util.DistanceResult;
import org.wowtools.neo4j.rtree.pojo.PointNd;
import org.wowtools.neo4j.rtree.pojo.RectNd;
import org.wowtools.neo4j.rtree.util.NearestNeighbour;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

public class RtreeNearestSearcherTest {

    private static final int num = 12345;//测试数据量 4
    private static final String indexName = "testIndex";
    private static final Random r = new Random(233);

    private static final double x = 0.5, y = 0.5;

    private static final int hitNum = 10;
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
    public void test() {
        System.out.println("init");
        RectNd[] rectNds = new RectNd[num];
        for (int i = 0; i < rectNds.length; i++) {
            double x1 = r.nextDouble();
            double y1 = r.nextDouble();
            RectNd rect2d = new RectNd(new PointNd(new double[]{x1, y1}), new PointNd(new double[]{x1, y1}));
            rect2d.setDataNodeId(i);
            rectNds[i] = rect2d;
        }
        try (RtreeEditor rtreeEditor = RtreeEditor.create(neo4jDbManager.getGraphDb(), 2000, indexName, 2, 8)) {
            for (int i = 0; i < num; i++) {
                rtreeEditor.add(rectNds[i]);
            }
        }
        RectNd[] distNds = rectNds.clone();
        Arrays.sort(distNds, Comparator.comparingDouble(RtreeNearestSearcherTest::dist));
        System.out.println("search");
        PointNd pt = new PointNd(new double[]{x, y});
        List<DistanceResult> nearests;
        try (Transaction tx = neo4jDbManager.getGraphDb().beginTx()) {
            RtreeNearestSearcher searcher = RtreeNearestSearcher.get(tx, indexName);
            NearestNeighbour nearestNeighbour = new NearestNeighbour(hitNum, pt) {
                @Override
                public DistanceResult createDistanceResult(PointNd pointNd, long dataNodeId) {
                    RectNd rectNd = rectNds[(int) dataNodeId];
                    double dist = dist(rectNd);
                    return new DistanceResult(dist, dataNodeId);
                }
            };
            nearests = searcher.nearest(nearestNeighbour, tx);
        }
        for (int i = 0; i < hitNum; i++) {
            Assert.assertEquals(distNds[i].getDataNodeId(), nearests.get(i).getDataNodeId());
        }
    }

    private static final double dist(RectNd rect2d) {
        double[] xy = rect2d.getMaxXs();
        double x1 = xy[0];
        double y1 = xy[1];
        return Math.sqrt(Math.pow(x1 - x, 2) + Math.pow(y1 - y, 2));
    }
}
