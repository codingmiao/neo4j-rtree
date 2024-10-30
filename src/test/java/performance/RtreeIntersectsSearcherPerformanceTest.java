package performance;

import org.neo4j.graphdb.Transaction;
import org.wowtools.neo4j.rtree.Neo4jDbManager;
import org.wowtools.neo4j.rtree.RtreeEditor;
import org.wowtools.neo4j.rtree.RtreeIntersectsSearcher;
import org.wowtools.neo4j.rtree.pojo.PointNd;
import org.wowtools.neo4j.rtree.pojo.RectNd;
import org.wowtools.neo4j.rtree.util.BooleanDataNodeVisitor;

import java.util.Random;

/**
 * @author liuyu
 * @date 2022/1/4
 */
public class RtreeIntersectsSearcherPerformanceTest {
    private static final String indexName = "test";
    private static final Neo4jDbManager neo4jDbManager = new Neo4jDbManager();


    public static void main(String[] args) {
        int num = 123456;//测试数据量 4
        int threadNum = 4;
        Random r = new Random(233);
        try (RtreeEditor rtreeEditor = RtreeEditor.create(neo4jDbManager.getGraphDb(), 2000, indexName, 2, 8)) {
            for (int i = 0; i < num; i++) {
                double xmin = r.nextDouble();
                double xmax = xmin + r.nextDouble() * 0.1;
                double ymin = r.nextDouble();
                double ymax = ymin + r.nextDouble() * 0.1;

                RectNd rect2d = new RectNd(new PointNd(new double[]{xmin, ymin}), new PointNd(new double[]{xmax, ymax}));
                rect2d.setDataNodeId(String.valueOf(i));
                rtreeEditor.add(rect2d);
            }
        }
        System.out.println("add end");

        for (int i = 0; i < threadNum; i++) {
            startQueryThread();
        }

    }

    private static void startQueryThread() {
        Random r = new Random();
        final RtreeIntersectsSearcher searcher;
        try (Transaction tx = neo4jDbManager.getGraphDb().beginTx()) {
            searcher = RtreeIntersectsSearcher.get(tx, indexName);
        }

        new Thread(() -> {
            while (true) {
                long t = System.currentTimeMillis();
                MyVisitor myVisitor = new MyVisitor();
                try (Transaction tx = neo4jDbManager.getGraphDb().beginTx()) {
                    double xmin = r.nextDouble();
                    double xmax = xmin + r.nextDouble() * 0.3;
                    double ymin = r.nextDouble();
                    double ymax = ymin + r.nextDouble() * 0.3;
                    RectNd rect2d = new RectNd(new PointNd(new double[]{xmin, ymin}), new PointNd(new double[]{xmax, ymax}));
                    searcher.intersects(rect2d, tx, myVisitor);
                }
                long t1 = System.currentTimeMillis() - t;
                System.out.println("cost " + t1 + "\t,num " + myVisitor.n);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();


    }

    private static final class MyVisitor implements BooleanDataNodeVisitor {
        int n = 0;

        @Override
        public boolean visit(String nodeId) {
            n++;
            return false;
        }
    }
}
