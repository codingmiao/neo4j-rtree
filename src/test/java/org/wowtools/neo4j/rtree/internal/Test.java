package org.wowtools.neo4j.rtree.internal;

import org.junit.After;
import org.junit.Before;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.wowtools.neo4j.rtree.Neo4jDbManager;
import org.wowtools.neo4j.rtree.internal.edit.RTree;
import org.wowtools.neo4j.rtree.internal.edit.SpatialSearches;
import org.wowtools.neo4j.rtree.pojo.PointNd;
import org.wowtools.neo4j.rtree.pojo.RectNd;
import org.wowtools.neo4j.rtree.internal.edit.TxCell;

import java.util.ArrayList;
import java.util.Random;
import java.util.function.Consumer;

/**
 * https://github.com/conversant/rtree/wiki
 *
 * @author liuyu
 * @date 2021/12/17
 */
public class Test {

    private Neo4jDbManager neo4jDbManager;

    @Before
    public void before() {
        neo4jDbManager = new Neo4jDbManager();
    }

    @After
    public void after() {
        neo4jDbManager.afterTest();
    }

    @org.junit.Test
    public void test() {
        System.out.println("init");
        GeometryFactory gf = new GeometryFactory();
        RectNd.Builder b = new RectNd.Builder();
        int mMin = 2;
        int mMax = 16;
        TxCell txCell = new TxCell(200, mMin, mMax, neo4jDbManager.getGraphDb());
        //构造测试数据
        double x0 = 0, x1 = 0.5, y0 = 0, y1 = 0.5;//查询范围
        int num = 32;//测试数据量 4
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
            rect2d.setDataNodeId(i);
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

        System.out.println("start");
        long t = System.currentTimeMillis();
        RTree tree = (RTree) SpatialSearches.rTree(b, "test1", mMin, mMax, txCell);

        // add
        for (int i = 0; i < num; i++) {
            tree.add(rectNds[i]);
            txCell.addChange(1);
            txCell.limitCommit();
        }
        txCell.commit();
        // end add

        // remove
        int removeNum = (int) (resNum * 0.4);//移除40%的数据
        resNum -= removeNum;
        txCell.newTx();
        for (int i = 0; i < removeNum; i++) {
            RectNd rn = intersectRectNds.get(i);
            tree.remove(rn);
            txCell.addChange(1);
            txCell.limitCommit();
        }
        txCell.commit();
        // end remove

        // update
        txCell.newTx();
        int updateStart = removeNum + 1;
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

            tree.update(told, rect2d);

            txCell.addChange(1);
            txCell.limitCommit();
        }
        txCell.commit();
        // end update

        long t1 = System.currentTimeMillis();
        System.out.println("build success cost " + (t1 - t));
        txCell.newTx();

        MyConsumer myConsumer = new MyConsumer();
        PointNd p0 = new PointNd(new double[]{x0, y0});
        PointNd p1 = new PointNd(new double[]{x1, y1});
        tree.intersects(new RectNd(p0, p1), myConsumer);
        System.out.println(myConsumer.num);
        System.out.println(resNum);

        System.out.println("end cost " + (System.currentTimeMillis() - t1));
    }

    static class MyConsumer implements Consumer<RectNd> {
        int num;

        @Override
        public void accept(RectNd rect2d) {
            num++;
        }
    }
}
