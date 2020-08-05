package org.wowtools.neo4j.rtree;

import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.wowtools.neo4j.rtree.spatial.RTreeIndex;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;


/**
 * 缓存性能测试
 *
 * @author liuyu
 * @date 2020/8/4
 */
public class CacheTest {
    final String geometryFileName = "geo";
    final String wktFileName = "wkt";
    final int geoNum = 10;
    final int pointNum = 1000;
    final Label testLabel = Label.label("TestNode");

    @Test
    public void test() {
        BuildCell cell1 = buildIndex(1);
        BuildCell cell2 = buildIndex(geoNum);
        statistics(cell2);
        statistics(cell1);

        System.exit(0);
    }

    private void statistics(BuildCell cell) {
        int n = 20;
        long count = 0;
        long max = -1;
        long min = Long.MAX_VALUE;
        for (int i = 0; i < n; i++) {
            long t = System.currentTimeMillis();
            query(cell);
            t = System.currentTimeMillis() - t;
            System.out.println(t);
            count += t;
            if (t > max) {
                max = t;
            }
            if (t < min) {
                min = t;
            }
        }
        System.out.println(cell.rTreeIndex.getIndexName() + "\tavg:" + ((double) count) / n + "\tmax:" + max + "\tmin:" + min);
    }

    class BuildCell {
        final GraphDatabaseService db;
        final RTreeIndex rTreeIndex;
        final Geometry inputGeo;

        public BuildCell(GraphDatabaseService db, RTreeIndex rTreeIndex, Geometry inputGeo) {
            this.db = db;
            this.rTreeIndex = rTreeIndex;
            this.inputGeo = inputGeo;
        }
    }

    private BuildCell buildIndex(int geoCacheSize) {
        GraphDatabaseService db = Neo4jDbManager.getGraphDb();
        RTreeIndex rTreeIndex = RTreeIndexManager.createIndex(db, "polygonIdx-" + geoCacheSize, geometryFileName, 64, geoCacheSize);
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
        GeometryFactory gf = new GeometryFactory();
        WKBWriter wkbWriter = new WKBWriter();
        Random random = new Random(233);
        try (Transaction tx = db.beginTx()) {
            List<Node> sidxList = new LinkedList<>();
            for (int i = 0; i < geoNum; i++) {
                Node node = tx.createNode(testLabel);//新建节点
                Coordinate[] coordinates = new Coordinate[pointNum];
                for (int i1 = 0; i1 < pointNum; i1++) {
                    coordinates[i1] = new Coordinate(random.nextDouble() * 100, random.nextDouble() * 100);
                }
                Geometry geo = gf.createLineString(coordinates);
//                geo = geo.buffer(1);
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
        return new BuildCell(db, rTreeIndex, inputGeo);
    }

    private long query(BuildCell buildCell) {
        long t = System.currentTimeMillis();
        try (Transaction tx = buildCell.db.beginTx()) {
            RtreeQuery.queryByGeometryIntersects(tx, buildCell.rTreeIndex, buildCell.inputGeo, (node, geometry) -> {
            });
        }
        return System.currentTimeMillis() - t;
    }

}
