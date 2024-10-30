package org.wowtools.neo4j.rtree.geometry2d;


import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBWriter;
import org.neo4j.driver.*;
import org.neo4j.graphdb.Node;
import org.wowtools.giscat.vector.pojo.Feature;
import org.wowtools.giscat.vector.pojo.FeatureCollection;
import org.wowtools.giscat.vector.pojo.converter.ProtoFeatureConverter;
import org.wowtools.neo4j.rtree.Neo4jDbManager;
import org.wowtools.neo4j.rtree.internal.edit.TxCell;
import org.wowtools.neo4j.rtree.util.CustomProceduresRegister;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class Geometry2dQueryFunctionTest {

    public static void main(String[] args) {
        //1、启动neo4j
        Neo4jDbManager neo4jDbManager = new Neo4jDbManager();
        //2、注册neo4j-rtree的函数到neo4j中
        CustomProceduresRegister.registerProcedures(neo4jDbManager.getGraphDb(), Geometry2dQueryFunction.class);

        //3、测试数据
        int num = 1234;//测试数据量 4
        String indexName = "testIndex";
        String geometryName = "geo";
        Random r = new Random(233);
        GeometryFactory geometryFactory = new GeometryFactory();
        WKBWriter wkbWriter = new WKBWriter();

        // add
        try (Geometry2dRtreeEditor rtreeEditor = Geometry2dRtreeEditor.create(neo4jDbManager.getGraphDb(), 2000, indexName, 2, 8, geometryName)) {
            TxCell txCell = rtreeEditor.getTxCell();
            for (int i = 0; i < num; i++) {
                double x = 100 + r.nextDouble() * 20;
                double y = 20 + r.nextDouble() * 10;
                Geometry geometry = geometryFactory.createPoint(new Coordinate(x, y));
                geometry = geometry.buffer(r.nextDouble() * 0.1);
                byte[] wkb = wkbWriter.write(geometry);
                Node dataNode = txCell.getTx().createNode();
                dataNode.setProperty("name", "node" + i);
                dataNode.setProperty(geometryName, wkb);
                rtreeEditor.add(dataNode.getElementId(), geometry);
            }
        }

        System.out.println("add end-----------------------------------------");
        //4、查询函数调用

        //实际开发时请注意关闭session、tx
        Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("1", "1"));
        Session session = driver.session();
        try (Transaction tx = session.beginTransaction()) {
            Result result = tx.run("return nr.g2d.bboxIntersects($indexName,$xmin,$ymin,$xmax,$ymax,$propertyNames)", Map.of(
                    "indexName", indexName,
                    "xmin", 100,
                    "ymin", 20,
                    "xmax", 110,
                    "ymax", 25,
                    "propertyNames", List.of("name")
            ));
            byte[] bytes = result.single().get(0).asByteArray();
            FeatureCollection featureCollection = ProtoFeatureConverter.proto2featureCollection(bytes, Constant.geometryFactory);
            for (Feature feature : featureCollection.getFeatures()) {
                System.out.println(feature.getProperties().get("name") + "\t" + feature.getGeometry());
            }
        }

        System.out.println("bboxIntersects end----------------------------------------");
        try (Transaction tx = session.beginTransaction()) {
            Result result = tx.run("return nr.g2d.geoIntersects($indexName,$wkt,$propertyNames)", Map.of(
                    "indexName", indexName,
                    "wkt", "POLYGON ((100 20, 110 20, 110 25, 100 25, 100 20))",
                    "propertyNames", List.of("name")
            ));
            byte[] bytes = result.single().get(0).asByteArray();
            FeatureCollection featureCollection = ProtoFeatureConverter.proto2featureCollection(bytes, Constant.geometryFactory);
            for (Feature feature : featureCollection.getFeatures()) {
                System.out.println(feature.getProperties().get("name") + "\t" + feature.getGeometry());
            }
        }
        System.out.println("geoIntersects end----------------------------------------");

        try (Transaction tx = session.beginTransaction()) {
            Result result = tx.run("return nr.g2d.nearest($indexName,$x,$y,$n,$propertyNames)", Map.of(
                    "indexName", indexName,
                    "x", 103,
                    "y", 24,
                    "n", 5,
                    "propertyNames", List.of("name")
            ));
            byte[] bytes = result.single().get(0).asByteArray();
            FeatureCollection featureCollection = ProtoFeatureConverter.proto2featureCollection(bytes, Constant.geometryFactory);
            for (Feature feature : featureCollection.getFeatures()) {
                System.out.println(feature.getProperties().get("name") + "\t" + feature.getGeometry());
            }
        }
        System.out.println("nearest end----------------------------------------");

//        System.exit(0);
    }

}
