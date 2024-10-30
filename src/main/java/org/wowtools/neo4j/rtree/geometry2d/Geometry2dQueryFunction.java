package org.wowtools.neo4j.rtree.geometry2d;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import org.wowtools.giscat.vector.pojo.Feature;
import org.wowtools.giscat.vector.pojo.FeatureCollection;
import org.wowtools.giscat.vector.pojo.converter.ProtoFeatureConverter;
import org.wowtools.neo4j.rtree.pojo.RectNd;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author liuyu
 * @date 2022/5/25
 */
public class Geometry2dQueryFunction {
    @Context
    public GraphDatabaseService graphDb;

    /**
     * 读Intersects查询结果并转为feature对象的遍历器
     */
    private static final class FeatureVisitor implements BooleanGeometryDataNodeVisitor {
        private final Transaction tx;
        private final LinkedList<Feature> features = new LinkedList<>();
        private final String[] propertyKeys;

        public FeatureVisitor(Transaction tx, String[] propertyKeys) {
            this.tx = tx;
            this.propertyKeys = propertyKeys;
        }

        @Override
        public boolean visit(String nodeId, Geometry geometry) {
            Feature feature = node2Feature(tx, nodeId, geometry, propertyKeys);
            features.add(feature);
            return false;
        }
    }

    private static Feature node2Feature(Transaction tx, String nodeId, Geometry geometry, String[] propertyKeys) {
        Node node = tx.getNodeByElementId(nodeId);
        Feature feature = new Feature();
        feature.setGeometry(geometry);
        if (null != propertyKeys && propertyKeys.length > 0) {
            Map<String, Object> properties = node.getProperties(propertyKeys);
            feature.setProperties(properties);
        }
        return feature;
    }


    @UserFunction("nr.g2d.bboxIntersects")
    @Description("传入索引名(indexName)、bbox范围(xmin、ymin、xmax、ymax)、需要返回的属性(propertyNames)，查询与bbox相交的节点，并转为ProtoFeature bytes范围")
    public byte[] bboxIntersects(@Name("indexName") String indexName,
                                 @Name("xmin") double xmin,
                                 @Name("ymin") double ymin,
                                 @Name("xmax") double xmax,
                                 @Name("ymax") double ymax,
                                 @Name("propertyNames") List<String> propertyNames) {
        String[] propertyKeys = new String[propertyNames.size()];
        propertyNames.toArray(propertyKeys);
        RectNd bbox = new RectNd(new double[]{xmin, ymin}, new double[]{xmax, ymax});
        List<Feature> features;
        try (Transaction tx = graphDb.beginTx()) {
            FeatureVisitor visitor = new FeatureVisitor(tx, propertyKeys);
            Geometry2dRtreeIntersectsSearcher searcher = Geometry2dRtreeIntersectsSearcher.get(tx, indexName);
            searcher.intersects(bbox, tx, visitor);
            features = visitor.features;
        }
        FeatureCollection featureCollection = new FeatureCollection();
        featureCollection.setFeatures(features);
        return ProtoFeatureConverter.featureCollection2Proto(featureCollection);
    }

    @UserFunction("nr.g2d.geoIntersects")
    @Description("传入索引名(indexName)、一个wkt字符串描述的geometry(wkt)、需要返回的属性(propertyNames)，查询与geometry相交的节点，并转为ProtoFeature bytes范围")
    public byte[] geoIntersects(@Name("indexName") String indexName,
                                @Name("wkt") String wkt,
                                @Name("propertyNames") List<String> propertyNames) {
        Geometry inputGeometry;
        try {
            inputGeometry = new WKTReader().read(wkt);
        } catch (Exception e) {
            throw new RuntimeException("解析wkt失败", e);
        }
        String[] propertyKeys = new String[propertyNames.size()];
        propertyNames.toArray(propertyKeys);
        List<Feature> features;
        try (Transaction tx = graphDb.beginTx()) {
            FeatureVisitor visitor = new FeatureVisitor(tx, propertyKeys);
            Geometry2dRtreeIntersectsSearcher searcher = Geometry2dRtreeIntersectsSearcher.get(tx, indexName);
            searcher.intersects(inputGeometry, tx, visitor);
            features = visitor.features;
        }
        FeatureCollection featureCollection = new FeatureCollection();
        featureCollection.setFeatures(features);
        return ProtoFeatureConverter.featureCollection2Proto(featureCollection);
    }

    @UserFunction("nr.g2d.nearest")
    @Description("传入索引名(indexName)、一个点(x、y)、最大返回条数(n)、需要返回的属性(propertyNames)，查询距离点最近的n条数据，若数据量不足，返回数量可能少于n")
    public byte[] nearest(@Name("indexName") String indexName,
                          @Name("x") double x,
                          @Name("y") double y,
                          @Name("n") long n,
                          @Name("propertyNames") List<String> propertyNames) {
        String[] propertyKeys = new String[propertyNames.size()];
        propertyNames.toArray(propertyKeys);
        List<Feature> features;
        try (Transaction tx = graphDb.beginTx()) {
            Geometry2dRtreeNearestSearcher nearestSearcher = Geometry2dRtreeNearestSearcher.get(tx, indexName);
            List<GeometryDistanceResult> rs = nearestSearcher.nearest(null, (int) n, x, y, tx);
            features = new ArrayList<>(rs.size());
            for (GeometryDistanceResult result : rs) {
                Feature feature = node2Feature(tx, result.getDataNodeId(), result.getGeometry(), propertyKeys);
                features.add(feature);
            }
        }
        FeatureCollection featureCollection = new FeatureCollection();
        featureCollection.setFeatures(features);
        return ProtoFeatureConverter.featureCollection2Proto(featureCollection);
    }


}
