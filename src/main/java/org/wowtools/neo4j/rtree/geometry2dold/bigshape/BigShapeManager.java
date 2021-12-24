package org.wowtools.neo4j.rtree.geometry2dold.bigshape;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.wowtools.neo4j.rtree.geometry2dold.Constant;
import org.wowtools.neo4j.rtree.geometry2dold.bigshape.pojo.BigShape;
import org.wowtools.neo4j.rtree.geometry2dold.bigshape.pojo.Grid;
import org.wowtools.neo4j.rtree.geometry2dold.bigshape.util.GridCuter;
import org.wowtools.neo4j.rtree.geometry2dold.spatial.EnvelopeDecoder;
import org.wowtools.neo4j.rtree.geometry2dold.spatial.RTreeIndex;
import org.wowtools.neo4j.rtree.geometry2dold.spatial.Envelope;
import org.wowtools.neo4j.rtree.geometry2dold.util.GeometryBbox;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * BigShape管理类
 *
 * @author liuyu
 * @date 2020/11/24
 */
public class BigShapeManager {
    /**
     * grid关键属性存储的字段名，关键属性:当格子被完全覆盖时存储bbox，否则存储相交geometry
     */
    public static final String keyFieldName = "g";
    /**
     * rtree索引名字 前缀
     */
    private static final String indexNamePrefix = "BigShapeSidx_";
    /**
     * 建索引的锁
     */
    private static final Object createRTreeIndexLock = new Object();

    /**
     * 获取一个BigShape
     *
     * @param database 图库
     * @param id       BigShape id
     * @return
     */
    public static BigShape get(GraphDatabaseService database, String id) {
        String indexName = indexNamePrefix + id;
        int maxNodeReferences;
        int nodeValueCacheSize = 256;//TODO 改为动态获取或传入
        try (Transaction tx = database.beginTx()) {
            Node rootNode = tx.findNode(Constant.RtreeLabel.ReferenceNode, Constant.RtreeProperty.indexName, indexName);
            if (null == rootNode) {
                throw new RuntimeException("index (" + indexName + ") is nonexistent");
            }
            maxNodeReferences = (int) rootNode.getProperty(Constant.RtreeProperty.maxNodeReferences);
        }
        RTreeIndex rTreeIndex = new RTreeIndex(indexName, keyFieldName, database, new BigShapeReadEnvelopeDecoder(), maxNodeReferences, 0, false);
        BigShape bigShape = new BigShape(rTreeIndex, nodeValueCacheSize);
        return bigShape;
    }

    /**
     * 构造BigShape并存入图库
     *
     * @param database
     * @param id       BigShape id，请确保其唯一性
     * @param geometry geometry
     * @param row      切成几行
     * @param column   切成几列
     */
    public static void build(GraphDatabaseService database, String id, Geometry geometry, int row, int column) {
        /** 0、构造一个rtree索引 **/
        String indexName = indexNamePrefix + id;
        Map<Long, Envelope> envelopeMap = new HashMap<>();//缓存一个bbox，防止多次读取
        RTreeIndex rTreeIndex;
        synchronized (createRTreeIndexLock) {
            try (Transaction tx = database.beginTx()) {
                tx.findNodes(Constant.RtreeLabel.ReferenceNode, Constant.RtreeProperty.indexName, indexName).forEachRemaining((node) -> {
                    throw new RuntimeException("index name(" + indexName + ") has been taken");
                });
            }
            rTreeIndex = new RTreeIndex(indexName, keyFieldName, database,
                    new BigShapeWriteEnvelopeDecoder(envelopeMap), 2, 0, true);
        }
        /** 1、切割geometry **/
        List<Grid> grids = GridCuter.cut(geometry, row, column);
        /** 2、 将切割结果写入索引 **/
        WKBWriter wkbWriter = new WKBWriter();
        try (Transaction tx = database.beginTx()) {
            List<Node> geomNodes = new LinkedList<>();
            for (Grid grid : grids) {
                Object value;//存到图库中的值
                GeometryBbox.Bbox bbox = GeometryBbox.getBbox(grid.getGeometry());
                if (grid.isCoverCompletely()) {//若完全覆盖网格，则存储bbox
                    value = bbox.toDoubleArray();
                } else {//若未完全覆盖网格，则存储相交部分的geometry wkb
                    value = wkbWriter.write(grid.getGeometry());
                }
                Node gridNode = tx.createNode();
                gridNode.setProperty(keyFieldName, value);
                envelopeMap.put(gridNode.getId(), bbox.toEnvelope());
                geomNodes.add(gridNode);
            }
            rTreeIndex.add(geomNodes, tx);
            tx.commit();
        }

    }

    /**
     * 写索引时的bbox获取器，直接从缓存map中获取
     */
    private static final class BigShapeWriteEnvelopeDecoder implements EnvelopeDecoder {
        private final Map<Long, Envelope> envelopeMap;

        public BigShapeWriteEnvelopeDecoder(Map<Long, Envelope> envelopeMap) {
            this.envelopeMap = envelopeMap;
        }

        @Override
        public Envelope decodeEnvelope(Node node) {
            return envelopeMap.get(node.getId());
        }
    }

    /**
     * 写索引时的bbox获取器，从关键字段中获取
     */
    private static final class BigShapeReadEnvelopeDecoder implements EnvelopeDecoder {
        private final WKBReader wkbReader = new WKBReader();

        @Override
        public Envelope decodeEnvelope(Node node) {
            Object value = node.getProperty(keyFieldName);
            if (value instanceof double[]) {
                double[] bbox = (double[]) value;
                return new Envelope(bbox[0], bbox[2], bbox[1], bbox[3]);
            } else {
                byte[] wkb = (byte[]) value;
                Geometry geometry;
                try {
                    geometry = wkbReader.read(wkb);
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
                GeometryBbox.Bbox bbox = GeometryBbox.getBbox(geometry);
                return bbox.toEnvelope();
            }
        }
    }
}
