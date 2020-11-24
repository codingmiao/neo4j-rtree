package org.wowtools.neo4j.rtree.bigshape;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBWriter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.wowtools.neo4j.rtree.Constant;
import org.wowtools.neo4j.rtree.bigshape.pojo.Grid;
import org.wowtools.neo4j.rtree.bigshape.util.GridCuter;
import org.wowtools.neo4j.rtree.spatial.Envelope;
import org.wowtools.neo4j.rtree.spatial.EnvelopeDecoder;
import org.wowtools.neo4j.rtree.spatial.RTreeIndex;
import org.wowtools.neo4j.rtree.util.GeometryBbox;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author liuyu
 * @date 2020/11/24
 */
public class BigShapeManager {

    private static final String geometryFieldName = "a";
    private static final String isCoverCompletelyFieldName = "b";
    private static final String indexNamePrefix = "BigShapeSidx_";

    private static final Object createRTreeIndexLock = new Object();

    public static void build(GraphDatabaseService database, String id, Geometry geometry, int row, int column) {
        String indexName = indexNamePrefix + id;
        Map<Long, Geometry> geometryMap = new HashMap<>();
        RTreeIndex rTreeIndex;
        synchronized (createRTreeIndexLock) {
            try (Transaction tx = database.beginTx()) {
                tx.findNodes(Constant.RtreeLabel.ReferenceNode, Constant.RtreeProperty.indexName, indexName).forEachRemaining((node) -> {
                    throw new RuntimeException("index name(" + indexName + ") has been taken");
                });
            }
            rTreeIndex = new RTreeIndex(indexName, geometryFieldName, database,
                    new BigShapeWriteEnvelopeDecoder(geometryMap), 64, 0, true);
        }

        List<Grid> grids = GridCuter.cut(geometry, row, column);

        WKBWriter wkbWriter = new WKBWriter();
        try (Transaction tx = database.beginTx()) {
            List<Node> geomNodes = new LinkedList<>();
            for (Grid grid : grids) {
                byte[] wkb = wkbWriter.write(grid.getGeometry());
                Node gridNode = tx.createNode();
                gridNode.setProperty(geometryFieldName, wkb);
                gridNode.setProperty(isCoverCompletelyFieldName, grid.isCoverCompletely());
                geometryMap.put(gridNode.getId(), grid.getGeometry());
                geomNodes.add(gridNode);
            }
            rTreeIndex.add(geomNodes, tx);
            tx.commit();
        }
    }

    private static final class BigShapeWriteEnvelopeDecoder implements EnvelopeDecoder {
        private final Map<Long, Geometry> geometryMap;

        public BigShapeWriteEnvelopeDecoder(Map<Long, Geometry> geometryMap) {
            this.geometryMap = geometryMap;
        }

        @Override
        public Envelope decodeEnvelope(Node node) {
            Geometry geometry = geometryMap.get(node.getId());
            GeometryBbox.Bbox bbox = GeometryBbox.getBbox(geometry);

            Envelope envelope = new Envelope(bbox.xmin, bbox.xmax, bbox.ymin, bbox.ymax);
            return envelope;
        }
    }
}
