package org.wowtools.neo4j.rtree.geometry2d;

import lombok.extern.slf4j.Slf4j;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.wowtools.neo4j.rtree.RtreeEditor;
import org.wowtools.neo4j.rtree.internal.define.Labels;
import org.wowtools.neo4j.rtree.internal.define.PropertyNames;
import org.wowtools.neo4j.rtree.internal.edit.TxCell;
import org.wowtools.neo4j.rtree.pojo.RectNd;
import org.wowtools.neo4j.rtree.util.VoidDataNodeVisitor;

/**
 * 二维geometry rtree编辑器，此对象实例化时，会启动一个事务，并在索引上加写锁，所以务必在结束时调用close方法
 *
 * @author liuyu
 * @date 2021/12/28
 */
@Slf4j
public class Geometry2dRtreeEditor implements AutoCloseable {

    private final RtreeEditor rtreeEditor;
    private final String geometryName;
    private final WKBReader wkbReader = new WKBReader();

    private Geometry2dRtreeEditor(RtreeEditor rtreeEditor, String geometryName) {
        this.rtreeEditor = rtreeEditor;
        this.geometryName = geometryName;
    }

    /**
     * 获取索引
     *
     * @param graphdb     neo4j db
     * @param commitLimit 操作达到多少个顶点时执行提交操作
     * @param name        索引名
     * @return Geometry2dRtreeEditor
     */
    public static Geometry2dRtreeEditor get(GraphDatabaseService graphdb, int commitLimit, String name) {
        RtreeEditor rtreeEditor = RtreeEditor.get(graphdb, commitLimit, name);
        try {
            String metadataNodeId = rtreeEditor.getrTree().getMetadataNodeId();
            Node metadataNode = rtreeEditor.getTxCell().getTx().getNodeByElementId(metadataNodeId);
            String geometryName = (String) metadataNode.getProperty(Constant.geometryNameKey);
            return new Geometry2dRtreeEditor(rtreeEditor, geometryName);
        } catch (Exception e) {
            throw new RuntimeException(e);

        }
    }


    /**
     * 新建索引
     *
     * @param graphdb      neo4j db
     * @param commitLimit  操作达到多少个顶点时执行提交操作
     * @param name         索引名
     * @param mMin         索引中每个节点最小子节点数
     * @param mMax         索引中每个节点最大子节点数
     * @param geometryName dataNode中geometry字段名
     * @return Geometry2dRtreeEditor
     */
    public static Geometry2dRtreeEditor create(GraphDatabaseService graphdb, int commitLimit, String name, int mMin, int mMax,
                                               String geometryName) {
        RtreeEditor rtreeEditor = RtreeEditor.create(graphdb, commitLimit, name, mMin, mMax);
        try {
            String metadataNodeId = rtreeEditor.getrTree().getMetadataNodeId();
            Node metadataNode = rtreeEditor.getTxCell().getTx().getNodeByElementId(metadataNodeId);
            metadataNode.setProperty(Constant.geometryNameKey, geometryName);
            Geometry2dRtreeEditor geometry2dRtreeEditor = new Geometry2dRtreeEditor(rtreeEditor, geometryName);
            return geometry2dRtreeEditor;
        } catch (Exception e) {
            throw new RuntimeException(e);

        }
    }


    /**
     * 若指定名称的索引存在，获取索引，若不存在，则新建一个
     *
     * @param graphdb      neo4j db
     * @param commitLimit  操作达到多少个顶点时执行提交操作
     * @param name         索引名
     * @param mMin         索引中每个节点最小子节点数，如索引已存在则使用现有值，此输入值失效
     * @param mMax         索引中每个节点最大子节点数，如索引已存在则使用现有值，此输入值失效
     * @param geometryName dataNode中geometry字段名，如索引已存在则使用现有值，此输入值失效
     * @return RtreeEditor
     */
    public static Geometry2dRtreeEditor getOrCreate(GraphDatabaseService graphdb, int commitLimit, String name, int mMin, int mMax,
                                                    String geometryName) {
        RtreeEditor rtreeEditor = RtreeEditor.getOrCreate(graphdb, commitLimit, name, mMin, mMax);
        try {
            String metadataNodeId = rtreeEditor.getrTree().getMetadataNodeId();
            Node metadataNode = rtreeEditor.getTxCell().getTx().getNodeByElementId(metadataNodeId);
            if (!metadataNode.hasProperty(Constant.geometryNameKey)) {
                metadataNode.setProperty(Constant.geometryNameKey, geometryName);
            }

            Geometry2dRtreeEditor geometry2dRtreeEditor = new Geometry2dRtreeEditor(rtreeEditor, geometryName);
            return geometry2dRtreeEditor;
        } catch (Exception e) {
            rtreeEditor.close();
            throw new RuntimeException(e);
        }
    }

    /**
     * 删除索引
     *
     * @param graphdb         neo4j db
     * @param name            索引名
     * @param dataNodeVisitor 数据节点访问器，具体实现遇到数据节点该如何处置（例如将数据节点删除）
     */
    public static void drop(GraphDatabaseService graphdb, String name, VoidDataNodeVisitor dataNodeVisitor) {
        RtreeEditor.drop(graphdb, name, dataNodeVisitor);
    }


    private RectNd getNodeRectNdFromDataNode(String dataNodeId) {
        Node node;
        try {
            node = rtreeEditor.getTxCell().getTx().getNodeByElementId(dataNodeId);
        } catch (NotFoundException e) {
            log.info("node不存在 {}", dataNodeId);
            return null;
        }
        byte[] wkb = (byte[]) node.getProperty(geometryName, null);
        if (null == wkb) {
            log.info("node没有geometry {} 字段 {}", geometryName, dataNodeId);
            return null;
        }
        Geometry geometry;
        try {
            geometry = wkbReader.read(wkb);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return GeometryBbox.getBbox(geometry).toRect2d();
    }

    private RectNd getNodeRectNdFromEntityNode(String dataNodeId) {
        Node node = rtreeEditor.getTxCell().getTx().findNode(Labels.RTREE_ENTITY, PropertyNames.entryDataId, dataNodeId);
        RectNd rectNd = new RectNd((double[])node.getProperty(PropertyNames.entryMin),(double[]) node.getProperty(PropertyNames.entryMax));
        return rectNd;
    }

    /**
     * 向索引中添加数据
     *
     * @param dataNodeId 数据节点neo4j id
     */
    public void add(String dataNodeId) {
        RectNd rectNd = getNodeRectNdFromDataNode(dataNodeId);
        if (null == rectNd) {
            return;
        }
        rectNd.setDataNodeId(dataNodeId);
        rtreeEditor.add(rectNd);
    }

    /**
     * 从索引中移除数据，注意不会删除数据节点，如需删除或其它操作应在自身业务代码中实现
     *
     * @param dataNodeId 被移除的数据节点neo4j id
     */
    public void remove(String dataNodeId) {
        RectNd rectNd = getNodeRectNdFromDataNode(dataNodeId);
        rectNd.setDataNodeId(dataNodeId);
        rtreeEditor.remove(rectNd);
    }

    /**
     * 修改现有数据
     *
     * @param dataNodeId  数据节点neo4j id
     */
    public void update(String dataNodeId) {
        RectNd oldRectNd = getNodeRectNdFromEntityNode(dataNodeId);
        oldRectNd.setDataNodeId(dataNodeId);
        RectNd newRectNd = getNodeRectNdFromDataNode(dataNodeId);
        newRectNd.setDataNodeId(dataNodeId);
        rtreeEditor.update(oldRectNd, newRectNd);
    }

    @Override
    public void close() {
        rtreeEditor.close();
    }

    public TxCell getTxCell() {
        return rtreeEditor.getTxCell();
    }
}
