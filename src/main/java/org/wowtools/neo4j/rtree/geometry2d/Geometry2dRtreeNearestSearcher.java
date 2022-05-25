package org.wowtools.neo4j.rtree.geometry2d;

import org.locationtech.jts.io.WKBReader;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.wowtools.neo4j.rtree.RtreeNearestSearcher;
import org.wowtools.neo4j.rtree.internal.define.Labels;
import org.wowtools.neo4j.rtree.pojo.PointNd;
import org.wowtools.neo4j.rtree.util.DistanceResultNodeFilter;
import org.wowtools.neo4j.rtree.util.NearestNeighbour;

import java.util.List;

/**
 * 二维geometry 最邻近搜索器
 *
 * @author liuyu
 * @date 2021/12/31
 */
public class Geometry2dRtreeNearestSearcher {

    private final RtreeNearestSearcher rtreeNearestSearcher;
    private final String geometryName;

    private Geometry2dRtreeNearestSearcher(RtreeNearestSearcher rtreeNearestSearcher, String geometryName) {
        this.rtreeNearestSearcher = rtreeNearestSearcher;
        this.geometryName = geometryName;
    }

    /**
     * 获取查询器
     *
     * @param tx   事务 此事务需要在外部手动关闭
     * @param name 索引名
     * @return Geometry2dRtreeNearestSearcher
     */
    public static Geometry2dRtreeNearestSearcher get(Transaction tx, String name) {
        Node metadataNode = tx.findNode(Labels.METADATA, "name", name);
        if (null == metadataNode) {
            throw new RuntimeException("索引不存在 " + name);
        }
        String geometryName = (String) metadataNode.getProperty(Constant.geometryNameKey);
        RtreeNearestSearcher rtreeNearestSearcher = RtreeNearestSearcher.get(tx, name);
        return new Geometry2dRtreeNearestSearcher(rtreeNearestSearcher, geometryName);
    }


    /**
     * 最邻近查询。查询距离输入坐标 x y 的距离最近的点
     *
     * @param filter  过滤器，不满足条件的dataNode会被丢弃 ,为空则不进行过滤
     * @param maxHits 最大返回条数
     * @param x       x
     * @param y       y
     * @param tx      事务 此事务需要在外部手动关闭
     * @return GeometryDistanceResult list，包含了geometry、与输入点的距离、nodeId
     */
    public List<GeometryDistanceResult> nearest(DistanceResultNodeFilter filter, int maxHits, double x, double y, Transaction tx) {
        PointNd pointNd = new PointNd(new double[]{x, y});
        WKBReader wkbReader = new WKBReader();
        if (null == filter) {
            filter = NearestNeighbour.alwaysTrue;
        }
        NearestNeighbour<GeometryDistanceResult> nearestNeighbour = new NearestNeighbour<>(filter, maxHits, pointNd) {
            @Override
            public GeometryDistanceResult createDistanceResult(PointNd pointNd, long dataNodeId) {
                return GeometryDistanceResult.newInstance(tx, geometryName, wkbReader, pointNd, dataNodeId);
            }
        };
        return rtreeNearestSearcher.nearest(nearestNeighbour, tx);
    }
}
