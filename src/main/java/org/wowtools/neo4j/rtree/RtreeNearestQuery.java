package org.wowtools.neo4j.rtree;

import org.locationtech.jts.geom.Geometry;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.wowtools.neo4j.rtree.nearest.DistanceResult;
import org.wowtools.neo4j.rtree.nearest.NearestNeighbour;
import org.wowtools.neo4j.rtree.spatial.RTreeIndex;

import java.util.List;

/**
 * @author liuyu
 * @date 2020/6/10
 */
public class RtreeNearestQuery {

    /**
     * 节点过滤器
     */
    @FunctionalInterface
    public interface NodeFilter {
        /**
         * 过滤
         *
         * @param node 节点
         * @param geometry geometry
         * @return 返回false则忽略此节点
         */
        boolean accept(Node node, Geometry geometry);
    }

    /**
     * 最邻近搜索。查询与输入点最近的设备，最多返回n个。
     * 原理参见https://www.cnblogs.com/arxive/p/8327516.html
     *
     * @param tx         tx
     * @param rTreeIndex 索引
     * @param x          输入点x坐标
     * @param y          输入点y坐标
     * @param n          最大返回node数
     * @param nodeFilter 节点过滤器
     * @return 查询结果
     */
    public static List<DistanceResult> queryNearestN(Transaction tx, RTreeIndex rTreeIndex, double x, double y, int n, NodeFilter nodeFilter) {
        Node rtreeNode = rTreeIndex.getIndexRoot(tx);
        NearestNeighbour nn =
                new NearestNeighbour(nodeFilter, n, rtreeNode, x, y, rTreeIndex);
        return nn.find();
    }
}
