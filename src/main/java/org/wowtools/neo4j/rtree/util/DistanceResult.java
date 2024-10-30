package org.wowtools.neo4j.rtree.util;


/**
 * 距离查询结果
 *
 * @author liuyu
 * @date 2020/6/12
 */
public class DistanceResult {
    protected final double dist;
    protected final String dataNodeId;

    /**
     * 构造方法需要传入距离和数据节点id
     *
     * @param dist       距离
     * @param dataNodeId 数据节点id
     */
    public DistanceResult(double dist, String dataNodeId) {
        this.dist = dist;
        this.dataNodeId = dataNodeId;
    }


    public double getDist() {
        return dist;
    }

    public String getDataNodeId() {
        return dataNodeId;
    }
}
