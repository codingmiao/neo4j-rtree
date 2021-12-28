package org.wowtools.neo4j.rtree.internal.nearest;


/**
 * 距离查询结果
 *
 * @author liuyu
 * @date 2020/6/12
 */
public class DistanceResult {
    protected final double dist;
    protected final long dataNodeId;

    public DistanceResult(double dist, long dataNodeId) {
        this.dist = dist;
        this.dataNodeId = dataNodeId;
    }


    public double getDist() {
        return dist;
    }

    public long getDataNodeId() {
        return dataNodeId;
    }
}
