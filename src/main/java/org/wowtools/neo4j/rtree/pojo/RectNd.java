package org.wowtools.neo4j.rtree.pojo;

import org.wowtools.neo4j.rtree.internal.edit.HyperPoint;
import org.wowtools.neo4j.rtree.internal.edit.HyperRect;
import org.wowtools.neo4j.rtree.internal.edit.RectBuilder;

/**
 * n维矩形
 *
 * @author liuyu
 * @date 2021/12/17
 */
public class RectNd implements HyperRect {
    private final PointNd min, max;


    /**
     * 连接到具体数据节点的id
     */
    private long dataNodeId = -1;

    public RectNd(PointNd min, PointNd max) {
        this.min = min;
        this.max = max;
    }

    public RectNd(double[] min, double[] max) {
        this.min = new PointNd(min);
        this.max = new PointNd(max);
    }

    public long getDataNodeId() {
        return dataNodeId;
    }

    public void setDataNodeId(long dataNodeId) {
        this.dataNodeId = dataNodeId;
    }

    @Override
    public boolean equals(Object o) {
        RectNd rectNd = (RectNd) o;
        if (dataNodeId > 0 && dataNodeId == rectNd.dataNodeId) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(dataNodeId);
    }

    @Override
    public HyperRect getMbr(HyperRect r) {
        RectNd r2 = (RectNd) r;
        double[] minXs = new double[min.getNDim()];
        double[] maxXs = new double[min.getNDim()];
        for (int i = 0; i < min.getNDim(); i++) {
            double max1 = max.getCoord(i);
            double max2 = r2.max.getCoord(i);
            maxXs[i] = max1 > max2 ? max1 : max2;

            double min1 = min.getCoord(i);
            double min2 = r2.min.getCoord(i);
            minXs[i] = min1 < min2 ? min1 : min2;
        }
        return new RectNd(new PointNd(minXs), new PointNd(maxXs));
    }

    @Override
    public int getNDim() {
        return min.getNDim();
    }

    public double[] getMinXs() {
        return min.getXs();
    }

    @Override
    public HyperPoint getMin() {
        return min;
    }

    public double[] getMaxXs() {
        return max.getXs();
    }

    @Override
    public HyperPoint getMax() {
        return max;
    }

    @Override
    public HyperPoint getCentroid() {
        double[] avgs = new double[min.getNDim()];
        for (int i = 0; i < min.getNDim(); i++) {
            avgs[i] = (min.getCoord(i) + max.getCoord(i)) / 2;
        }
        return new PointNd(avgs);
    }

    @Override
    public double getRange(int d) {
        return max.getCoord(d) - min.getCoord(d);
    }

    @Override
    public boolean contains(HyperRect r) {
        RectNd r2 = (RectNd) r;
        for (int i = 0; i < min.getNDim(); i++) {
            if (!(min.getCoord(i) <= r2.min.getCoord(i) &&
                    max.getCoord(i) >= r2.max.getCoord(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean intersects(HyperRect r) {
        RectNd r2 = (RectNd) r;
        for (int i = 0; i < min.getNDim(); i++) {
            if (min.getCoord(i) > r2.max.getCoord(i) ||
                    r2.min.getCoord(i) > max.getCoord(i)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public double cost() {
        double res = 1;
        for (int i = 0; i < min.getNDim(); i++) {
            res = res * (max.getCoord(i) - min.getCoord(i));
        }
        return Math.abs(res);
    }

    @Override
    public double perimeter() {
        double n = Math.pow(2, getNDim());
        double p = 0.0;
        final int nD = this.getNDim();
        for (int d = 0; d < nD; d++) {
            p += n * this.getRange(d);
        }
        return p;
    }


    public final static class Builder implements RectBuilder {
        public Builder() {
        }

        @Override
        public HyperRect getBBox(RectNd rectNd) {
            return rectNd;
        }

        @Override
        public HyperRect getMbr(HyperPoint p1, HyperPoint p2) {
            double[] minXs = new double[p1.getNDim()];
            double[] maxXs = new double[p1.getNDim()];

            for (int i = 0; i < p1.getNDim(); i++) {
                double x1 = p1.getCoord(i);
                double x2 = p2.getCoord(i);
                if (x1 > x2) {
                    minXs[i] = x2;
                    maxXs[i] = x1;
                } else {
                    minXs[i] = x1;
                    maxXs[i] = x2;
                }
            }

            PointNd min = new PointNd(minXs);
            PointNd max = new PointNd(maxXs);
            return new RectNd(min, max);
        }
    }
}
