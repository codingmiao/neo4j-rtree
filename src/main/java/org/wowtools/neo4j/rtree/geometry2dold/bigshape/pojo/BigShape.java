package org.wowtools.neo4j.rtree.geometry2dold.bigshape.pojo;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.wowtools.common.utils.LruCache;
import org.wowtools.neo4j.rtree.geometry2dold.Constant;
import org.wowtools.neo4j.rtree.geometry2dold.bigshape.BigShapeManager;
import org.wowtools.neo4j.rtree.geometry2dold.spatial.RTreeIndex;
import org.wowtools.neo4j.rtree.geometry2dold.util.BboxIntersectUtil;
import org.wowtools.neo4j.rtree.geometry2dold.util.GeometryBbox;
import org.wowtools.neo4j.rtree.geometry2dold.util.WkbReaderManager;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;

/**
 * 大shape对象
 *
 * @author liuyu
 * @date 2020/11/23
 */
public class BigShape {
    private final RTreeIndex rTreeIndex;
    private final int nodeValueCacheSize;

    public BigShape(RTreeIndex rTreeIndex, int nodeValueCacheSize) {
        this.rTreeIndex = rTreeIndex;
        this.nodeValueCacheSize = nodeValueCacheSize;
    }

//    /**
//     * 计算输入的geometry是否与本BigShape的相交部分
//     * TODO 下个版本再实现
//     * @param tx 图数据库事务
//     * @param g  输入geometry
//     * @return
//     */
//    public Geometry intersection(Transaction tx, Geometry g) {
//        return null;
//    }

    /**
     * 判断输入的geometry是否与本BigShape相交
     *
     * @param tx 图数据库事务
     * @param g  输入geometry
     * @return
     */
    public boolean intersects(Transaction tx, Geometry g) {
        final IntersectsJudge intersectsJudge;
        if (g instanceof Point) {
            intersectsJudge = new PointIntersectsJudge(g, nodeValueCacheSize);
        } else {
            intersectsJudge = new OtherIntersectsJudge(g, nodeValueCacheSize);
        }
        //FIXME 这里由于找到一个相交对象即可return，所以未复用RtreeTraverser的方法，后续有相同场景的话抽取公共代码
        Node rtreeNode = rTreeIndex.getIndexRoot(tx);
        Deque<Node> stack = new ArrayDeque<>();//辅助遍历的栈
        stack.push(rtreeNode);
        while (!stack.isEmpty()) {
            rtreeNode = stack.pop();
            //判断当前节点的bbox是否与输入bbox相交
            double[] nodeBbox = (double[]) rtreeNode.getProperty(Constant.RtreeProperty.bbox);
            if (!BboxIntersectUtil.bboxIntersect(intersectsJudge.geoBbox, nodeBbox)) {
                continue;
            }

            if (rtreeNode.hasRelationship(Direction.OUTGOING, Constant.Relationship.RTREE_CHILD)) {
                //若有下级索引节点,下级索引节点入栈
                for (Relationship relationship : rtreeNode.getRelationships(Direction.OUTGOING, Constant.Relationship.RTREE_CHILD)) {
                    Node child = relationship.getEndNode();
                    stack.push(child);
                }
            } else if (rtreeNode.hasRelationship(Direction.OUTGOING, Constant.Relationship.RTREE_REFERENCE)) {
                //若有下级对象节点，返回结果
                for (Relationship relationship : rtreeNode.getRelationships(Direction.OUTGOING, Constant.Relationship.RTREE_REFERENCE)) {
                    Node objNode = relationship.getEndNode();
                    if (intersectsJudge.judge(objNode)) {
                        return true;//有一个相交即可直接返回
                    }
                }
            }
        }
        return false;
    }

    /**
     * 判断geometry是相交的抽象类
     */
    private static abstract class IntersectsJudge {

        protected final Geometry geometry;
        protected final double[] geoBbox;
        protected final Map<Long, Object> cache;

        public IntersectsJudge(Geometry geometry, int cacheSize) {
            this.geometry = geometry;
            geoBbox = GeometryBbox.getBbox(geometry).toDoubleArray();
            cache = LruCache.buildCache(cacheSize, 32);
        }

        public boolean judge(Node node) {
            Object value = cache.get(node.getId());
            if (null == value) {
                value = node.getProperty(BigShapeManager.keyFieldName);
                if (value instanceof byte[]) {
                    try {
                        value = WkbReaderManager.get().read((byte[]) value);
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
                }
                cache.put(node.getId(), value);
            }
            return judgeValue(value);
        }

        protected boolean judgeValue(Object value) {
            if (value instanceof double[]) {//bbox
                return judgeBbox((double[]) value);
            } else {//wkb
                return judgeGeometry((Geometry) value);
            }
        }

        abstract boolean judgeBbox(double[] bbox);

        protected boolean judgeGeometry(Geometry geometry) {
//            double[] gb = GeometryBbox.getBbox(geometry).toDoubleArray();
//            if(!BboxIntersectUtil.bboxIntersect(gb,geoBbox)){
//                return false;
//            }
            return this.geometry.intersects(geometry);
        }
    }

    /**
     * 点相交判断
     */
    private static class PointIntersectsJudge extends IntersectsJudge {

        private final double x;
        private final double y;

        public PointIntersectsJudge(Geometry geometry, int cacheSize) {
            super(geometry, cacheSize);
            Point point = (Point) geometry;
            x = point.getX();
            y = point.getY();
        }

        @Override
        boolean judgeBbox(double[] bbox) {
            return BboxIntersectUtil.pointInBbox(bbox, x, y);
        }

    }

    /**
     * 其它geometry相交判断
     */
    private static class OtherIntersectsJudge extends IntersectsJudge {

        public OtherIntersectsJudge(Geometry geometry, int cacheSize) {
            super(geometry, cacheSize);
        }

        @Override
        boolean judgeBbox(double[] bbox) {
            if (!BboxIntersectUtil.bboxIntersect(geoBbox, bbox)) {
                return false;//bbox不相交的话肯定不会相交
            }
            return geometry.intersects(BboxIntersectUtil.bbox2Geometry(bbox));
        }

    }

}
