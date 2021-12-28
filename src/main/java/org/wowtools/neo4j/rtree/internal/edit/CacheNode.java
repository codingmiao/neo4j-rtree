package org.wowtools.neo4j.rtree.internal.edit;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.wowtools.neo4j.rtree.internal.define.Relationships;
import org.wowtools.neo4j.rtree.pojo.PointNd;
import org.wowtools.neo4j.rtree.pojo.RectNd;
import org.wowtools.neo4j.rtree.util.TxCell;

import java.util.*;

/**
 * 缓存neo4j中的数据，事务提交时清空缓存
 *
 * @author liuyu
 * @date 2021/12/17
 */
public class CacheNode {
    private static final Object empty = new Object() {
        @Override
        public String toString() {
            return "emptyObj";
        }
    };

    private final long nodeId;
    private final TxCell txCell;
    private final HashMap<String, Object> properties = new HashMap<>();
    private final HashSet<String> changedKey = new HashSet<>();//标记哪些属性发生过变化，commit时统一node.setProperty
    private RectNd mbr;
    private org.wowtools.neo4j.rtree.internal.edit.Node[] children;
    private RectNd[] r;
    private RectNd[] entry;

    public final NodeType nodeType;


    public CacheNode(long nodeId, TxCell txCell, NodeType nodeType) {
        this.nodeId = nodeId;
        this.txCell = txCell;
        this.nodeType = nodeType;
    }

    public void commit() {
        if (changedKey.size() == 0) {
            return;
        }
        Node node = _node();
        for (String k : changedKey) {
            Object v = properties.get(k);
            if (v == empty) {
                node.removeProperty(k);
            } else {
                node.setProperty(k, v);
            }
        }
    }

    public void clearCache() {
        properties.clear();
        changedKey.clear();
        mbr = null;
        children = null;
        r = null;
        entry = null;
    }

    public Object getProperty(String key) {
        Object p = properties.get(key);
        if (empty == p) {
            return null;
        }
        if (null != p) {
            return p;
        }
        p = _node().getProperty(key, null);
        if (null == p) {
            properties.put(key, empty);
            return null;
        } else {
            return p;
        }
    }

    private Map<String, Object> getProperties(List<String> keys) {
        HashMap<String, Object> res = new HashMap<>(keys.size());
        //读缓存
        LinkedList<String> notInCacheKeys = new LinkedList<>();
        for (String key : keys) {
            Object p = properties.get(key);
            if (empty == p) {
                continue;
            } else if (null == p) {
                notInCacheKeys.add(key);
            } else {
                res.put(key, p);
            }
        }
        //不在缓存中的查图库
        if (notInCacheKeys.size() > 0) {
            String[] queryKeys = new String[notInCacheKeys.size()];
            notInCacheKeys.toArray(queryKeys);
            Map<String, Object> np = _node().getProperties(queryKeys);
            np.forEach((k, v) -> {
                if (null == v) {
                    properties.put(k, empty);
                } else {
                    properties.put(k, v);
                    res.put(k, v);
                }
            });
        }
        return res;
    }

    public void setProperty(String key, Object value) {
        changedKey.add(key);
        if (null == value) {
//            _node().removeProperty(key);
//            properties.remove(key);
            properties.put(key, empty);
        } else {
//            _node().setProperty(key, value);
            properties.put(key, value);
        }
    }

    public Node getNode() {
        return _node();
    }


    public org.wowtools.neo4j.rtree.internal.edit.Node[] getChildren() {
        if (null == children) {
            int mMax = (int) getProperty("mMax");
            children = new org.wowtools.neo4j.rtree.internal.edit.Node[mMax];
            int i = 0;
            for (Relationship relationship : _node().getRelationships(Direction.OUTGOING, Relationships.RTREE_PARENT_TO_CHILD)) {
                children[i] = txCell.getNodeFromNeo4j(relationship.getEndNodeId());
                i++;
            }
        }

        return children;
    }

    public void setChildAtI(int i, org.wowtools.neo4j.rtree.internal.edit.Node node) {
        children = getChildren();
        org.wowtools.neo4j.rtree.internal.edit.Node old = children[i];
        Transaction tx = txCell.getTx();
        if (null != old) {
            //切断旧node的引用
            Node oldNode = tx.getNodeById(old.getNeoNodeId());
            oldNode.getRelationships(Direction.INCOMING, Relationships.RTREE_PARENT_TO_CHILD).forEach((r) -> {
                r.delete();
            });
        }
        children[i] = node;
        if (null != node) {
            //建立新节点的引用
            Node neoNode = tx.getNodeById(node.getNeoNodeId());
            tx.getNodeById(nodeId).createRelationshipTo(neoNode, Relationships.RTREE_PARENT_TO_CHILD);
        }

    }


    public int addChild(final org.wowtools.neo4j.rtree.internal.edit.Node n) {
        int size = (int) getProperty("size");
        int mMax = (int) getProperty("mMax");
        RectNd mbr = getMbr();
        if (size < mMax) {
            setChildAtI(size, n);
            size += 1;
            setProperty("size", size);

            if (mbr != null) {
                mbr = mbr.getMbr(n.getBound());
            } else {
                mbr = n.getBound();
            }
            setMbr((RectNd) mbr);

//            //建立新节点的引用
//            Transaction tx = txCell.getTx();
//            Node neoNode = tx.getNodeById(n.getNeoNodeId());
//
//            neoNode.getRelationships(Direction.INCOMING, Relationships.RTREE_PARENT_TO_CHILD).forEach((r) -> {
//                r.delete();
//            });
//            tx.getNodeById(nodeId).createRelationshipTo(neoNode, Relationships.RTREE_PARENT_TO_CHILD);

            return size - 1;
        } else {
            throw new RuntimeException("Too many children");
        }
    }

    public RectNd[] getR() {
        if (null == r) {
            int mMax = (int) getProperty("mMax");
            r = new RectNd[mMax];
            for (int i = 0; i < mMax; i++) {
                double[] rMinI = (double[]) getProperty("rMin" + i);
                if (null == rMinI) {
                    continue;
                }
                double[] rMaxI = (double[]) getProperty("rMax" + i);
                r[i] = new RectNd(new PointNd(rMinI), new PointNd(rMaxI));
            }
        }
        return r;
    }

    public void setRAtI(int i, RectNd ri) {
        r = getR();
        if (null == ri) {
            setProperty("rMin" + i, null);
            setProperty("rMax" + i, null);
        } else {
            setProperty("rMin" + i, ri.getMinXs());
            setProperty("rMax" + i, ri.getMaxXs());
        }
        r[i] = ri;

    }

    public RectNd[] getEntry() {
        if (null == entry) {
            int mMax = (int) getProperty("mMax");
            List<String> keys = new ArrayList<>(mMax * 3);
            for (int i = 0; i < mMax; i++) {
                keys.add("entryMin" + i);
                keys.add("entryMax" + i);
                keys.add("entryDataId" + i);
            }
            Map<String, Object> properties = getProperties(keys);
            entry = new RectNd[mMax];
            for (int i = 0; i < mMax; i++) {
                double[] eMinI = (double[]) properties.get("entryMin" + i);
                if (null == eMinI) {
                    continue;
                }
                double[] eMaxI = (double[]) properties.get("entryMax" + i);
                RectNd e = new RectNd(new PointNd(eMinI), new PointNd(eMaxI));
                e.setDataNodeId((long) properties.get("entryDataId" + i));
                entry[i] = e;

            }
        }
        return entry;
    }

    public void setEntryAtI(int i, RectNd ei) {
        entry = getEntry();
        if (null == ei) {
            setProperty("entryMin" + i, null);
            setProperty("entryMax" + i, null);
            setProperty("entryDataId" + i, null);
        } else {
            setProperty("entryMin" + i, ei.getMinXs());
            setProperty("entryMax" + i, ei.getMaxXs());
            setProperty("entryDataId" + i, ei.getDataNodeId());
        }
        entry[i] = ei;
    }


    public RectNd getMbr() {
        Node node = _node();
        if (null == mbr) {
            double[] mbrMin = (double[]) node.getProperty("mbrMin", null);
            if (null == mbrMin) {
                return null;
            }
            double[] mbrMax = (double[]) node.getProperty("mbrMax");
            mbr = new RectNd(new PointNd(mbrMin), new PointNd(mbrMax));
        }
        return mbr;
    }

    public void setMbr(RectNd mbr) {
        if (mbr == null) {
            setProperty("mbrMin", null);
            setProperty("mbrMax", null);
        } else {
            double[] mbrMin = mbr.getMinXs();
            double[] mbrMax = mbr.getMaxXs();
            setProperty("mbrMin", mbrMin);
            setProperty("mbrMax", mbrMax);
        }

        this.mbr = mbr;
    }

    private Node _node() {
        return txCell.getTx().getNodeById(nodeId);
    }

    public enum NodeType {
        Branch("Branch"),
        Leaf("Leaf");
        public final String type;

        NodeType(String type) {
            this.type = type;
        }
    }
}
