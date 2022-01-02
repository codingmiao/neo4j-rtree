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
            properties.put(key, empty);
        } else {
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
            HashSet<Long> added = new HashSet<>();
            for (Relationship relationship : _node().getRelationships(Direction.OUTGOING, Relationships.RTREE_PARENT_TO_CHILD)) {
                children[i] = txCell.getNodeFromNeo4j(relationship.getEndNodeId());
                if (!added.add(relationship.getEndNodeId())) {
                    throw new RuntimeException();
                }
                i++;
            }
        }

        return children;
    }

    public void setChildAtI(int i, org.wowtools.neo4j.rtree.internal.edit.Node node) {
        children = getChildren();
        org.wowtools.neo4j.rtree.internal.edit.Node old = children[i];
        if (null != old) {
            //切断旧node的引用
            txCell.setNodeParent(old.getNeoNodeId(), null);
        }
        children[i] = node;
        if (null != node) {
            //建立新节点的引用
            txCell.setNodeParent(node.getNeoNodeId(), nodeId);

        }

    }


    /**
     * 把i+1位置开始，所有child的位置上移一位
     * 作用等同于System.arraycopy(child, i + 1, child, i, size - i - 1)
     *
     * @param i
     */
    public void childIndexUp(int i) {
        int size = (int) getProperty("size");
        children = getChildren();

        int j = i;
        int num = size - i - 1;
        for (int i1 = 0; i1 < num; i1++) {
//            setChildAtI(j, children[j + 1]);
            children[j] = children[j + 1];
            j++;
        }
        size--;
        setProperty("size", size);
        org.wowtools.neo4j.rtree.internal.edit.Node old = children[size];
        if (null != old) {
            txCell.setNodeParent(old.getNeoNodeId(), null);
            children[size] = null;
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
            return size - 1;
        } else {
            throw new RuntimeException("Too many children");
        }
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
