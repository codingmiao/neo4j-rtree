/**
 * Copyright (c) 2002-2013 "Neo Technology," Network Engine for Objects in Lund
 * AB [http://neotechnology.com]
 * <p>
 * This file is part of Neo4j Spatial.
 * <p>
 * Neo4j is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 * <p>
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.wowtools.neo4j.rtree.geometry2dold.spatial;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.neo4j.graphdb.*;
import org.wowtools.common.utils.LruCache;
import org.wowtools.neo4j.rtree.geometry2dold.Constant;
import org.wowtools.neo4j.rtree.geometry2dold.RTreeIndexManager;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * 索引，注意，不要直接new，通过RTreeIndexManager构建，否则可能存在潜在的并发安全问题
 *
 * @see RTreeIndexManager
 */
public class RTreeIndex {

    private final String indexName;

    public static int concurrency = 32;
    private final String geometryFieldName;

    /**
     * 从node中读geometry的接口
     */
    private interface NodeGeometryReader {
        Geometry read(Node objNode, WKBReader wkbReader);

        void clear();

        void remove(long nodeId);
    }

    private static final class NoCacheNodeGeometryReader implements NodeGeometryReader {
        private final String geometryFieldName;

        public NoCacheNodeGeometryReader(String geometryFieldName) {
            this.geometryFieldName = geometryFieldName;
        }

        @Override
        public Geometry read(Node objNode, WKBReader wkbReader) {
            return getObjNodeGeometryFromNode(objNode, geometryFieldName, wkbReader);
        }

        @Override
        public void clear() {

        }

        @Override
        public void remove(long nodeId) {

        }
    }

    private static final class CacheNodeGeometryReader implements NodeGeometryReader {
        private final String geometryFieldName;
        /**
         * 由于对外wkb缓存转到堆内并转geometry比较耗时，故这里加了一个lru的缓存以直接获取geometry
         */
        private final Map<Long, Geometry> geometryCache;
        private final ReentrantReadWriteLock geometryCacheLock = new ReentrantReadWriteLock();

        public CacheNodeGeometryReader(Map<Long, Geometry> geometryCache, String geometryFieldName) {
            this.geometryCache = geometryCache;
            this.geometryFieldName = geometryFieldName;

        }

        @Override
        public Geometry read(Node objNode, WKBReader wkbReader) {
            Geometry geometry;
            //先读缓存
            if (geometryCacheLock.readLock().tryLock()) {//tryLock来保证在缓存命中率低时不至于被锁浪费时间,下同
                geometry = geometryCache.get(objNode.getId());
                geometryCacheLock.readLock().unlock();
                if (null != geometry) {
                    return geometry;
                }
            } else {
                return getObjNodeGeometryFromNode(objNode, geometryFieldName, wkbReader);
            }

            //缓存没有再读neo4j并写入缓存
            geometry = getObjNodeGeometryFromNode(objNode, geometryFieldName, wkbReader);
            if (geometryCacheLock.writeLock().tryLock()) {
                geometryCache.put(objNode.getId(), geometry);
                geometryCacheLock.writeLock().unlock();
            }

            return geometry;
        }

        @Override
        public void clear() {
            geometryCacheLock.writeLock().lock();
            geometryCache.clear();
            geometryCacheLock.writeLock().unlock();
        }

        @Override
        public void remove(long nodeId) {
            geometryCacheLock.writeLock().lock();
            geometryCache.remove(nodeId);
            geometryCacheLock.writeLock().unlock();
        }
    }


    private final NodeGeometryReader nodeGeometryReader;

    public RTreeIndex(String indexName, String geometryFieldName, GraphDatabaseService database, EnvelopeDecoder envelopeDecoder, int maxNodeReferences, int geometryCacheSize, boolean init) {
        this.indexName = indexName;
        this.geometryFieldName = geometryFieldName;
        if (geometryCacheSize > 0) {
            Map<Long, Geometry> geometryCache = LruCache.buildCache(geometryCacheSize, concurrency);
            nodeGeometryReader = new CacheNodeGeometryReader(geometryCache, geometryFieldName);
        } else {
            nodeGeometryReader = new NoCacheNodeGeometryReader(geometryFieldName);
        }
        if (init) {
            try (Transaction tx = database.beginTx()) {
                Node rootNode = tx.createNode(Constant.RtreeLabel.ReferenceNode);
                rootNode.setProperty(Constant.RtreeProperty.indexName, indexName);
                rootNode.setProperty(Constant.RtreeProperty.geometryFieldName, geometryFieldName);
                rootNode.setProperty(Constant.RtreeProperty.maxNodeReferences, maxNodeReferences);
                this.rootNodeId = rootNode.getId();
                tx.commit();
            }
        } else {
            try (Transaction tx = database.beginTx()) {
                Node rootNode = tx.findNode(Constant.RtreeLabel.ReferenceNode, Constant.RtreeProperty.indexName, indexName);
                this.rootNodeId = rootNode.getId();
            }
        }
        this.database = database;
        this.envelopeDecoder = envelopeDecoder;
        this.maxNodeReferences = maxNodeReferences;
        monitor = new EmptyMonitor();
        if (envelopeDecoder == null) {
            throw new NullPointerException("envelopeDecoder is NULL");
        }
        if (init) {
            initIndexRoot();
            initIndexMetadata();
        }
    }

    public static final String INDEX_PROP_BBOX = Constant.RtreeProperty.bbox;

    public static final String KEY_SPLIT = "splitMode";
    public static final String QUADRATIC_SPLIT = "quadratic";
    public static final String GREENES_SPLIT = "greene";

    public static final String KEY_MAX_NODE_REFERENCES = Constant.RtreeProperty.maxNodeReferences;
    public static final String KEY_SHOULD_MERGE_TREES = "shouldMergeTrees";
    public static final long MIN_MAX_NODE_REFERENCES = 10;
    public static final long MAX_MAX_NODE_REFERENCES = 1000000;


    private TreeMonitor monitor;

    public void addMonitor(TreeMonitor monitor) {
        this.monitor = monitor;
    }


    public EnvelopeDecoder getEnvelopeDecoder() {
        return this.envelopeDecoder;
    }

    public void configure(Map<String, Object> config) {
        for (String key : config.keySet()) {
            switch (key) {
                case KEY_SPLIT:
                    String value = config.get(key).toString();
                    switch (value) {
                        case QUADRATIC_SPLIT:
                        case GREENES_SPLIT:
                            splitMode = value;
                            break;
                        default:
                            throw new IllegalArgumentException("No such RTreeIndex value for '" + key + "': " + value);
                    }
                    break;
                case KEY_MAX_NODE_REFERENCES:
                    int intValue = Integer.parseInt(config.get(key).toString());
                    if (intValue < MIN_MAX_NODE_REFERENCES) {
                        throw new IllegalArgumentException("RTreeIndex does not allow " + key + " less than " + MIN_MAX_NODE_REFERENCES);
                    }
                    if (intValue > MAX_MAX_NODE_REFERENCES) {
                        throw new IllegalArgumentException("RTreeIndex does not allow " + key + " greater than " + MAX_MAX_NODE_REFERENCES);
                    }
                    this.maxNodeReferences = intValue;
                    break;
                case KEY_SHOULD_MERGE_TREES:
                    this.shouldMergeTrees = Boolean.parseBoolean(config.get(key).toString());
                    break;
                default:
                    throw new IllegalArgumentException("No such RTreeIndex configuration key: " + key);
            }
        }
    }

    public synchronized void add(Node geomNode, Transaction tx) {
        // initialize the search with root
        Node parent = getIndexRoot(tx);

        addBelow(parent, geomNode, tx);

        countSaved = false;
        totalGeometryCount++;
    }


    public Geometry getObjNodeGeometry(Node objNode, WKBReader wkbReader) {
        return nodeGeometryReader.read(objNode, wkbReader);
    }

    private static Geometry getObjNodeGeometryFromNode(Node objNode, String geometryFieldName, WKBReader wkbReader) {
        byte[] wkb = (byte[]) objNode.getProperty(geometryFieldName);
        try {
            Geometry geometry = wkbReader.read(wkb);
            return geometry;
        } catch (ParseException e) {
            throw new RuntimeException("parse wkb error", e);
        }
    }

    /**
     * This method will add the node somewhere below the parent.
     */
    private void addBelow(Node parent, Node geomNode, Transaction tx) {
        // 找到合适的rtree树叶子节点，后续把geomNode挂到叶子节点上
        while (!nodeIsLeaf(parent)) {
            parent = chooseSubTree(parent, geomNode, tx);
        }
        if (countChildren(parent, RTreeRelationshipTypes.RTREE_REFERENCE) >= maxNodeReferences) {
            //超过最大限制，分裂节点
            insertInLeaf(parent, geomNode, tx);
            splitAndAdjustPathBoundingBox(parent, tx);
        } else {
            //没有超过最大限制，直接添加
            if (insertInLeaf(parent, geomNode, tx)) {
                // bbox enlargement needed
                adjustPathBoundingBox(parent, tx);
            }
        }
    }


    /**
     * Use this method if you want to insert an index node as a child of a given index node. This will recursively
     * update the bounding boxes above the parent to keep the tree consistent.
     */
    private void insertIndexNodeOnParent(Node parent, Node child, Transaction tx) {
        int numChildren = countChildren(parent, RTreeRelationshipTypes.RTREE_CHILD);
        boolean needExpansion = addChild(parent, RTreeRelationshipTypes.RTREE_CHILD, child, tx);
        if (numChildren < maxNodeReferences) {
            if (needExpansion) {
                adjustPathBoundingBox(parent, tx);
            }
        } else {
            splitAndAdjustPathBoundingBox(parent, tx);
        }
    }

    /**
     * 最大单次添加节点数，若add(List geomNodes, Transaction tx)方法的geomNodes超过此值，将被自动拆分为多次添加以避免栈内存溢出
     */
    public static int MaxAddListSize = 1024;

    /**
     * Depending on the size of the incumbent tree, this will either attempt to rebuild the entire index from scratch
     * (strategy used if the insert larger than 40% of the current tree size - may give heap out of memory errors for
     * large inserts as has O(n) space complexity in the total tree size. It has n*log(n) time complexity. See function
     * partition for more details.) or it will insert using the method of seeded clustering, where you attempt to use the
     * existing tree structure to partition your data.
     * This is based on the Paper "Bulk Insertion for R-trees by seeded clustering" by T.Lee, S.Lee B Moon.
     * Repeated use of this strategy will lead to degraded query performance, especially if used for
     * many relatively small insertions compared to tree size. Though not worse than one by one insertion.
     * In practice, it should be fine for most uses.
     *
     * @param geomNodes ...
     * @param tx        ...
     */

    public synchronized void add(List<Node> geomNodes, Transaction tx) {
        if (geomNodes.size() > MaxAddListSize) {
            ArrayList<Node> subList = new ArrayList<>(MaxAddListSize);
            for (Node geomNode : geomNodes) {
                subList.add(geomNode);
                if (subList.size() == MaxAddListSize) {
                    _add(subList, tx);
                    subList = new ArrayList<>(MaxAddListSize);
                }
            }
            if (subList.size() > 0) {
                _add(subList, tx);
            }
        } else {
            _add(geomNodes, tx);
        }


    }

    private void _add(List<Node> geomNodes, Transaction tx) {
        //If the insertion is large relative to the size of the tree, simply rebuild the whole tree.
        if (geomNodes.size() > totalGeometryCount * 0.4) {
            List<Node> nodesToAdd = new ArrayList<>(geomNodes.size() + totalGeometryCount);
            for (Node n : getAllIndexedNodes(tx)) {
                nodesToAdd.add(n);
            }
            nodesToAdd.addAll(geomNodes);
            detachGeometryNodes(false, getIndexRoot(tx), new NullListener(), tx);
            deleteTreeBelow(getIndexRoot(tx));
            buildRtreeFromScratch(getIndexRoot(tx), decodeGeometryNodeEnvelopes(nodesToAdd), 0.7, tx);
            countSaved = false;
            totalGeometryCount = nodesToAdd.size();
            monitor.addNbrRebuilt(this);
        } else {

            List<NodeWithEnvelope> outliers = bulkInsertion(getIndexRoot(tx), getHeight(getIndexRoot(tx), 0), decodeGeometryNodeEnvelopes(geomNodes), 0.7, tx);
            countSaved = false;
            totalGeometryCount = totalGeometryCount + (geomNodes.size() - outliers.size());
            for (NodeWithEnvelope n : outliers) {
                add(n.node, tx);
            }
        }
    }

    private List<NodeWithEnvelope> decodeGeometryNodeEnvelopes(List<Node> nodes) {
        return nodes.stream().map(GeometryNodeWithEnvelope::new).collect(Collectors.toList());
    }

    public static class NodeWithEnvelope {
        public Envelope envelope;
        Node node;

        public NodeWithEnvelope(Node node, Envelope envelope) {
            this.node = node;
            this.envelope = envelope;
        }
    }

    public class GeometryNodeWithEnvelope extends NodeWithEnvelope {
        GeometryNodeWithEnvelope(Node node) {
            super(node, envelopeDecoder.decodeEnvelope(node));
        }
    }

    /**
     * Returns the height of the tree, starting with the rootNode and adding one for each subsequent level. Relies on the
     * balanced property of the RTree that all leaves are on the same level and no index nodes are empty. In the convention
     * the index is level 0, so if there is just the index and the leaf nodes, the leaf nodes are level one and the height is one.
     * Thus the lowest level is 1.
     */
    int getHeight(Node rootNode, int height) {
        Iterator<Relationship> rels = rootNode.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD).iterator();
        if (rels.hasNext()) {
            return getHeight(rels.next().getEndNode(), height + 1);
        } else {
            // Add one to account for the step to leaf nodes.
            return height + 1; // todo should this really be +1 ?
        }
    }

    List<NodeWithEnvelope> getIndexChildren(Node rootNode, Transaction tx) {
        List<NodeWithEnvelope> result = new ArrayList<>();
        for (Relationship r : rootNode.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD)) {
            Node child = r.getEndNode();
            result.add(new NodeWithEnvelope(child, getIndexNodeEnvelope(child, tx)));
        }
        return result;
    }

    private List<NodeWithEnvelope> getIndexChildren(Node rootNode, int depth, Transaction tx) {
        if (depth < 1) {
            throw new IllegalArgumentException("Depths must be at least one");
        }

        List<NodeWithEnvelope> rootChildren = getIndexChildren(rootNode, tx);
        if (depth == 1) {
            return rootChildren;
        } else {
            List<NodeWithEnvelope> result = new ArrayList<>(rootChildren.size() * 5);
            for (NodeWithEnvelope child : rootChildren) {
                result.addAll(getIndexChildren(child.node, depth - 1, tx));
            }
            return result;
        }
    }

    private List<NodeWithEnvelope> bulkInsertion(Node rootNode, int rootNodeHeight, final List<NodeWithEnvelope> geomNodes, final double loadingFactor, Transaction tx) {
        List<NodeWithEnvelope> children = getIndexChildren(rootNode, tx);
        if (children.isEmpty()) {
            return geomNodes;
        }
        children.sort(new IndexNodeAreaComparator());

        Map<NodeWithEnvelope, List<NodeWithEnvelope>> map = new HashMap<>(children.size());
        int nodesPerRootSubTree = Math.max(16, geomNodes.size() / children.size());
        for (NodeWithEnvelope n : children) {
            map.put(n, new ArrayList<>(nodesPerRootSubTree));
        }

        // The outliers are those nodes which do not fit into the existing tree hierarchy.
        List<NodeWithEnvelope> outliers = new ArrayList<>(geomNodes.size() / 10); // 10% outliers
        for (NodeWithEnvelope n : geomNodes) {
            Envelope env = n.envelope;
            boolean flag = true;

            //exploits that the iterator returns the list inorder, which is sorted by size, as above. Thus child
            //is always added to the smallest existing envelope which contains it.
            for (NodeWithEnvelope c : children) {
                if (c.envelope.contains(env)) {
                    map.get(c).add(n); //add to smallest area envelope which contains the child;
                    flag = false;
                    break;
                }
            }
            // else add to outliers.
            if (flag) {
                outliers.add(n);
            }
        }
        for (NodeWithEnvelope child : children) {
            List<NodeWithEnvelope> cluster = map.get(child);

            if (cluster.isEmpty()) continue;

            // todo move each branch into a named method
            int expectedHeight = expectedHeight(loadingFactor, cluster.size());

            //In an rtree is this height it will add as a single child to the current child node.
            int currentRTreeHeight = rootNodeHeight - 2;
//			if(expectedHeight-currentRTreeHeight > 1 ){
//				throw new RuntimeException("Due to h_i-l_t > 1");
//			}
            if (expectedHeight < currentRTreeHeight) {
                monitor.addCase("h_i < l_t ");
                //if the height is smaller than that recursively sort and split.
                outliers.addAll(bulkInsertion(child.node, rootNodeHeight - 1, cluster, loadingFactor, tx));
            } //if constructed tree is the correct size insert it here.
            else if (expectedHeight == currentRTreeHeight) {

                //Do not create underfull nodes, instead use the add logic, except we know the root not to add them too.
                //this handles the case where the number of nodes in a cluster is small.

                if (cluster.size() < maxNodeReferences * loadingFactor / 2) {
                    monitor.addCase("h_i == l_t && small cluster");
                    // getParent because addition might cause a split. This strategy not ideal,
                    // but does tend to limit overlap more than adding to the child exclusively.

                    for (NodeWithEnvelope n : cluster) {
                        addBelow(rootNode, n.node, tx);
                    }
                } else {
                    monitor.addCase("h_i == l_t && big cluster");
                    Node newRootNode = tx.createNode();
                    buildRtreeFromScratch(newRootNode, cluster, loadingFactor, tx);
                    if (shouldMergeTrees) {
                        NodeWithEnvelope nodeWithEnvelope = new NodeWithEnvelope(newRootNode, getIndexNodeEnvelope(newRootNode, tx));
                        List<NodeWithEnvelope> insert = new ArrayList<>(Arrays.asList(new NodeWithEnvelope[]{nodeWithEnvelope}));
                        monitor.beforeMergeTree(child.node, insert);
                        mergeTwoSubtrees(child, insert, tx);
                        monitor.afterMergeTree(child.node);
                    } else {
                        insertIndexNodeOnParent(child.node, newRootNode, tx);
                    }
                }

            } else {
                Node newRootNode = tx.createNode();
                buildRtreeFromScratch(newRootNode, cluster, loadingFactor, tx);
                int newHeight = getHeight(newRootNode, 0);
                if (newHeight == 1) {
                    monitor.addCase("h_i > l_t (d==1)");
                    for (Relationship geom : newRootNode.getRelationships(RTreeRelationshipTypes.RTREE_REFERENCE)) {
                        addBelow(child.node, geom.getEndNode(), tx);
                        geom.delete();
                    }
                } else {
                    monitor.addCase("h_i > l_t (d>1)");
                    int insertDepth = newHeight - (currentRTreeHeight);
                    List<NodeWithEnvelope> childrenToBeInserted = getIndexChildren(newRootNode, insertDepth, tx);
                    for (NodeWithEnvelope n : childrenToBeInserted) {
                        Relationship relationship = n.node.getSingleRelationship(RTreeRelationshipTypes.RTREE_CHILD, Direction.INCOMING);
                        relationship.delete();
                        if (!shouldMergeTrees) {
                            insertIndexNodeOnParent(child.node, n.node, tx);
                        }
                    }
                    if (shouldMergeTrees) {
                        monitor.beforeMergeTree(child.node, childrenToBeInserted);
                        mergeTwoSubtrees(child, childrenToBeInserted, tx);
                        monitor.afterMergeTree(child.node);
                    }
                }
                // todo wouldn't it be better for this temporary tree to only live in memory?
                deleteRecursivelySubtree(newRootNode, null); // remove the buffer tree remnants
            }
        }
        monitor.addSplit(rootNode); // for debugging via images

        return outliers;
    }

    class NodeTuple {
        private final double overlap;
        NodeWithEnvelope left;
        NodeWithEnvelope right;

        NodeTuple(NodeWithEnvelope left, NodeWithEnvelope right) {
            this.left = left;
            this.right = right;
            this.overlap = left.envelope.overlap(right.envelope);
        }

        boolean contains(NodeWithEnvelope entry) {
            return left.node.equals(entry.node) || right.node.equals(entry.node);
        }
    }

    protected void mergeTwoSubtrees(NodeWithEnvelope parent, List<NodeWithEnvelope> right, Transaction tx) {
        ArrayList<NodeTuple> pairs = new ArrayList<>();
        HashSet<NodeWithEnvelope> disconnectedChildren = new HashSet<>();
        List<NodeWithEnvelope> left = getIndexChildren(parent.node, tx);
        for (NodeWithEnvelope leftNode : left) {
            for (NodeWithEnvelope rightNode : right) {
                NodeTuple pair = new NodeTuple(leftNode, rightNode);
                if (pair.overlap > 0.1) {
                    pairs.add(pair);
                }
            }
        }
        pairs.sort((o1, o2) -> Double.compare(o1.overlap, o2.overlap));
        while (!pairs.isEmpty()) {
            NodeTuple pair = pairs.remove(pairs.size() - 1);
            Envelope merged = new Envelope(pair.left.envelope);
            merged.expandToInclude(pair.right.envelope);
            NodeWithEnvelope newNode = new NodeWithEnvelope(pair.left.node, merged);
            setIndexNodeEnvelope(newNode.node, newNode.envelope);
            List<NodeWithEnvelope> rightChildren = getIndexChildren(pair.right.node, tx);
            pairs.removeIf(t -> t.contains(pair.left) || t.contains(pair.right));
            for (Relationship rel : pair.right.node.getRelationships()) {
                rel.delete();
            }
            disconnectedChildren.add(pair.right);
            mergeTwoSubtrees(newNode, rightChildren, tx);
        }

        right.removeIf(t -> disconnectedChildren.contains(t));
        disconnectedChildren.forEach(t -> t.node.delete());

        for (NodeWithEnvelope n : right) {
            n.node.getSingleRelationship(RTreeRelationshipTypes.RTREE_CHILD, Direction.INCOMING);
            parent.node.createRelationshipTo(n.node, RTreeRelationshipTypes.RTREE_CHILD);
            parent.envelope.expandToInclude(n.envelope);
        }
        setIndexNodeEnvelope(parent.node, parent.envelope);
        if (countChildren(parent.node, RTreeRelationshipTypes.RTREE_CHILD) > maxNodeReferences) {
            splitAndAdjustPathBoundingBox(parent.node, tx);
        } else {
            adjustPathBoundingBox(parent.node, tx);
        }
    }

    private int expectedHeight(double loadingFactor, int size) {
        if (size == 1) {
            return 1;
        } else {
            final int targetLoading = (int) Math.floor(maxNodeReferences * loadingFactor);
            return (int) Math.ceil(Math.log(size) / Math.log(targetLoading)); //exploit change of base formula
        }

    }

    /**
     * This algorithm is based on Overlap Minimizing Top-down Bulk Loading Algorithm for R-tree by T Lee and S Lee.
     * This is effectively a wrapper function around the function Partition which will attempt to parallelise the task.
     * This can work better or worse since the top level may have as few as two nodes, in which case it fails is not optimal.
     * The loadingFactor must be between 0.1 and 1, this is how full each node will be, approximately.
     * Use 1 for static trees (will not be added to after build built), lower numbers if there are to be many subsequent updates.
     * //TODO - Better parallelisation strategy.
     */
    private void buildRtreeFromScratch(Node rootNode, final List<NodeWithEnvelope> geomNodes, double loadingFactor, Transaction tx) {
        partition(rootNode, geomNodes, 0, loadingFactor, tx);
    }


    /**
     * partition方法的输入，用以将递归改为循环
     */
    private static final class PartitionInput {
        final Node indexNode;
        final List<NodeWithEnvelope> nodes;
        final int depth;
        final double loadingFactor;

        public PartitionInput(Node indexNode, List<NodeWithEnvelope> nodes, int depth, double loadingFactor) {
            this.indexNode = indexNode;
            this.nodes = nodes;
            this.depth = depth;
            this.loadingFactor = loadingFactor;
        }
    }

    private void partitionWithoutRecursion(PartitionInput input, Transaction tx) {
        Deque<PartitionInput> stack = new ArrayDeque<>();//辅助遍历的栈
        stack.push(input);
        while (!stack.isEmpty()) {
            input = stack.pop();
            // We want to split by the longest dimension to avoid degrading into extremely thin envelopes
            int longestDimension = findLongestDimension(input.nodes);

            // Sort the entries by the longest dimension and then create envelopes around left and right halves
            input.nodes.sort(new SingleDimensionNodeEnvelopeComparator(longestDimension));

            //work out the number of times to partition it:
            final int targetLoading = (int) Math.round(maxNodeReferences * input.loadingFactor);
            int nodeCount = input.nodes.size();

            if (nodeCount <= targetLoading) {
                // We have few enough nodes to add them directly to the current index node
                boolean expandRootNodeBoundingBox = false;
                for (NodeWithEnvelope n : input.nodes) {
                    expandRootNodeBoundingBox |= insertInLeaf(input.indexNode, n.node, tx);
                }
                if (expandRootNodeBoundingBox) {
                    adjustPathBoundingBox(input.indexNode, tx);
                }
            } else {
                // We have more geometries than can fit in the current index node - create clusters and index them
                final int height = expectedHeight(input.loadingFactor, nodeCount); //exploit change of base formula
                final int subTreeSize = (int) Math.round(Math.pow(targetLoading, height - 1));
                final int numberOfPartitions = (int) Math.ceil((double) nodeCount / (double) subTreeSize);
                // - TODO change this to use the sort function above
                List<List<NodeWithEnvelope>> partitions = partitionList(input.nodes, numberOfPartitions);

                //recurse on each partition
                for (List<NodeWithEnvelope> partition : partitions) {
                    Node newIndexNode = tx.createNode();
                    if (partition.size() > 1) {
//                        partition(newIndexNode, partition, input.depth + 1, input.loadingFactor, tx);
                        stack.push(new PartitionInput(newIndexNode, partition, input.depth + 1, input.loadingFactor));
                    } else {
                        addBelow(newIndexNode, partition.get(0).node, tx);
                    }
                    insertIndexNodeOnParent(input.indexNode, newIndexNode, tx);
                }
                monitor.addSplit(input.indexNode);
            }
        }
    }

    /**
     * This will partition a collection of nodes under the specified index node. The nodes are clustered into one
     * or more groups based on the loading factor, and the tree is expanded if necessary. If the nodes all fit
     * into the parent, they are added directly, otherwise the depth is increased and partition called for each
     * cluster at the deeper depth based on a new root node for each cluster.
     */
    private void partition(Node indexNode, List<NodeWithEnvelope> nodes, int depth, final double loadingFactor, Transaction tx) {
//        partitionWithoutRecursion(new PartitionInput(indexNode,nodes,depth,loadingFactor),tx);
        // We want to split by the longest dimension to avoid degrading into extremely thin envelopes
        int longestDimension = findLongestDimension(nodes);

        // Sort the entries by the longest dimension and then create envelopes around left and right halves
        nodes.sort(new SingleDimensionNodeEnvelopeComparator(longestDimension));

        //work out the number of times to partition it:
        final int targetLoading = (int) Math.round(maxNodeReferences * loadingFactor);
        int nodeCount = nodes.size();

        if (nodeCount <= targetLoading) {
            // We have few enough nodes to add them directly to the current index node
            boolean expandRootNodeBoundingBox = false;
            for (NodeWithEnvelope n : nodes) {
                expandRootNodeBoundingBox |= insertInLeaf(indexNode, n.node, tx);
            }
            if (expandRootNodeBoundingBox) {
                adjustPathBoundingBox(indexNode, tx);
            }
        } else {
            // We have more geometries than can fit in the current index node - create clusters and index them
            final int height = expectedHeight(loadingFactor, nodeCount); //exploit change of base formula
            final int subTreeSize = (int) Math.round(Math.pow(targetLoading, height - 1));
            final int numberOfPartitions = (int) Math.ceil((double) nodeCount / (double) subTreeSize);
            // - TODO change this to use the sort function above
            List<List<NodeWithEnvelope>> partitions = partitionList(nodes, numberOfPartitions);

            //recurse on each partition
            for (List<NodeWithEnvelope> partition : partitions) {
                Node newIndexNode = tx.createNode();
                if (partition.size() > 1) {
                    partition(newIndexNode, partition, depth + 1, loadingFactor, tx);
                } else {
                    addBelow(newIndexNode, partition.get(0).node, tx);
                }
                insertIndexNodeOnParent(indexNode, newIndexNode, tx);
            }
            monitor.addSplit(indexNode);
        }
    }




    // quick dirty way to partition a set into equal sized disjoint subsets
    // - TODO why not use list.sublist() without copying ?

    private List<List<NodeWithEnvelope>> partitionList(List<NodeWithEnvelope> nodes, int numberOfPartitions) {
        int nodeCount = nodes.size();
        List<List<NodeWithEnvelope>> partitions = new ArrayList<>(numberOfPartitions);

        int partitionSize = nodeCount / numberOfPartitions; //it is critical that partitionSize is always less than the target loading.
        if (nodeCount % numberOfPartitions > 0) {
            partitionSize++;
        }
        for (int i = 0; i < numberOfPartitions; i++) {
            partitions.add(nodes.subList(i * partitionSize, Math.min((i + 1) * partitionSize, nodeCount)));
        }
        return partitions;
    }


    public synchronized void remove(long geomNodeId, boolean deleteGeomNode, boolean throwExceptionIfNotFound, Transaction tx) {
        Node geomNode = null;
        // getNodeById throws NotFoundException if node is already removed
        try {
            geomNode = tx.getNodeById(geomNodeId);

        } catch (NotFoundException nfe) {

            // propagate exception only if flag is set
            if (throwExceptionIfNotFound) {
                throw nfe;
            }
        }
        if (geomNode != null && isGeometryNodeIndexed(geomNode)) {

            Node indexNode = findLeafContainingGeometryNode(geomNode);

            // be sure geomNode is inside this RTree
            if (isIndexNodeInThisIndex(indexNode, tx)) {

                // remove the entry
                final Relationship geometryRtreeReference = geomNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_REFERENCE, Direction.INCOMING);
                if (geometryRtreeReference != null) {
                    geometryRtreeReference.delete();
                }
                if (deleteGeomNode) {
                    deleteNode(geomNode);
                }

                // reorganize the tree if needed
                if (countChildren(indexNode, RTreeRelationshipTypes.RTREE_REFERENCE) == 0) {
                    indexNode = deleteEmptyTreeNodes(indexNode, RTreeRelationshipTypes.RTREE_REFERENCE);
                    adjustParentBoundingBox(indexNode, RTreeRelationshipTypes.RTREE_CHILD, tx);
                } else {
                    adjustParentBoundingBox(indexNode, RTreeRelationshipTypes.RTREE_REFERENCE, tx);
                }

                adjustPathBoundingBox(indexNode, tx);

                countSaved = false;
                totalGeometryCount--;
            } else if (throwExceptionIfNotFound) {
                throw new RuntimeException("GeometryNode not indexed in this RTree: " + geomNodeId);
            }
        } else if (throwExceptionIfNotFound) {
            throw new RuntimeException("GeometryNode not indexed with an RTree: " + geomNodeId);
        }
        nodeGeometryReader.remove(geomNodeId);
    }

    private Node deleteEmptyTreeNodes(Node indexNode, RelationshipType relType) {
        if (countChildren(indexNode, relType) == 0) {
            Node parent = getIndexNodeParent(indexNode);
            if (parent != null) {
                indexNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_CHILD, Direction.INCOMING).delete();

                indexNode.delete();
                return deleteEmptyTreeNodes(parent, RTreeRelationshipTypes.RTREE_CHILD);
            } else {
                // root
                return indexNode;
            }
        } else {
            return indexNode;
        }
    }

    private void detachGeometryNodes(final boolean deleteGeomNodes, Node indexRoot, final Listener monitor, Transaction tx) {
        monitor.begin(count());
        try {
            // delete all geometry nodes
            visitInTx(new SpatialIndexVisitor() {
                public boolean needsToVisit(Envelope indexNodeEnvelope) {
                    return true;
                }

                public void onIndexReference(Node geomNode) {
                    geomNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_REFERENCE, Direction.INCOMING).delete();
                    if (deleteGeomNodes) {
                        deleteNode(geomNode);
                    }

                    monitor.worked(1);
                }
            }, indexRoot.getId(), tx);
        } finally {
            monitor.done();
        }
    }


    public synchronized void removeAll(final boolean deleteGeomNodes, final Listener monitor, Transaction tx) {
        Node indexRoot = getIndexRoot(tx);

        detachGeometryNodes(deleteGeomNodes, indexRoot, monitor, tx);

        indexRoot.getSingleRelationship(RTreeRelationshipTypes.RTREE_ROOT, Direction.INCOMING).delete();

        // delete tree
        deleteRecursivelySubtree(indexRoot, null);

        // delete tree metadata
        Relationship metadataNodeRelationship = getRootNode(tx).getSingleRelationship(RTreeRelationshipTypes.RTREE_METADATA, Direction.OUTGOING);
        Node metadataNode = metadataNodeRelationship.getEndNode();
        metadataNodeRelationship.delete();
        metadataNode.delete();


        countSaved = false;
        totalGeometryCount = 0;
        nodeGeometryReader.clear();
    }

    public synchronized void clear(final Listener monitor, Transaction tx) {
        removeAll(false, new NullListener(), tx);
        initIndexRoot();
        initIndexMetadata();
    }


    public Envelope getBoundingBox() {
        try (Transaction tx = database.beginTx()) {
            Envelope result = getIndexNodeEnvelope(getIndexRoot(tx), tx);
            return result;
        }
    }


    public int count() {
        saveCount();
        return totalGeometryCount;
    }


    public boolean isEmpty(Transaction tx) {
        Node indexRoot = getIndexRoot(tx);
        return !indexRoot.hasProperty(INDEX_PROP_BBOX);
    }


    public boolean isNodeIndexed(Long geomNodeId, Transaction tx) {
        Node geomNode = tx.getNodeById(geomNodeId);
        // be sure geomNode is inside this RTree
        return geomNode != null && isGeometryNodeIndexed(geomNode)
                && isIndexNodeInThisIndex(findLeafContainingGeometryNode(geomNode), tx);
    }

    public void warmUp() {
        try (Transaction tx = database.beginTx()) {
            visit(new WarmUpVisitor(), getIndexRoot(tx), tx);
        }
    }


    public List<Node> getAllIndexedNodes(Transaction tx) {
        Node rtreeNode = getIndexRoot(tx);
        Deque<Node> stack = new ArrayDeque<>();//辅助遍历的栈
        List<Node> list = new LinkedList<>();
        stack.push(rtreeNode);
        while (!stack.isEmpty()) {
            rtreeNode = stack.pop();
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
                    list.add(objNode);
                }
            }
        }
        return list;
    }


    private void visit(SpatialIndexVisitor visitor, Node indexNode, Transaction tx) {
        Deque<Node> stack = new ArrayDeque<>();//辅助遍历的栈
        stack.push(indexNode);

        while (!stack.isEmpty()) {
            indexNode = stack.pop();
            if (!visitor.needsToVisit(getIndexNodeEnvelope(indexNode, tx))) {
                continue;
            }
            if (indexNode.hasRelationship(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD)) {
                // Node is not a leaf
                for (Relationship rel : indexNode.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD)) {
                    Node child = rel.getEndNode();
                    // collect children results
                    stack.push(child);
                }
            } else if (indexNode.hasRelationship(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_REFERENCE)) {
                // Node is a leaf
                for (Relationship rel : indexNode.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_REFERENCE)) {
                    visitor.onIndexReference(rel.getEndNode());
                }
            }

        }


    }

    public Node getIndexRoot(Transaction tx) {
        Node indexRoot = getRootNode(tx).getSingleRelationship(RTreeRelationshipTypes.RTREE_ROOT, Direction.OUTGOING)
                .getEndNode();
        return indexRoot;
    }

    // Private methods

    /***
     * This will get the envelope of the child. The relationshipType acts as as flag to allow the function to
     * know whether the child is a leaf or an index node.
     */
    private Envelope getChildNodeEnvelope(Node child, RelationshipType relType, Transaction tx) {
        if (relType.name().equals(RTreeRelationshipTypes.RTREE_REFERENCE.name())) {
            return getLeafNodeEnvelope(child);
        } else {
            return getIndexNodeEnvelope(child, tx);
        }
    }

    /**
     * The leaf nodes belong to the domain model, and as such need to use
     * the layers domain-specific GeometryEncoder for decoding the envelope.
     *
     * @param geomNode geomNode
     * @return env
     */
    public Envelope getLeafNodeEnvelope(Node geomNode) {
//        throw new UnsupportedOperationException();
        return envelopeDecoder.decodeEnvelope(geomNode);
    }

    /**
     * The index nodes do NOT belong to the domain model, and as such need
     * to use the indexes internal knowledge of the index tree and node
     * structure for decoding the envelope.
     *
     * @param indexNode indexNode
     * @param tx        tx
     * @return env
     */
    public Envelope getIndexNodeEnvelope(Node indexNode, Transaction tx) {
        if (indexNode == null) {
            indexNode = getIndexRoot(tx);
        }
        if (!indexNode.hasProperty(INDEX_PROP_BBOX)) {
            // this is ok after an index node split
            return null;
        }

        double[] bbox = (double[]) indexNode.getProperty(INDEX_PROP_BBOX);
        // Envelope parameters: xmin, xmax, ymin, ymax
        return new Envelope(bbox[0], bbox[2], bbox[1], bbox[3]);
    }

    private void visitInTx(SpatialIndexVisitor visitor, Long indexNodeId, Transaction tx) {
        Deque<Long> stack = new ArrayDeque<>();//辅助遍历的栈
        stack.push(indexNodeId);
        while (!stack.isEmpty()) {
            indexNodeId = stack.pop();
            Node indexNode = tx.getNodeById(indexNodeId);
            if (!visitor.needsToVisit(getIndexNodeEnvelope(indexNode, tx))) {
                continue;
            }

            if (indexNode.hasRelationship(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD)) {
                // Node is not a leaf
                for (Relationship rel : indexNode.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD)) {
                    stack.push(rel.getEndNode().getId());
                }
            } else if (indexNode.hasRelationship(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_REFERENCE)) {
                for (Relationship rel : indexNode.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_REFERENCE)) {
                    visitor.onIndexReference(rel.getEndNode());
                }
            }
        }

    }

    private void initIndexMetadata() {
        try (Transaction tx = database.beginTx()) {
            Node layerNode = getRootNode(tx);
            Node metadataNode;
            if (layerNode.hasRelationship(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_METADATA)) {
                // metadata already present
                metadataNode = getMetadataNode(tx);
                metadataNode = layerNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_METADATA, Direction.OUTGOING).getEndNode();

                maxNodeReferences = (Integer) metadataNode.getProperty("maxNodeReferences");
            } else {
                // metadata initialization
                metadataNode = tx.createNode();
                layerNode.createRelationshipTo(metadataNode, RTreeRelationshipTypes.RTREE_METADATA);

                metadataNode.setProperty("maxNodeReferences", maxNodeReferences);
            }
            tx.commit();
        }
        saveCount();
    }

    private void initIndexRoot() {
        try (Transaction tx = database.beginTx()) {
            Node layerNode = getRootNode(tx);
            if (!layerNode.hasRelationship(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_ROOT)) {
                // index initialization
                Node root = tx.createNode();
                layerNode.createRelationshipTo(root, RTreeRelationshipTypes.RTREE_ROOT);
            }
            tx.commit();
        }

    }

    private Node getMetadataNode(Transaction tx) {
        return getRootNode(tx).getSingleRelationship(RTreeRelationshipTypes.RTREE_METADATA, Direction.OUTGOING).getEndNode();
    }

    /**
     * Save the geometry count to the database if it has not been saved yet.
     * However, if the count is zero, first do an exhaustive search of the
     * tree and count everything before saving it.
     */
    private void saveCount() {

        if (totalGeometryCount == 0) {
            SpatialIndexRecordCounter counter = new SpatialIndexRecordCounter();
            try (Transaction tx = database.beginTx()) {
                visit(counter, getIndexRoot(tx), tx);
                totalGeometryCount = counter.getResult();
                Node metadataNode = getMetadataNode(tx);
                int savedGeometryCount = (int) metadataNode.getProperty("totalGeometryCount", 0);
                countSaved = savedGeometryCount == totalGeometryCount;
            }

        }

        if (!countSaved) {
            try (Transaction tx = database.beginTx()) {
                getMetadataNode(tx).setProperty("totalGeometryCount", totalGeometryCount);
                countSaved = true;
                tx.commit();
            }
        }
    }

    private boolean nodeIsLeaf(Node node) {
        return !node.hasRelationship(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD);
    }

    private Node chooseSubTree(Node parentIndexNode, Node geomRootNode, Transaction tx) {
        // children that can contain the new geometry
        List<Node> indexNodes = new ArrayList<>();

        // pick the child that contains the new geometry bounding box
        Iterable<Relationship> relationships = parentIndexNode.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD);
        for (Relationship relation : relationships) {
            Node indexNode = relation.getEndNode();
            if (getIndexNodeEnvelope(indexNode, tx).contains(getLeafNodeEnvelope(geomRootNode))) {
                indexNodes.add(indexNode);
            }
        }

        if (indexNodes.size() > 1) {
            return chooseIndexNodeWithSmallestArea(indexNodes, tx);
        } else if (indexNodes.size() == 1) {
            return indexNodes.get(0);
        }

        // pick the child that needs the minimum enlargement to include the new geometry
        double minimumEnlargement = Double.POSITIVE_INFINITY;
        relationships = parentIndexNode.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD);
        for (Relationship relation : relationships) {
            Node indexNode = relation.getEndNode();
            double enlargementNeeded = getAreaEnlargement(indexNode, geomRootNode, tx);

            if (enlargementNeeded < minimumEnlargement) {
                indexNodes.clear();
                indexNodes.add(indexNode);
                minimumEnlargement = enlargementNeeded;
            } else if (enlargementNeeded == minimumEnlargement) {
                indexNodes.add(indexNode);
            }
        }

        if (indexNodes.size() > 1) {
            return chooseIndexNodeWithSmallestArea(indexNodes, tx);
        } else if (indexNodes.size() == 1) {
            return indexNodes.get(0);
        } else {
            // this shouldn't happen
            throw new RuntimeException("No IndexNode found for new geometry");
        }
    }

    private double getAreaEnlargement(Node indexNode, Node geomRootNode, Transaction tx) {
        Envelope before = getIndexNodeEnvelope(indexNode, tx);

        Envelope after = getLeafNodeEnvelope(geomRootNode);
        after.expandToInclude(before);

        return getArea(after) - getArea(before);
    }

    private Node chooseIndexNodeWithSmallestArea(List<Node> indexNodes, Transaction tx) {
        Node result = null;
        double smallestArea = -1;

        for (Node indexNode : indexNodes) {
            double area = getArea(getIndexNodeEnvelope(indexNode, tx));
            if (result == null || area < smallestArea) {
                result = indexNode;
                smallestArea = area;
            }
        }

        return result;
    }

    private int countChildren(Node indexNode, RelationshipType relationshipType) {
        int counter = 0;
        for (Relationship ignored : indexNode.getRelationships(Direction.OUTGOING, relationshipType)) {
            counter++;
        }
        return counter;
    }

    /**
     * @return is enlargement needed?
     */
    private boolean insertInLeaf(Node indexNode, Node geomRootNode, Transaction tx) {
        return addChild(indexNode, RTreeRelationshipTypes.RTREE_REFERENCE, geomRootNode, tx);
    }

    private void splitAndAdjustPathBoundingBox(Node indexNode, Transaction tx) {
        // create a new node and distribute the entries
        Node newIndexNode = splitMode.equals(GREENES_SPLIT) ? greenesSplit(indexNode, tx) : quadraticSplit(indexNode, tx);
        Node parent = getIndexNodeParent(indexNode);
//        System.out.println("spitIndex " + newIndexNode.getId());
//        System.out.println("parent " + parent.getId());
        if (parent == null) {
            // if indexNode is the root
            createNewRoot(indexNode, newIndexNode, tx);
        } else {
            expandParentBoundingBoxAfterNewChild(parent, (double[]) indexNode.getProperty(INDEX_PROP_BBOX));

            addChild(parent, RTreeRelationshipTypes.RTREE_CHILD, newIndexNode, tx);

            if (countChildren(parent, RTreeRelationshipTypes.RTREE_CHILD) > maxNodeReferences) {
                splitAndAdjustPathBoundingBox(parent, tx);
            } else {
                adjustPathBoundingBox(parent, tx);
            }
        }
        monitor.addSplit(newIndexNode);
    }

    private Node quadraticSplit(Node indexNode, Transaction tx) {
        if (nodeIsLeaf(indexNode)) {
            return quadraticSplit(indexNode, RTreeRelationshipTypes.RTREE_REFERENCE, tx);
        } else {
            return quadraticSplit(indexNode, RTreeRelationshipTypes.RTREE_CHILD, tx);
        }
    }

    private Node greenesSplit(Node indexNode, Transaction tx) {
        if (nodeIsLeaf(indexNode)) {
            return greenesSplit(indexNode, RTreeRelationshipTypes.RTREE_REFERENCE, tx);
        } else {
            return greenesSplit(indexNode, RTreeRelationshipTypes.RTREE_CHILD, tx);
        }
    }

    private NodeWithEnvelope[] mostDistantByDeadSpace(List<NodeWithEnvelope> entries) {
        NodeWithEnvelope seed1 = entries.get(0);
        NodeWithEnvelope seed2 = entries.get(0);
        double worst = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < entries.size(); ++i) {
            NodeWithEnvelope e = entries.get(i);
            for (int j = i + 1; j < entries.size(); ++j) {
                NodeWithEnvelope e1 = entries.get(j);
                double deadSpace = e.envelope.separation(e1.envelope);
                if (deadSpace > worst) {
                    worst = deadSpace;
                    seed1 = e;
                    seed2 = e1;
                }
            }
        }
        return new NodeWithEnvelope[]{seed1, seed2};
    }

    private int findLongestDimension(List<NodeWithEnvelope> entries) {
        if (entries.size() > 0) {
            Envelope env = new Envelope(entries.get(0).envelope);
            for (NodeWithEnvelope entry : entries) {
                env.expandToInclude(entry.envelope);
            }
            int longestDimension = 0;
            double maxWidth = Double.NEGATIVE_INFINITY;
            for (int i = 0; i < env.getDimension(); i++) {
                double width = env.getWidth(i);
                if (width > maxWidth) {
                    maxWidth = width;
                    longestDimension = i;
                }
            }
            return longestDimension;
        } else {
            return 0;
        }
    }

    private List<NodeWithEnvelope> extractChildNodesWithEnvelopes(Node indexNode, RelationshipType relationshipType, Transaction tx) {
        List<NodeWithEnvelope> entries = new ArrayList<>();

        Iterable<Relationship> relationships = indexNode.getRelationships(Direction.OUTGOING, relationshipType);
        for (Relationship relationship : relationships) {
            Node node = relationship.getEndNode();
            entries.add(new NodeWithEnvelope(node, getChildNodeEnvelope(node, relationshipType, tx)));
            relationship.delete();
        }
        return entries;
    }

    private Node greenesSplit(Node indexNode, RelationshipType relationshipType, Transaction tx) {
        // Disconnect all current children from the index and return them with their envelopes
        List<NodeWithEnvelope> entries = extractChildNodesWithEnvelopes(indexNode, relationshipType, tx);

        // We want to split by the longest dimension to avoid degrading into extremely thin envelopes
        int longestDimension = findLongestDimension(entries);

        // Sort the entries by the longest dimension and then create envelopes around left and right halves
        entries.sort(new SingleDimensionNodeEnvelopeComparator(longestDimension));
        int splitAt = entries.size() / 2;
        List<NodeWithEnvelope> left = entries.subList(0, splitAt);
        List<NodeWithEnvelope> right = entries.subList(splitAt, entries.size());

        return reconnectTwoChildGroups(indexNode, left, right, relationshipType, tx);
    }

    private static class SingleDimensionNodeEnvelopeComparator implements Comparator<NodeWithEnvelope> {
        private final int dimension;

        public SingleDimensionNodeEnvelopeComparator(int dimension) {
            this.dimension = dimension;
        }


        public int compare(NodeWithEnvelope o1, NodeWithEnvelope o2) {
            double length = o2.envelope.centre(dimension) - o1.envelope.centre(dimension);
            if (length < 0.0) return -1;
            else if (length > 0.0) return 1;
            else return 0;
        }
    }

    private Node quadraticSplit(Node indexNode, RelationshipType relationshipType, Transaction tx) {
        // Disconnect all current children from the index and return them with their envelopes
        List<NodeWithEnvelope> entries = extractChildNodesWithEnvelopes(indexNode, relationshipType, tx);

        // pick two seed entries such that the dead space is maximal
        NodeWithEnvelope[] seeds = mostDistantByDeadSpace(entries);

        List<NodeWithEnvelope> group1 = new ArrayList<>();
        group1.add(seeds[0]);
        Envelope group1envelope = seeds[0].envelope;

        List<NodeWithEnvelope> group2 = new ArrayList<>();
        group2.add(seeds[1]);
        Envelope group2envelope = seeds[1].envelope;

        entries.remove(seeds[0]);
        entries.remove(seeds[1]);
        while (entries.size() > 0) {
            // compute the cost of inserting each entry
            List<NodeWithEnvelope> bestGroup = null;
            Envelope bestGroupEnvelope = null;
            NodeWithEnvelope bestEntry = null;
            double expansionMin = Double.POSITIVE_INFINITY;
            for (NodeWithEnvelope e : entries) {
                double expansion1 = getArea(createEnvelope(e.envelope, group1envelope)) - getArea(group1envelope);
                double expansion2 = getArea(createEnvelope(e.envelope, group2envelope)) - getArea(group2envelope);

                if (expansion1 < expansion2 && expansion1 < expansionMin) {
                    bestGroup = group1;
                    bestGroupEnvelope = group1envelope;
                    bestEntry = e;
                    expansionMin = expansion1;
                } else if (expansion2 < expansion1 && expansion2 < expansionMin) {
                    bestGroup = group2;
                    bestGroupEnvelope = group2envelope;
                    bestEntry = e;
                    expansionMin = expansion2;
                } else if (expansion1 == expansion2 && expansion1 < expansionMin) {
                    // in case of equality choose the group with the smallest area
                    if (getArea(group1envelope) < getArea(group2envelope)) {
                        bestGroup = group1;
                        bestGroupEnvelope = group1envelope;
                    } else {
                        bestGroup = group2;
                        bestGroupEnvelope = group2envelope;
                    }
                    bestEntry = e;
                    expansionMin = expansion1;
                }
            }

            if (bestEntry == null) {
                throw new RuntimeException("Should not be possible to fail to find a best entry during quadratic split");
            } else {
                // insert the best candidate entry in the best group
                bestGroup.add(bestEntry);
                bestGroupEnvelope.expandToInclude(bestEntry.envelope);

                entries.remove(bestEntry);
            }
        }

        return reconnectTwoChildGroups(indexNode, group1, group2, relationshipType, tx);
    }

    private Node reconnectTwoChildGroups(Node indexNode, List<NodeWithEnvelope> group1, List<NodeWithEnvelope> group2, RelationshipType relationshipType, Transaction tx) {
        // reset bounding box and add new children
        indexNode.removeProperty(INDEX_PROP_BBOX);
        for (NodeWithEnvelope entry : group1) {
            addChild(indexNode, relationshipType, entry.node, tx);
        }

        // create new node from split
        Node newIndexNode = tx.createNode();
        for (NodeWithEnvelope entry : group2) {
            addChild(newIndexNode, relationshipType, entry.node, tx);
        }

        return newIndexNode;
    }

    private void createNewRoot(Node oldRoot, Node newIndexNode, Transaction tx) {
        Node newRoot = tx.createNode();
        addChild(newRoot, RTreeRelationshipTypes.RTREE_CHILD, oldRoot, tx);
        addChild(newRoot, RTreeRelationshipTypes.RTREE_CHILD, newIndexNode, tx);

        Node layerNode = getRootNode(tx);
        layerNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_ROOT, Direction.OUTGOING).delete();
        layerNode.createRelationshipTo(newRoot, RTreeRelationshipTypes.RTREE_ROOT);
    }

    private boolean addChild(Node parent, RelationshipType type, Node newChild, Transaction tx) {
        Envelope childEnvelope = getChildNodeEnvelope(newChild, type, tx);
        double[] childBBox = new double[]{
                childEnvelope.getMinX(), childEnvelope.getMinY(),
                childEnvelope.getMaxX(), childEnvelope.getMaxY()};
        parent.createRelationshipTo(newChild, type);
        return expandParentBoundingBoxAfterNewChild(parent, childBBox);
    }

    private void adjustPathBoundingBox(Node node, Transaction tx) {
        Deque<Node> stack = new ArrayDeque<>();
        stack.push(node);
        do {
            node = stack.pop();
            Node parent = getIndexNodeParent(node);
            if (parent != null) {
                if (adjustParentBoundingBox(parent, RTreeRelationshipTypes.RTREE_CHILD, tx)) {
                    // entry has been modified: adjust the path for the parent
                    stack.push(parent);
                }
            }
        } while (!stack.isEmpty());
    }

    /**
     * Fix an IndexNode bounding box after a child has been added or removed removed. Return true if something was
     * changed so that parents can also be adjusted.
     */
    private boolean adjustParentBoundingBox(Node indexNode, RelationshipType relationshipType, Transaction tx) {
        double[] old = null;
        if (indexNode.hasProperty(INDEX_PROP_BBOX)) {
            old = (double[]) indexNode.getProperty(INDEX_PROP_BBOX);
        }

        Envelope bbox = null;

        for (Relationship relationship : indexNode.getRelationships(Direction.OUTGOING, relationshipType)) {
            Node childNode = relationship.getEndNode();

            if (bbox == null) {
                bbox = new Envelope(getChildNodeEnvelope(childNode, relationshipType, tx));
            } else {
                bbox.expandToInclude(getChildNodeEnvelope(childNode, relationshipType, tx));
            }
        }

        if (bbox == null) {
            // this could happen in an empty tree
            bbox = new Envelope(0, 0, 0, 0);
        }

        if (old == null || old.length != 4
                || bbox.getMinX() != old[0]
                || bbox.getMinY() != old[1]
                || bbox.getMaxX() != old[2]
                || bbox.getMaxY() != old[3]) {
            setIndexNodeEnvelope(indexNode, bbox);
            return true;
        } else {
            return false;
        }
    }

    protected void setIndexNodeEnvelope(Node indexNode, Envelope bbox) {
        indexNode.setProperty(INDEX_PROP_BBOX, new double[]{bbox.getMinX(), bbox.getMinY(), bbox.getMaxX(), bbox.getMaxY()});
    }

    /**
     * Adjust IndexNode bounding box according to the new child inserted
     *
     * @param parent    IndexNode
     * @param childBBox geomNode inserted
     * @return is bbox changed?
     */
    protected boolean expandParentBoundingBoxAfterNewChild(Node parent, double[] childBBox) {
        if (!parent.hasProperty(INDEX_PROP_BBOX)) {
            parent.setProperty(INDEX_PROP_BBOX, new double[]{childBBox[0], childBBox[1], childBBox[2], childBBox[3]});
            return true;
        }

        double[] parentBBox = (double[]) parent.getProperty(INDEX_PROP_BBOX);

        boolean valueChanged = setMin(parentBBox, childBBox, 0);
        valueChanged = setMin(parentBBox, childBBox, 1) || valueChanged;
        valueChanged = setMax(parentBBox, childBBox, 2) || valueChanged;
        valueChanged = setMax(parentBBox, childBBox, 3) || valueChanged;

        if (valueChanged) {
            parent.setProperty(INDEX_PROP_BBOX, parentBBox);
        }

        return valueChanged;
    }

    private boolean setMin(double[] parent, double[] child, int index) {
        if (parent[index] > child[index]) {
            parent[index] = child[index];
            return true;
        } else {
            return false;
        }
    }

    private boolean setMax(double[] parent, double[] child, int index) {
        if (parent[index] < child[index]) {
            parent[index] = child[index];
            return true;
        } else {
            return false;
        }
    }

    private Node getIndexNodeParent(Node indexNode) {
        Relationship relationship = indexNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_CHILD, Direction.INCOMING);
        if (relationship == null) {
            return null;
        } else {
            return relationship.getStartNode();
        }
    }

    private double getArea(Envelope e) {
        return e.getArea();
    }

    private void deleteTreeBelow(Node rootNode) {
        for (Relationship relationship : rootNode.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD)) {
            deleteRecursivelySubtree(relationship.getEndNode(), relationship);
        }
    }

    private void deleteRecursivelySubtree(Node node, Relationship incoming) {
        for (Relationship relationship : node.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD)) {
            deleteRecursivelySubtree(relationship.getEndNode(), relationship);
        }
        if (incoming != null) {
            incoming.delete();
        }
        for (Relationship rel : node.getRelationships()) {
            System.out.println("Unexpected relationship found on " + node + ": " + rel.toString());
            rel.delete();
        }
        node.delete();
    }

    protected boolean isGeometryNodeIndexed(Node geomNode) {
        return geomNode.hasRelationship(Direction.INCOMING, RTreeRelationshipTypes.RTREE_REFERENCE);
    }

    protected Node findLeafContainingGeometryNode(Node geomNode) {
        return geomNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_REFERENCE, Direction.INCOMING).getStartNode();
    }

    protected boolean isIndexNodeInThisIndex(Node indexNode, Transaction tx) {
        Node child = indexNode;
        Node root = null;
        while (root == null) {
            Node parent = getIndexNodeParent(child);
            if (parent == null) {
                root = child;
            } else {
                child = parent;
            }
        }
        return root.getId() == getIndexRoot(tx).getId();
    }

    private void deleteNode(Node node) {
        for (Relationship r : node.getRelationships()) {
            r.delete();
        }
        node.delete();
    }

    private Node getRootNode(Transaction tx) {
        Node root = tx.getNodeById(rootNodeId);
        if (null == root) {
            throw new RuntimeException("root node is null");
        }
        return root;
    }

    /**
     * Create a bounding box encompassing the two bounding boxes passed in.
     */
    private static Envelope createEnvelope(Envelope e, Envelope e1) {
        Envelope result = new Envelope(e);
        result.expandToInclude(e1);
        return result;
    }

    // Attributes
    public GraphDatabaseService getDatabase() {
        return database;
    }

    private final GraphDatabaseService database;

    private final long rootNodeId;
    private final EnvelopeDecoder envelopeDecoder;
    private int maxNodeReferences;
    private String splitMode = GREENES_SPLIT;
    private boolean shouldMergeTrees = false;

    private int totalGeometryCount = 0;
    private boolean countSaved = false;

    // Private classes
    private class WarmUpVisitor implements SpatialIndexVisitor {

        public boolean needsToVisit(Envelope indexNodeEnvelope) {
            return true;
        }

        public void onIndexReference(Node geomNode) {
        }
    }

    private class IndexNodeAreaComparator implements Comparator<NodeWithEnvelope> {


        public int compare(NodeWithEnvelope o1, NodeWithEnvelope o2) {
            return Double.compare(o1.envelope.getArea(), o2.envelope.getArea());
        }
    }

    public String getGeometryFieldName() {
        return geometryFieldName;
    }

    public String getIndexName() {
        return indexName;
    }
}
