//package org.wowtools.neo4j.rtree;
//
//import org.neo4j.graphdb.*;
//import org.wowtools.neo4j.rtree.internal.define.Labels;
//import org.wowtools.neo4j.rtree.internal.define.PropertyNames;
//import org.wowtools.neo4j.rtree.internal.define.Relationships;
//
//import java.util.ArrayDeque;
//import java.util.ArrayList;
//import java.util.HashSet;
//import java.util.Map;
//
///**
// * 树节点的“垃圾回收”操作，移除掉图库中由于更新而没有被使用的索引节点或字段以节约空间
// *
// * @author liuyu
// * @date 2024/11/1
// */
//public class RtreeGc {
//
//    private final Transaction tx;
//    //在使用中的节点
//    private final HashSet<String> usedNodeIds = new HashSet<>();
//
//    public RtreeGc(Transaction tx) {
//        this.tx = tx;
//    }
//
//    public void gc() {
//        //1、找到所有叶子节点，清理叶子节点上的属性
//        ResourceIterator<Node> nodes = tx.findNodes(Labels.RTREE_LEAF);
//        nodes.forEachRemaining(this::clearLeaf);
//        nodes.close();
//        //2、找到所有叶子节点和非叶子节点，向下找没有子节点的删除，向上遍历找不到root的删除
//
//        //1、找到所有叶子节点，向上遍历，找不到root就删除掉，找到root则检查没有存在的entryDataIdX，删除entryDataIdX entryMaxX并调整mbrMax mbrMin size
//        ResourceIterator<Node> nodes = tx.findNodes(Labels.RTREE_LEAF);
//        nodes.forEachRemaining((node) -> {
//            if (traversalUp(node)) {
//                clearLeaf(node);
//            }
//        });
//        nodes.close();
//        //2、找到所有非叶子节点，向上遍历，找不到root就删除掉
//        nodes = tx.findNodes(Labels.RTREE_BRANCH);
//        nodes.forEachRemaining(this::traversalUp);
//    }
//
//    private void clearLeaf(Node node) {
//        Map<String, Object> properties = node.getAllProperties();
//        int size = (int) properties.get(PropertyNames.size);
//        ArrayList<String> entryDataIds = new ArrayList<>(size);
//        ArrayList<double[]> entryMaxs = new ArrayList<>(size);
//        ArrayList<double[]> entryMins = new ArrayList<>(size);
//        for (int i = 0; i < size; i++) {
//            try {
//                tx.getNodeByElementId(PropertyNames.entryDataId + i);
//                continue;
//            } catch (NotFoundException ignored) {
//            }
//            entryDataIds.add((String) properties.get(PropertyNames.entryDataId + i));
//            entryMaxs.add((double[]) properties.get(PropertyNames.entryMax + i));
//            entryMins.add((double[]) properties.get(PropertyNames.entryMin + i));
//        }
//        if (entryDataIds.isEmpty()) {
//            //没有元素了，删除节点
//            rDelete(node);
//        } else if (entryDataIds.size() < size) {
//            int newSize = entryDataIds.size();
//            //元素少了，重新整理
//            double[] mbrMax = new double[]{Double.MAX_VALUE, Double.MAX_VALUE};
//            double[] mbrMin = new double[]{Double.MIN_VALUE, Double.MIN_VALUE};
//            for (double[] entryMax : entryMaxs) {
//                if (entryMax[0] > mbrMax[0]) {
//                    mbrMax[0] = entryMax[0];
//                }
//                if (entryMax[1] > mbrMax[1]) {
//                    mbrMax[1] = entryMax[1];
//                }
//            }
//            for (double[] entryMin : entryMins) {
//                if (entryMin[0] < mbrMin[0]) {
//                    mbrMin[0] = entryMin[0];
//                }
//                if (entryMin[1] < mbrMin[1]) {
//                    mbrMin[1] = entryMin[1];
//                }
//            }
//            for (int i = 0; i < newSize; i++) {
//                node.setProperty(PropertyNames.entryDataId + i, entryDataIds.get(i));
//                node.setProperty(PropertyNames.entryMin + i, entryMins.get(i));
//                node.setProperty(PropertyNames.entryMax + i, entryMaxs.get(i));
//            }
//            for (int i = newSize; i < size; i++) {
//                node.removeProperty(PropertyNames.entryDataId + i);
//                node.removeProperty(PropertyNames.entryMin + i);
//                node.removeProperty(PropertyNames.entryMax + i);
//            }
//            node.setProperty(PropertyNames.size, newSize);
//        }
//    }
//
//    private void rDelete(Node node) {
//        ResourceIterable<Relationship> rs = node.getRelationships();
//        for (Relationship r : rs) {
//            r.delete();
//        }
//        node.delete();
//        rs.close();
//    }
//
//    private boolean traversalUp(Node node) {
//        HashSet<Node> traversalNodes = new HashSet<>();
//        ArrayDeque<Node> stack = new ArrayDeque<>();
//        stack.push(node);
//        boolean findRoot = false;
//        do {
//            node = stack.pop();
//            String nodeId = node.getElementId();
//            traversalNodes.add(node);
//            //检查节点是否已在usedNodeIds，在则说明已在使用
//            if (usedNodeIds.contains(nodeId)) {
//                findRoot = true;
//                break;
//            }
//
//            //检查是否有关系 RTREE_METADATA_TO_ROOT 有则说明连接到了根节点，已在使用
//            if (node.hasRelationship(Direction.INCOMING, Relationships.RTREE_METADATA_TO_ROOT)) {
//                findRoot = true;
//                break;
//            }
//            //将父节点丢进栈继续
//            Relationship parentNodeRelationship = node.getSingleRelationship(Relationships.RTREE_PARENT_TO_CHILD, Direction.INCOMING);
//            if (null != parentNodeRelationship) {
//                stack.push(parentNodeRelationship.getStartNode());
//            }
//        } while (!stack.isEmpty());
//        if (findRoot) {
//            for (Node traversalNode : traversalNodes) {
//                usedNodeIds.add(traversalNode.getElementId());
//            }
//            return false;
//        } else {
//            for (Node traversalNode : traversalNodes) {
//                traversalNode.delete();
//            }
//            return false;
//        }
//    }
//
//}
