package org.wowtools.neo4j.rtree.internal.edit;

/*
 * #%L
 * Conversant RTree
 * ~~
 * Conversantmedia.com © 2016, Conversant, Inc. Conversant® is a trademark of Conversant, Inc.
 * ~~
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.wowtools.neo4j.rtree.pojo.RectNd;
import org.wowtools.neo4j.rtree.internal.define.Labels;
import org.wowtools.neo4j.rtree.util.TxCell;

import java.util.function.Consumer;

/**
 * RTree node that contains leaf nodes
 * <p>
 * Created by jcairns on 4/30/15.
 */
public final class NodeOfBranch implements Node {

    private final RectBuilder builder;

    private final int mMax;

    private final int mMin;

    private final CacheNode cacheNode;

    private final TxCell txCell;

    public static NodeOfBranch getFromNeo(RectBuilder builder, long neoId, TxCell txCell) {
        org.neo4j.graphdb.Node cacheNode = txCell.getTx().getNodeById(neoId);
        return new NodeOfBranch(builder, cacheNode, txCell);
    }

    private NodeOfBranch(RectBuilder builder, org.neo4j.graphdb.Node node, TxCell txCell) {
        this.builder = builder;
        this.cacheNode = txCell.getNode(node.getId());
        this.mMax = (int) cacheNode.getProperty("mMax");
        this.mMin = (int) cacheNode.getProperty("mMin");
        if (null == cacheNode) {
            throw new RuntimeException("逻辑错误");
        }
        this.txCell = txCell;
    }

    NodeOfBranch(final RectBuilder builder, final int mMin, final int mMax, TxCell txCell) {
        cacheNode = txCell.newNode(Labels.RTREE_BRANCH);
        cacheNode.setProperty("mMax", mMax);
        cacheNode.setProperty("mMin", mMin);
        cacheNode.setProperty("size", 0);

        this.txCell = txCell;
        this.mMin = mMin;
        this.mMax = mMax;
        this.builder = builder;
    }

    /**
     * Add a new node to this branch's list of children
     *
     * @param n node to be added (can be leaf or branch)
     * @return position of the added node
     */
    protected int addChild(final Node n) {
        return cacheNode.addChild(n);
    }

    @Override
    public boolean isLeaf() {
        return false;
    }

    @Override
    public HyperRect getBound() {
        return cacheNode.getMbr();
    }

    /**
     * Adds a data entry to one of the child nodes of this branch
     *
     * @param t data entry to add
     * @return Node that the entry was added to
     */
    @Override
    public Node add(final RectNd t) {
        final HyperRect tRect = builder.getBBox(t);
        int size = (int) cacheNode.getProperty("size");
        Node[] child = cacheNode.getChildren();
        if (size < mMin) {
            for (int i = 0; i < size; i++) {
                if (child[i].getBound().contains(tRect)) {
                    cacheNode.setChildAtI(i, child[i].add(t));
                    cacheNode.setMbr((RectNd) child[i].getBound());
                    return this;
                }
            }
            // no overlapping node - grow
            final Node nextLeaf = NodeOfLeaf.create(builder, mMin, mMax, txCell);
            nextLeaf.add(t);
            final int nextChild = addChild(nextLeaf);
            cacheNode.setMbr((RectNd) child[nextChild].getBound());

            return this;

        } else {
            final int bestLeaf = chooseLeaf(t, tRect);

            cacheNode.setChildAtI(bestLeaf,child[bestLeaf].add(t));

            RectNd mbr = cacheNode.getMbr();
            HyperRect r = child[bestLeaf].getBound();
            mbr = (RectNd) mbr.getMbr(r);
            cacheNode.setMbr(mbr);

            return this;
        }
    }

    @Override
    public Node remove(final RectNd t) {
        final HyperRect tRect = builder.getBBox(t);
        int size = (int) cacheNode.getProperty("size");
        Node[] child = cacheNode.getChildren();
        for (int i = 0; i < size; i++) {
            if (child[i].getBound().intersects(tRect)) {
                cacheNode.setChildAtI(i, child[i].remove(t));
                if (child[i] == null) {
//                    System.arraycopy(child, i + 1, child, i, size - i - 1);
                    //把所有child的位置上移一位
                    int j = i;
                    int num = size - i - 1;
                    for (int i1 = 0; i1 < num; i1++) {
                        cacheNode.setChildAtI(j, child[j + 1]);
                        j++;
                    }
                    size--;
                    cacheNode.setChildAtI(size, null);

                    if (size > 0) i--;
                }
            }
        }

        if (size == 0) {
            return null;
        } else if (size == 1) {
            // unsplit branch
            return child[0];
        }

        HyperRect mbr = child[0].getBound();
        for (int i = 1; i < size; i++) {
            mbr = mbr.getMbr(child[i].getBound());
        }
        cacheNode.setMbr((RectNd) mbr);

        return this;
    }

    @Override
    public Node update(final RectNd told, final RectNd tnew) {
        final HyperRect tRect = builder.getBBox(told);
        int size = (int) cacheNode.getProperty("size");
        Node[] child = cacheNode.getChildren();
        for (int i = 0; i < size; i++) {
            if (tRect.intersects(child[i].getBound())) {
                cacheNode.setChildAtI(i, child[i].update(told, tnew));
            }
            HyperRect mbr = cacheNode.getMbr();
            if (i == 0) {
                mbr = child[i].getBound();
                cacheNode.setMbr((RectNd) mbr);
            } else {
                mbr = mbr.getMbr(child[i].getBound());
                cacheNode.setMbr((RectNd) mbr);
            }
        }
        return this;
    }

    @Override
    public void search(HyperRect rect, Consumer consumer) {
        int size = (int) cacheNode.getProperty("size");
        Node[] child = cacheNode.getChildren();
        for (int i = 0; i < size; i++) {
            if (rect.intersects(child[i].getBound())) {
                child[i].search(rect, consumer);
            }
        }
    }

    @Override
    public int search(final HyperRect rect, final RectNd[] t, int n) {
        final int tLen = t.length;
        final int n0 = n;
        int size = (int) cacheNode.getProperty("size");
        Node[] child = cacheNode.getChildren();
        for (int i = 0; i < size && n < tLen; i++) {
            if (rect.intersects(child[i].getBound())) {
                n += child[i].search(rect, t, n);
            }
        }
        return n - n0;
    }

    @Override
    public void intersects(HyperRect rect, Consumer consumer) {
        int size = (int) cacheNode.getProperty("size");
        Node[] child = cacheNode.getChildren();
        for (int i = 0; i < size; i++) {
            if (rect.intersects(child[i].getBound())) {
                child[i].intersects(rect, consumer);
            }
        }
    }

    @Override
    public int intersects(final HyperRect rect, final RectNd[] t, int n) {
        final int tLen = t.length;
        final int n0 = n;
        int size = (int) cacheNode.getProperty("size");
        Node[] child = cacheNode.getChildren();
        for (int i = 0; i < size && n < tLen; i++) {
            if (rect.intersects(child[i].getBound())) {
                n += child[i].intersects(rect, t, n);
            }
        }
        return n - n0;
    }

    /**
     * @return number of child nodes
     */
    @Override
    public int size() {
        return (int) cacheNode.getProperty("size");
    }

    @Override
    public int totalSize() {
        int s = 0;
        int size = (int) cacheNode.getProperty("size");
        Node[] child = cacheNode.getChildren();

        for (int i = 0; i < size; i++) {
            s += child[i].totalSize();
        }
        return s;
    }

    private int chooseLeaf(final RectNd t, final HyperRect tRect) {
        int size = (int) cacheNode.getProperty("size");
        Node[] child = cacheNode.getChildren();
        if (size > 0) {
            int bestNode = 0;
            HyperRect childMbr = child[0].getBound().getMbr(tRect);
            double leastEnlargement = childMbr.cost() - (child[0].getBound().cost() + tRect.cost());
            double leastPerimeter = childMbr.perimeter();

            for (int i = 1; i < size; i++) {
                childMbr = child[i].getBound().getMbr(tRect);
                final double nodeEnlargement = childMbr.cost() - (child[i].getBound().cost() + tRect.cost());
                if (nodeEnlargement < leastEnlargement) {
                    leastEnlargement = nodeEnlargement;
                    leastPerimeter = childMbr.perimeter();
                    bestNode = i;
                } else if (RTree.isEqual(nodeEnlargement, leastEnlargement)) {
                    final double childPerimeter = childMbr.perimeter();
                    if (childPerimeter < leastPerimeter) {
                        leastEnlargement = nodeEnlargement;
                        leastPerimeter = childPerimeter;
                        bestNode = i;
                    }
                } // else its not the least

            }
            return bestNode;
        } else {
            final Node n = NodeOfLeaf.create(builder, mMin, mMax, txCell);
            n.add(t);
            cacheNode.setChildAtI(size, n);
            size = size + 1;
            cacheNode.setProperty("size", size);

            HyperRect mbr = cacheNode.getMbr();
            if (mbr == null) {
                mbr = n.getBound();
            } else {
                mbr = mbr.getMbr(n.getBound());
            }
            cacheNode.setMbr((RectNd) mbr);

            return size - 1;
        }
    }

    /**
     * Return child nodes of this branch.
     *
     * @return array of child nodes (leaves or branches)
     */
    public Node[] getChildren() {
        return cacheNode.getChildren();
    }

    @Override
    public void forEach(Consumer consumer) {
        int size = (int) cacheNode.getProperty("size");
        Node[] child = cacheNode.getChildren();
        for (int i = 0; i < size; i++) {
            child[i].forEach(consumer);
        }
    }

    @Override
    public boolean contains(HyperRect rect, RectNd t) {
        int size = (int) cacheNode.getProperty("size");
        Node[] child = cacheNode.getChildren();
        for (int i = 0; i < size; i++) {
            if (rect.intersects(child[i].getBound())) {
                child[i].contains(rect, t);
            }
        }
        return false;
    }

    @Override
    public void collectStats(Stats stats, int depth) {
        int size = (int) cacheNode.getProperty("size");
        Node[] child = cacheNode.getChildren();
        for (int i = 0; i < size; i++) {
            child[i].collectStats(stats, depth + 1);
        }
        stats.countBranchAtDepth(depth);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(128);
        sb.append("BRANCH[");
        sb.append(cacheNode.getMbr());
        sb.append(']');

        return sb.toString();
    }

    @Override
    public Node instrument() {
        int size = (int) cacheNode.getProperty("size");
        Node[] child = cacheNode.getChildren();
        for (int i = 0; i < size; i++) {
            child[i] = child[i].instrument();
        }
        return new NodeOfCounter(this);
    }

    @Override
    public long getNeoNodeId() {
        return cacheNode.getNode().getId();
    }

}
