package org.wowtools.neo4j.rtree.internal.edit;

import org.neo4j.graphdb.Transaction;

/**
 * @author liuyu
 * @date 2024/10/30
 */
public interface TxBuilder {
    Transaction beginTx();
}
