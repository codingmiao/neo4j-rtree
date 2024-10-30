package org.wowtools.neo4j.rtree.internal.edit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

/**
 * @author liuyu
 * @date 2024/10/30
 */
public final class GraphDbTxBuilder implements TxBuilder {
    private final GraphDatabaseService graphDb;

    public GraphDbTxBuilder(GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    @Override
    public Transaction beginTx() {
        return graphDb.beginTx();
    }
}
