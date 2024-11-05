package localhosttest;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.wowtools.neo4j.rtree.Neo4jDbManager;
import org.wowtools.neo4j.rtree.util.CustomProceduresRegister;

/**
 * @author liuyu
 * @date 2024/11/4
 */
public class T1 {
    public static void main(String[] args) {
        Neo4jDbManager neo4jDbManager = new Neo4jDbManager();
        try (Transaction tx = neo4jDbManager.getGraphDb().beginTx()){
            Node node1 = tx.createNode(Label.label("hello"));
            node1.setProperty("sid","1");
            Node node2 = tx.createNode(Label.label("hello"));
            node2.setProperty("sid","2");
            node1.createRelationshipTo(node2, RelationshipType.withName("link"));
            tx.commit();
        }
        CustomProceduresRegister.registerProcedures(neo4jDbManager.getGraphDb(), DemoProcedures.class);
        System.out.println("-----------");
    }
}
