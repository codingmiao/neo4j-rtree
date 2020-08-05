package org.wowtools.neo4j.rtree;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.wowtools.neo4j.rtree.spatial.RTreeIndex;

import java.util.Scanner;

import static org.junit.Assert.*;

public class RTreeIndexManagerTest {

    @Test
    public void testEditIndex() {
        GraphDatabaseService db = Neo4jDbManager.getGraphDb();
        RTreeIndexManager.createIndex(db, "index1", "g", 64,1024);
        System.out.println("新建测试通过");
        try {
            RTreeIndexManager.createIndex(db, "index1", "g", 64,1024);
            throw new RuntimeException("未检出重复");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println("重复校验测试通过");
        }
        RTreeIndexManager.createIndex(db, "index2", "g2", 64,1024);
        RTreeIndex index2 = RTreeIndexManager.getIndex(db, "index2",1024);
        assertEquals("index2", index2.getIndexName());
        System.out.println("get测试通过");

        RTreeIndex index3 = RTreeIndexManager.getOrCreateIndex(db, "index3", "g", 64,1024);
        assertEquals("index3", index3.getIndexName());
        index2 = RTreeIndexManager.getOrCreateIndex(db, "index2", "gxx", 64,1024);
        assertEquals("index2", index2.getIndexName());
        assertEquals("g2", index2.getGeometryFieldName());
        System.out.println("getOrCreateIndex测试通过");

        RTreeIndexManager.dropIndex(db, "index1");
        try {
            RTreeIndexManager.getIndex(db,"index1",1024);
            throw new RuntimeException("未成功删除");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println("删除测试通过");
        }
//        Scanner sin = new Scanner(System.in);
//        sin.next();
    }
}
