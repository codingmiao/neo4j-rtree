# neo4j-rtree
a spatial index for neo4j 4.x.

[README](README.md) | [中文文档](README_zh.md)

## What can it do

create a spatial index
~~~java
RTreeIndex rTreeIndex = RTreeIndexManager.createIndex(db, "index1", "geometry", 64);
~~~


add node(s) to spatial index
~~~java
Transaction tx = db.beginTx();
Node node = tx.createNode(testLabel);//create node
Point geo = wkbReader.read("POINT(10 20)");
byte[] wkb = wkbWriter.write(geo);//to wkb
node.setProperty("geometry", wkb);//set the property of the spatial field
rTreeIndex.add(node,tx);//add to spatial index(if you have many vertices, add as list for efficiency)

~~~

spatial query
~~~java
//Enter a rectangle and query the node covered by the rectangle
double[] bbox = new double[]{3, 1, 8, 9};
try (Transaction tx = db.beginTx()) {
    RtreeQuery.queryByBbox(tx, rTreeIndex, bbox, (node, geometry) -> {
        System.out.println(node.getProperty("xxxx"));
    });
}
~~~

~~~java
//Input an geometry, query the node covered by the geometry
//对于狭长的geometry，使用queryByStripGeometryIntersects方法有更高的性能
Geometry inputGeo = new WKTReader().read("POLYGON ((11 24, 22 28, 29 15, 11 24))");
try (Transaction tx = db.beginTx()) {
    RtreeQuery.queryByGeometryIntersects(tx, rTreeIndex, inputGeo, (node, geometry) -> {
        System.out.println(node.getProperty("xxxx"));
    });
}
~~~

nearest neighbor search
~~~java
//Query the 5 nodes closest to point (10.2, 13.2)
try (Transaction tx = db.beginTx()) {
    List<DistanceResult> res = RtreeNearestQuery.queryNearestN(tx, rTreeIndex, 10.2, 13.2, 5, (node, geometry) -> true);
    System.out.println(res);
}

~~~
~~~java
//Query the 5 nodes closest to point (10.2, 13.2) with filter
try (Transaction tx = db.beginTx()) {
    List<DistanceResult> res = RtreeNearestQuery.queryNearestN(tx, rTreeIndex, 10.2, 13.2, 5, (node, geometry) -> geometry.getCoordinate().x<10);
    System.out.println(res);//DistanceResult里包含了node、距离以及geometry，详见测试用例
}
~~~



## install


maven import in your project
```
<dependency>
    <groupId>org.wowtools</groupId>
    <artifactId>neo4j-rtree</artifactId>
    <version>1.2</version>
</dependency>
```
Maven central repository build by jdk11，So if you use jdk8，you must build yourself:

clone & install

```
git clone https://github.com/codingmiao/neo4j-rtree.git
mvn clean install -DskipTests

```


## about this project
In this project source, package org.wowtools.neo4j.rtree.spatial is based on from project "Neo4j Spatial" (https://github.com/neo4j-contrib/spatial)

but "Neo4j Spatial" has not been updated for 16 months. With the release of Neo4j 4.0, "Neo4j Spatial" has become unavailable due to a large number of API rewriting.
Therefore, I extracted the Spatial index part in the Neo4j Spatial and adapted it to Neo4j 4.0.

At the same time, OSM, SHP parsing and other contents in the original project were removed to make the project more streamlined. 
The idea of this project is to simplify and decouple, and you can use tools such as GeoTools to import files such as SHP independently or integrated with Neo4j, instead of being bundled with Neo4J.

package org.wowtools.neo4j.rtree.nearest is based on project PRTree (https://github.com/EngineHub/PRTree)
, the nearest neighbor search based on the branch limit method
