# neo4j-rtree
a spatial index for neo4j 4.x.

[README](README.md) | [中文文档](README_zh.md)

## What can it do

### create a spatial index
~~~java
//5个参数依次是:  neo4j的GraphDatabaseService实例    索引名(唯一) 空间属性名   rtree最大子节点数 最大缓存geometry对象数
RTreeIndex rTreeIndex = RTreeIndexManager.createIndex(db, "index1", "geometry", 64, 1024);
~~~


### add node(s) to spatial index
~~~java
Transaction tx = db.beginTx();
Node node = tx.createNode(testLabel);//create node
Point geo = wkbReader.read("POINT(10 20)");
byte[] wkb = wkbWriter.write(geo);//to wkb
node.setProperty("geometry", wkb);//set the property of the spatial field
rTreeIndex.add(node,tx);//add to spatial index(if you have many vertices, add as list for efficiency)

~~~

### spatial query
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

### nearest neighbor search
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
### Faster spatial analysis of big geometry
As for very fine geometry, because it contains too many points, the performance of spatial relationship calculation by using JTS's ``intersects`` function directly will be poor. At this point, it can be considered to use the Tool BigShapeManager to cut it open and establish index before analysis:
~~~java
String wkt = "...";//一个很多点构成的多边形
Geometry geometry = wktReader.read(wkt);
String indexId = "123";//声明一个id来构建索引，注意id唯一性
BigShapeManager.build(graphDb, indexId, geometry, 10, 10);//切割多边形为10行10列并构建索引
BigShape bigShape = BigShapeManager.get(graphDb, indexId);//获取到BigShape对象
Geometry s1 = new WKTReader().read("POINT (866.8184900283813 132.99309968948364)");
System.out.println(bigShape.intersects(tx, s1));//利用BigShape来做空间关系计算
~~~
The actual measurement performance of BigShape is up to 100 times faster than that of direct JTS for fine and complex geometry (simple geometry does not need BigShape cutting, otherwise the performance may decline)


## install
The latest version is `1.4.2.RELEASE`

maven import in your project
```
            <dependency>
                <groupId>org.wowtools</groupId>
                <artifactId>neo4j-rtree</artifactId>
                <version>${neo4j-rtree-version}</version>
            </dependency>
```
If you already use another version of Neo4j (for example, the Enterprise version) in your project, add exclusions tag:
```
            <dependency>
                <groupId>org.wowtools</groupId>
                <artifactId>neo4j-rtree</artifactId>
                <version>${neo4j-rtree-version}</version>
                <exclusions>
                    <exclusion>
                        <groupId>org.neo4j</groupId>
                        <artifactId>neo4j-common</artifactId>
                    </exclusion>
                    <exclusion>
                        <groupId>org.neo4j</groupId>
                        <artifactId>neo4j</artifactId>
                    </exclusion>
                </exclusions>
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
