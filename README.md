# neo4j-rtree
一个用于neo4j的空间索引

## 这个项目能干什么

新建空间索引
~~~java
RTreeIndex rTreeIndex = RTreeIndexManager.createIndex(db, "index1", "geometry", 64);
~~~


简洁地为node加入空间索引:
~~~java
Transaction tx = db.beginTx();
Node node = tx.createNode(testLabel);//新建节点
Point geo = wkbReader.read("POINT(10 20)");
byte[] wkb = wkbWriter.write(geo);//转为wkb
node.setProperty("geometry", wkb);//设置空间字段值,必须为wkb格式
rTreeIndex.add(node,tx);//加入索引(效率起见，多个node的话用list add，详见测试用例)

~~~

空间查询
~~~java
//输入一个矩形范围，查询矩形覆盖的节点
double[] bbox = new double[]{3, 1, 8, 9};
try (Transaction tx = db.beginTx()) {
    RtreeQuery.queryByBbox(tx, rTreeIndex, bbox, (node, geometry) -> {
        System.out.println(node.getProperty("xxxx"));
    });
}
~~~

~~~java
//输入一个geometry，查询geometry覆盖的节点
Geometry inputGeo = new WKTReader().read("POLYGON ((11 24, 22 28, 29 15, 11 24))");
try (Transaction tx = db.beginTx()) {
    RtreeQuery.queryByGeometryIntersects(tx, rTreeIndex, inputGeo, (node, geometry) -> {
        System.out.println(node.getProperty("xxxx"));
    });
}
~~~

最邻近搜索
~~~java
//查询距离点(10.2, 13.2)最近的5个node
try (Transaction tx = db.beginTx()) {
    List<DistanceResult> res = RtreeNearestQuery.queryNearestN(tx, rTreeIndex, 10.2, 13.2, 5, (node, geometry) -> true);
    System.out.println(res);
}

~~~
~~~java
//查询满足约束条件且距离点(10.2, 13.2)最近的5个node
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
注意，maven中央库的依赖用jdk11编译，所以如果你的项目使用了jdk8，你需要自己编译一份适合于你的jdk的:

clone & install

```
git clone https://github.com/codingmiao/neo4j-rtree.git
mvn clean install -DskipTests

```


## 关于本项目
本项目源码中的org.wowtools.neo4j.rtree.spatial包取自开源项目Neo4j Spatial(https://github.com/neo4j-contrib/spatial)

截至本项目首次push起，Neo4j Spatial已经有16个月未更新，Neo4j 4.0的发布，大量的api重写导致Neo4j Spatial已不可用，
所以我抽取了Neo4j Spatial中的空间索引部分并适配至Neo4j 4.0。

同时，去掉了原项目中的osm、shp解析等内容，旨在使项目更精简，这个项目的理念是精简与解耦，你可以独立或集成使用geotools等工具来实现shp等文件的导入，而非与neo4j捆绑在一起。

