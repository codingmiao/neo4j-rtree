# neo4j-rtree

一个基于neo4j构建的n维度空间索引

github: https://github.com/codingmiao/neo4j-rtree

gitee: https://gitee.com/wowtools/neo4j-rtree


[中文文档](README.md) | [README](README_en.md)

## 简介

rtree，是一个计算n维空间内对象的位置关系、距离关系的索引。 很多前辈开源了各类rtree的在内存中的实现，但是，在大数据量的场景下，完全构建在内存中的rtree存在内存占用高，初始化时间长等问题。
因此，本项目参考[conversant/rtree](https://github.com/conversant/rtree) 的实现思路，将rtree构建在图数据库 [neo4j](https://github.com/neo4j/neo4j)
中， 形成了一个内存可控、可持久化的rtree索引项目。

(注意: 旧版本 [v1.x](https://github.com/codingmiao/neo4j-rtree/tree/v1.x)
版本基于 [neo4j spatial](https://github.com/neo4j-contrib/spatial) 构建，存在递归过多容易栈内存溢出、只能用于2维空间等问题，已不再进行功能性更新)

## 使用示例

### 索引数据的的增删改

~~~java
    try(RtreeEditor rtreeEditor=RtreeEditor.create(db,2000,indexName)){
        //构建一个RectNd对象，描述n维对象的最小外接矩形
        RectNd rect2d = new RectNd(new PointNd(new double[]{xmin, ymin}), new PointNd(new double[]{xmax, ymax}));
        //add
        rtreeEditor.add(rect2d);
        //remove、update方法类似
    }
~~~

完整示例请参见 [测试用例](https://github.com/codingmiao/neo4j-rtree/blob/v2.x/src/test/java/org/wowtools/neo4j/rtree/RtreeEditorTest.java)


### 相交关系查询
查询索引中的对象是否与输入的n维矩形相交
~~~java
//构造输入矩形
PointNd p0 = new PointNd(new double[]{x0, y0});
PointNd p1 = new PointNd(new double[]{x1, y1});
RectNd inputRange = new RectNd(p0, p1);
try (Transaction tx = db.beginTx()) {
    //构建一个RtreeIntersectsSearcher对象
    RtreeIntersectsSearcher searcher = RtreeIntersectsSearcher.get(tx, indexName);
    searcher.intersects(inputRange, tx, (dataNodeId)->{
        //打印查询到的nodeId，也可以拿着id在neo4j中查询node更详细的信息
        System.out.println(dataNodeId);
        return false;
    });
}
~~~
完整示例请参见 [测试用例](https://github.com/codingmiao/neo4j-rtree/blob/v2.x/src/test/java/org/wowtools/neo4j/rtree/RtreeEditorTest.java)



### 最邻近搜索

~~~java
    //求距离点(x,y)距离最近的5个点点
    double x = 0.5, y = 0.5;
    int hitNum = 5；
    try (Transaction tx = db.beginTx()) {
        //构建一个RtreeNearestSearcher对象
        RtreeNearestSearcher searcher = RtreeNearestSearcher.get(tx, indexName);
        //构建查询条件NearestNeighbour，包括点PointNd、最大返回条数、距离计算公式
        PointNd pt = new PointNd(new double[]{x, y});
        NearestNeighbour nearestNeighbour = new NearestNeighbour(hitNum, pt) {
            @Override
            public DistanceResult createDistanceResult(PointNd pointNd, long dataNodeId) {
                RectNd rectNd = ...;//根据dataNodeId从neo4j中查询dataNode的
                double dist = dist(rectNd);//计算dataNode到(x,y)的距离
                return new DistanceResult(dist, dataNodeId);
            }
        };
        nearests = searcher.nearest(nearestNeighbour, tx);
    }
    ...
    //定义距离公式
    private static final double dist(RectNd rect2d) {
        double[] xy = rect2d.getMaxXs();
        double x1 = xy[0];
        double y1 = xy[1];
        return Math.sqrt(Math.pow(x1 - x, 2) + Math.pow(y1 - y, 2));
    }

~~~
完整示例请参见 [测试用例](https://github.com/codingmiao/neo4j-rtree/blob/v2.x/src/test/java/org/wowtools/neo4j/rtree/RtreeNearestSearcherTest.java)


### 基于jts geometry对象的二维索引
[geometry2d](https://github.com/codingmiao/neo4j-rtree/tree/v2.x/src/main/java/org/wowtools/neo4j/rtree/geometry2d) 是一个针对二维几何对象的特化包，同样也包含了上述功能，示例如下：

geometry2d 索引数据的的增删改 [Geometry2dRtreeEditor](https://github.com/codingmiao/neo4j-rtree/blob/v2.x/src/test/java/org/wowtools/neo4j/rtree/geometry2d/Geometry2dRtreeEditorTest.java)

geometry2d 相交关系查询 [Geometry2dRtreeIntersectsSearcher](https://github.com/codingmiao/neo4j-rtree/blob/v2.x/src/test/java/org/wowtools/neo4j/rtree/geometry2d/Geometry2dRtreeEditorTest.java)

geometry2d 最邻近搜索 [Geometry2dRtreeNearestSearcher](https://github.com/codingmiao/neo4j-rtree/blob/v2.x/src/test/java/org/wowtools/neo4j/rtree/geometry2d/Geometry2dRtreeNearestSearcherTest.java)

## install

引入maven依赖，最新版本号为2.0.0

```
            <dependency>
                <groupId>org.wowtools</groupId>
                <artifactId>neo4j-rtree</artifactId>
                <version>${neo4j-rtree-version}</version>
            </dependency>
```

如果你的项目中已经使用了其它版本的neo4j(例如企业版)而引起冲突，可考虑exclusions:

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

注意，maven中央库的依赖用jdk11编译，所以如果你的项目使用了jdk8或其它版本，你可能需要自己编译一份适合于你的jdk的:

clone & install

```
git clone https://github.com/codingmiao/neo4j-rtree.git
mvn clean install -DskipTests

```
