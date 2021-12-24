package org.wowtools.neo4j.rtree.geometry2dold.bigshape.util;

import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKTReader;
import org.wowtools.common.utils.ResourcesReader;
import org.wowtools.neo4j.rtree.geometry2dold.bigshape.pojo.Grid;
import org.wowtools.neo4j.rtree.geometry2dold.util.Singleton;

import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * 测一下性能
 * @author liuyu
 * @date 2020/11/24
 */
public class GridCuterTest1 {
    public static void main(String[] args) throws Exception{
        String wkt = ResourcesReader.readStr(GridCuterTest1.class,"wkt.txt");
        WKTReader wktReader = new WKTReader();
        Geometry geometry = wktReader.read(wkt);
        long t = System.currentTimeMillis();
        List<Grid> grids = GridCuter.cut(geometry, 10, 10);
        System.out.println("cost:"+(System.currentTimeMillis()-t));
        Geometry[] geos = new Geometry[grids.size()];
        int i = 0;
        for (Grid grid : grids) {
            geos[i] = grid.getGeometry();
            i++;
        }
        GeometryCollection geogrid = Singleton.geometryFactory.createGeometryCollection(geos);
        assertTrue(geogrid.buffer(0).equalsTopo(SelfIntersectingDispose.validate(geogrid).buffer(0)));
    }
}
