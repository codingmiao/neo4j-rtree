package org.wowtools.neo4j.rtree.bigshape.util;

import junit.framework.TestCase;
import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKTReader;
import org.wowtools.neo4j.rtree.bigshape.pojo.Grid;
import org.wowtools.neo4j.rtree.util.Singleton;

import java.util.List;
import java.util.Random;

public class GridCuterTest extends TestCase {

    @Test
    public void test() throws Exception {
        int testNum = 20;
        int pointNum = 100;
        Random random = new Random();
        for (int s = 0; s < testNum; s++) {
            Coordinate[] shell = new Coordinate[pointNum+1];

            for (int j = 0; j < pointNum; j++) {
                shell[j] = new Coordinate(random.nextDouble(),random.nextDouble());
            }
            shell[pointNum] = shell[0];

            Polygon geometry = Singleton.geometryFactory.createPolygon(shell);
            List<Grid> grids = GridCuter.cut(geometry, 10, 10);
            Geometry[] geos = new Geometry[grids.size()];
            int i = 0;
            for (Grid grid : grids) {
                geos[i] = grid.getGeometry();
                i++;
            }
            GeometryCollection geogrid = Singleton.geometryFactory.createGeometryCollection(geos);
            System.out.println(geometry);
            System.out.println(geogrid);
            assertTrue(geogrid.buffer(0).equalsTopo(SelfIntersectingDispose.validate(geogrid).buffer(0)));
        }

    }


}
