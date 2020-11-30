package org.wowtools.neo4j.rtree;


import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.operation.linemerge.LineMerger;
import org.wowtools.neo4j.rtree.spatial.Envelope;
import org.wowtools.neo4j.rtree.spatial.EnvelopeDecoder;

import java.util.Random;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.wowtools.neo4j.rtree.util.BboxIntersectUtil.bboxIntersect;

/**
 * @author liuyu
 * @date 2020/5/27
 */
public class TestSome {
    public static void main(String[] args) throws Exception {
    }


}
