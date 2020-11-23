package org.wowtools.neo4j.rtree;


import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.wowtools.neo4j.rtree.spatial.Envelope;
import org.wowtools.neo4j.rtree.spatial.EnvelopeDecoder;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.wowtools.neo4j.rtree.util.BboxIntersectUtil.bboxIntersect;

/**
 * @author liuyu
 * @date 2020/5/27
 */
public class TestSome {
    public static void main(String[] args) throws Exception{
        ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        readWriteLock.readLock().lock();
        readWriteLock.writeLock().lock();
        readWriteLock.writeLock().unlock();
        readWriteLock.readLock().unlock();
    }

    private static void t1(){
        System.out.println(bboxIntersect(
                new double[]{3, 1, 8, 9},
                new double[]{
                        13,
                        0,
                        98,
                        99
                }));
    }




}
