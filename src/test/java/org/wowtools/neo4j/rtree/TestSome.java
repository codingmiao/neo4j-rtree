package org.wowtools.neo4j.rtree;


import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.wowtools.neo4j.rtree.spatial.Envelope;
import org.wowtools.neo4j.rtree.spatial.EnvelopeDecoder;

import static org.wowtools.neo4j.rtree.util.BboxIntersectUtil.bboxIntersect;

/**
 * @author liuyu
 * @date 2020/5/27
 */
public class TestSome {
    public static void main(String[] args) throws Exception{
        t2();
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


    private static void t2() throws Exception{
        class MyEnvelopeDecoder implements EnvelopeDecoder {
            @Override
            public Envelope decodeEnvelope(Object o) {
                byte[] wkb = (byte[]) o;
                Geometry geometry;
                try {
                    geometry = new WKBReader().read(wkb);
                } catch (ParseException e) {
                    throw new RuntimeException("pase wkb error ", e);
                }
                Geometry bound = geometry.getBoundary();
                Coordinate[] coords = bound.getCoordinates();
                double xmin, xmax, ymin, ymax;
                if (coords.length > 1) {
                    xmin = Double.MAX_VALUE;
                    ymin = Double.MAX_VALUE;
                    xmax = Double.MIN_VALUE;
                    ymax = Double.MIN_VALUE;
                    for (Coordinate coordinate : coords) {
                        double x = coordinate.x;
                        double y = coordinate.y;
                        if (x < xmin) {
                            xmin = x;
                        }
                        if (y < ymin) {
                            ymin = y;
                        }
                        if (x > xmax) {
                            xmax = x;
                        }
                        if (y > ymax) {
                            ymax = y;
                        }
                    }
                } else {
                    Coordinate coord = geometry.getCoordinate();
                    xmin = coord.x;
                    ymin = coord.y;
                    xmax = coord.x;
                    ymax = coord.y;
                }

                Envelope envelope = new Envelope(xmin, xmax, ymin, ymax);
                return envelope;
            }
        }
        MyEnvelopeDecoder myEnvelopeDecoder = new MyEnvelopeDecoder();
        Geometry line = new WKTReader().read("LINESTRING (93.27624401382229 51.39388590149433, 6.576662043340875 7.392660808666973, 19.293425313955648 36.84401696147332)");
        byte[] wkb = new WKBWriter().write(line);
        Envelope env = myEnvelopeDecoder.decodeEnvelope(wkb);
        System.out.println(env);
    }


}
