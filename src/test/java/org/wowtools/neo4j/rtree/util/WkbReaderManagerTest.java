package org.wowtools.neo4j.rtree.util;

import junit.framework.TestCase;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

public class WkbReaderManagerTest extends TestCase {

    private volatile boolean flag;
    @Test
    public void test() {
        test1();
    }

    public void test1(){
        //1、测试单个wkbreader多线程环境,应该会抛异常
        WKBReader wkbReader = new WKBReader();
        for (int i = 0; i < 500; i++) {
            final double d = i;
            new Thread(()->{
                byte[] wkb = new WKBWriter().write(new GeometryFactory().createPoint(new Coordinate(d, d)));
                try {
                    wkbReader.read(wkb);
                } catch (Exception e) {
//                    e.printStackTrace();
                    flag = true;
                }
            }).start();

        }
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if(!flag){
            throw new RuntimeException("wkbreader未产生多线程异常，请多次运行确认，或检查是否jts官方已优化");
        }
    }
}
