package org.wowtools.neo4j.rtree.geometry2dold.util;

import org.locationtech.jts.geom.GeometryFactory;

/**
 * 单例对象类
 *
 * @author liuyu
 * @date 2020/11/30
 */
public class Singleton {
    public static final GeometryFactory geometryFactory = new GeometryFactory();

}
