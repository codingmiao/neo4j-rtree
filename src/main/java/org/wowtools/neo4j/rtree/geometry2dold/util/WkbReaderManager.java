package org.wowtools.neo4j.rtree.geometry2dold.util;

import org.locationtech.jts.io.WKBReader;

/**
 * WkbReader的ThreadLocal获取器
 *
 * @author liuyu
 * @date 2020/11/27
 */
public class WkbReaderManager {
    private static final ThreadLocal<WKBReader> wkbReaderThreadLocal = new ThreadLocal<>();

    public static WKBReader get() {
        WKBReader wkbReader = wkbReaderThreadLocal.get();
        if (null == wkbReader) {
            wkbReader = new WKBReader();
            wkbReaderThreadLocal.set(wkbReader);
        }
        return wkbReader;
    }
}
