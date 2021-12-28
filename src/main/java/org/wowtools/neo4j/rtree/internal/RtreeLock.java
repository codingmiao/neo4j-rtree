package org.wowtools.neo4j.rtree.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 索引的锁，保证读写安全
 *
 * @author liuyu
 * @date 2021/12/24
 */
public class RtreeLock {
    private static final Map<String, ReadWriteLock> useReadWriteLocks = new HashMap<>();
    private static final Object createIndexLock = new Object();

    public static ReadWriteLock getUseReadWriteLock(String indexName) {
        synchronized (useReadWriteLocks) {
            ReadWriteLock lock = useReadWriteLocks.get(indexName);
            if (null == lock) {
                lock = new ReentrantReadWriteLock();
                useReadWriteLocks.put(indexName, lock);
            }
            return lock;
        }
    }

    public static Object getCreateIndexLock() {
        return createIndexLock;
    }
}
