package org.wowtools.neo4j.rtree;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.CommunityBootstrapper;
import org.neo4j.server.NeoBootstrapper;
import org.wowtools.common.utils.ResourcesReader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * neoj4数据库管理工具
 *
 * @author liuyu
 * @date 2018/11/14
 */
public class Neo4jDbManager {
    private final NeoBootstrapper serverBootstrapper;
    private final String dbPath;
    private final GraphDatabaseService graphDb;


    private final FileLock lock;

    {

        Properties p = new Properties();
        try {
            p.load(ResourcesReader.readStream(Neo4jDbManager.class, "neo4j.conf"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        dbPath = p.getProperty("dbms.directories.data");

        try {
            FileChannel channel = new FileOutputStream(dbPath + ".testlock", true).getChannel();
            lock = channel.lock();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


        File storeDir = new File(dbPath);

        deleteFiles(storeDir);//删除现有文件夹
        serverBootstrapper = new CommunityBootstrapper();
        String cfgFilePath = ResourcesReader.getClassRootPath(Neo4jDbManager.class) +
                "/neo4j.conf";
        Optional<File> cfgFile = Optional.of(new File(cfgFilePath));
        System.out.println("======== neo cfgFilePath:" + cfgFilePath + " " + cfgFile.get().exists());

        var cfgKeys = p.stringPropertyNames();
        Map<String, String> configOverrides = new HashMap<>(cfgKeys.size());
        for (String cfgKey : cfgKeys) {//把所有配置都读到map里，解决默认端口无法修改的问题
            configOverrides.put(cfgKey, p.getProperty(cfgKey));
        }
        serverBootstrapper.start(storeDir.toPath(), configOverrides);
        graphDb = serverBootstrapper.getDatabaseManagementService().database("neo4j");
    }

    private void deleteFiles(File path) {
        if (null != path) {
            if (!path.exists())
                return;
            if (path.isFile()) {
                boolean result = path.delete();
                int tryCount = 0;
                while (!result && tryCount++ < 10) {
                    System.gc(); // 回收资源
                    result = path.delete();
                }
            }
            File[] files = path.listFiles();
            if (null != files) {
                for (int i = 0; i < files.length; i++) {
                    deleteFiles(files[i]);
                }
            }
            path.delete();
        }
    }

    /**
     * 获取内嵌模式db操作对象
     *
     * @return
     */
    public GraphDatabaseService getGraphDb() {
        return graphDb;
    }

    /**
     * 获取服务模式对象
     *
     * @return
     */
    public NeoBootstrapper getServer() {
        return serverBootstrapper;
    }

    public void afterTest() {
        try {
            lock.release();
        } catch (IOException e) {
            e.printStackTrace();
        }
        serverBootstrapper.stop();
        System.out.println("test end");
    }

}
