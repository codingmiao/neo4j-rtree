package org.wowtools.neo4j.rtree;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.CommunityBootstrapper;
import org.wowtools.common.utils.ResourcesReader;

import java.io.File;
import java.io.IOException;
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
    private static final CommunityBootstrapper serverBootstrapper;
    private static final String dbPath;
    private static final GraphDatabaseService graphDb;


    static {

        Properties p = new Properties();
        try {
            p.load(ResourcesReader.readStream(Neo4jDbManager.class, "neo4j.conf"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        dbPath = p.getProperty("dbms.directories.data");

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
        serverBootstrapper.start(storeDir, configOverrides);
        graphDb = serverBootstrapper.getDatabaseManagementService().database("neo4j");
    }

    private static void deleteFiles(File path) {
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
    public static GraphDatabaseService getGraphDb() {
        return graphDb;
    }

    /**
     * 获取服务模式对象
     *
     * @return
     */
    public static CommunityBootstrapper getServer() {
        return serverBootstrapper;
    }
}
