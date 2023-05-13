// package org.example;
//
// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.FileSystem;
// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.hive.conf.HiveConf;
// import org.apache.hadoop.hive.metastore.HiveMetaStore;
// import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
// import org.apache.hadoop.hive.metastore.api.MetaException;
// import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
// import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
// import org.apache.hadoop.mapreduce.MRConfig;
// import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
// import org.apache.hadoop.yarn.client.api.YarnClient;
// import org.apache.hadoop.yarn.conf.YarnConfiguration;
// import org.apache.hive.service.Service;
// import org.apache.hive.service.server.HiveServer2;
// import org.junit.Assert;
// import org.junit.BeforeClass;
// import org.junit.Test;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
// import java.io.File;
// import java.io.FileWriter;
// import java.io.IOException;
// import java.net.URL;
// import java.sql.*;
// import java.util.Objects;
//
/// **
// * HiveServerSimpleTest
// *
// * @author hhoa
// * @since 2023/5/9
// **/
//
// public class HiveSimpleTest {
//    protected static final String RESOURCE_DIR =
// Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource("")).getPath();
//    protected final static String ROOT_DIR = new File(RESOURCE_DIR).getParent();
//    protected final static String HIVE_DIR = new File(ROOT_DIR, "hive").getPath();
//    private static final Logger LOG = LoggerFactory.getLogger(HiveSimpleTest.class);
//    protected static MiniYARNCluster yarnCluster;
//    protected static MiniDFSCluster dfsCluster;
//    protected static YarnClient yarnClient;
//    protected static Configuration config;
//    protected static HiveConf hiveConf;
//    protected static HiveMetaStoreClient metaStoreClient;
//    protected static HiveServer2 hiveServer2;
//
//    static {
//        File file = new File(HIVE_DIR);
//        file.mkdirs();
//
//    }
//
//
//    public static void writeConfig(Configuration config, String configName) {
//        File coreSite = new File(RESOURCE_DIR, configName);
//        try (FileWriter writer = new FileWriter(coreSite)) {
//            config.writeXml(writer);
//            writer.flush();
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    public static void cleanUp() {
//        FileUtils.deleteFile(new Path(ROOT_DIR, "test").toString());
//        FileUtils.deleteFile(new Path(ROOT_DIR, "hive").toString());
//        FileUtils.deleteFile(new Path(ROOT_DIR, "zookeeper").toString());
//
//        FileUtils.deleteFile(new Path(RESOURCE_DIR, "yarn-site.xml").toString());
//        FileUtils.deleteFile(new Path(RESOURCE_DIR, "core-site.xml").toString());
//        FileUtils.deleteFile(new Path(RESOURCE_DIR, "hive-site.xml").toString());
//    }
//
//    @BeforeClass
//    public static void beforeClass() throws Exception {
//        cleanUp();
//        String user = System.getenv("USER");
//        config = new Configuration();
//        config.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
//        config.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
//        config.set(YarnConfiguration.YARN_MINICLUSTER_NM_PMEM_MB, "8192");
//        config.setBoolean(JHAdminConfig.MR_HISTORY_MINICLUSTER_FIXED_PORTS, true);
//        config.set(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_FRAMEWORK_NAME);
//        config.set(String.format("hadoop.proxyuser.%s.hosts", user), "*");
//        config.set(String.format("hadoop.proxyuser.%s.groups", user), "*");
//
//        dfsCluster = new MiniDFSCluster.Builder(config).build();
//        dfsCluster.waitClusterUp();
//
//        yarnCluster = new MiniMRYarnCluster("yarn");
//        yarnCluster.init(config);
//        yarnCluster.start();
//        while (!yarnCluster.waitForNodeManagersToConnect(500)) {
//            LOG.info("Waiting for Nodemanagers to connect");
//        }
//        writeConfig(config, "yarn-site.xml");
//        writeConfig(config, "core-site.xml");
//
//        yarnClient = YarnClient.createYarnClient();
//        yarnClient.init(config);
//        yarnClient.start();
//
//        InstanceSpec spec = new InstanceSpec(
//                new File(ROOT_DIR, "zookeeper")
//                , 20010
//                , 22001
//                , 22002
//                , false
//                , 100123
//                , 2000
//                , 60);
//        TestingServer testingServer = new TestingServer(spec, true);
//        testingServer.start();
//
//        hiveConf = new HiveConf();
//        hiveConf.addResource(config);
//        hiveConf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM,
// testingServer.getConnectString());
//        hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_IN_TEST, Boolean.TRUE);
//        hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, 20203);
//        hiveConf.set("datanucleus.schema.autoCreateTables", "true");
//        hiveConf.set("hive.metastore.schema.verification", "false");
//        hiveConf.set("javax.jdo.option.ConnectionURL"
//                , String.format("jdbc:derby:;databaseName=%s;create=true",
//                        new File(HIVE_DIR, "metastore_db")
//                                .getAbsolutePath()));
//        System.setProperty("derby.system.home", new File(HIVE_DIR).getAbsolutePath());
//        MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.AUTO_CREATE_ALL, true);
//
//        writeConfig(hiveConf, "hive-site.xml");
//        HiveConf.setHiveSiteLocation(new File(RESOURCE_DIR, "hive-site.xml").toURI().toURL());
//        new Thread(() -> {
//            try {
//                HiveMetaStore.startMetaStore(
//
// Integer.parseInt(HiveConf.ConfVars.METASTORE_SERVER_PORT.getDefaultValue())
//                        , HadoopThriftAuthBridge.getBridge()
//                        , hiveConf);
//            } catch (Throwable e) {
//                throw new RuntimeException(e);
//            }
//        }).start();
//
//        boolean isHiveMetaStoreStarted = false;
//        while (!isHiveMetaStoreStarted) {
//            try {
//                metaStoreClient = new HiveMetaStoreClient(hiveConf);
//                isHiveMetaStoreStarted = true;
//            } catch (MetaException e) {
//                LOG.info("HiveMetaStore is still starting, waiting...");
//                Thread.sleep(1000);
//            }
//        }
//        hiveServer2 = new HiveServer2();
//        hiveServer2.init(hiveConf);
//        hiveServer2.start();
//        Service.STATE serviceState = hiveServer2.getServiceState();
//        LOG.info(String.format("HiveServer2 current state is %s, waiting...",
// serviceState.toString()));
//        while (serviceState.compareTo(Service.STATE.STARTED) != 0) {
//            LOG.info(String.format("HiveServer2 current state is %s, waiting...",
// serviceState.toString()));
//            Thread.sleep(1000);
//            serviceState = hiveServer2.getServiceState();
//        }
//    }
//
//    @Test
//    public void testCrud() throws SQLException, IOException, ClassNotFoundException,
// InterruptedException {
//        Class.forName("org.apache.hive.jdbc.HiveDriver");
//        String user = System.getenv("USER");
//        int intVar = hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT);
//        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:" + intVar +
// "/default", user, "");
//        Statement stmt = con.createStatement();
//
//        LOG.info("create tables");
//        String tableName = "test_hive";
//        String dropTablePrepareStatement = "DROP TABLE IF EXISTS %s";
//        stmt.execute(String.format(dropTablePrepareStatement, tableName));
//        String createTablePrepareStatement = "CREATE TABLE %s (userid INT,movieid INT,rating
// INT,unixtime STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS TEXTFILE";
//        stmt.execute(String.format(createTablePrepareStatement, tableName));
//
//        LOG.info("show tables");
//        String showTablesPrepareStatement = "show tables '%s'";
//        ResultSet res = stmt.executeQuery(String.format(showTablesPrepareStatement, tableName));
//        if (res.next()) {
//            Assert.assertEquals(tableName.toLowerCase(), res.getString(1));
//        }
//
//        LOG.info("describe table");
//        String describeTablePrepareStatement = "DESCRIBE %s";
//        res = stmt.executeQuery(String.format(describeTablePrepareStatement, tableName));
//        while (res.next()) {
//            String colName = res.getString(1);
//            String type = res.getString(2);
//            switch (colName) {
//                case "userid":
//                case "rating":
//                case "movieid":
//                    Assert.assertEquals("int", type);
//                    break;
//                case "unixtime":
//                    Assert.assertEquals("string", type);
//            }
//        }
//
//        LOG.info("load data into table");
//        FileSystem fileSystem = FileSystem.get(config);
//        URL resource = ClassLoader.getSystemClassLoader().getResource("u.data");
//        fileSystem.copyFromLocalFile(new Path(Objects.requireNonNull(resource).getFile()), new
// Path("/tmp/u.data"));
//
//        String filepath = "/tmp/u.data";
//        String loadDataPrepareStatement = "LOAD DATA LOCAL INPATH '%s' OVERWRITE INTO TABLE %s";
//        stmt.execute(String.format(loadDataPrepareStatement, filepath, tableName));
//
//        String selectAllPrepareStatement = "SELECT * FROM %s WHERE userid=303 AND movieid=55";
//        res = stmt.executeQuery(String.format(selectAllPrepareStatement, tableName));
//        while (res.next()) {
//            Assert.assertEquals(303, res.getInt("userid"));
//            Assert.assertEquals(55, res.getInt("movieid"));
//        }
//
//        LOG.info("regular hive query");
//        String countPrepareStatement = "SELECT COUNT(*) FROM %s";
//        res = stmt.executeQuery(String.format(countPrepareStatement, tableName));
//        res.next();
//        Assert.assertEquals(100000, res.getInt(1));
//    }
// }
