// package org.example;
//
// import com.sun.corba.se.spi.orb.PropertyParser;
// import org.apache.hadoop.hive.conf.HiveConf;
// import org.junit.AfterClass;
// import org.junit.BeforeClass;
// import org.junit.Test;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
// import java.io.IOException;
// import java.sql.*;
//
// import static org.junit.Assert.assertEquals;
//
// public class HiveLocalServer2IntegrationTest {
//
//    // Logger
//    private static final Logger LOG =
// LoggerFactory.getLogger(HiveLocalServer2IntegrationTest.class);
//
//    // Setup the property parser
//    private static PropertyParser propertyParser;
//    private static ZookeeperLocalCluster zookeeperLocalCluster;
//    private static HiveLocalMetaStore hiveLocalMetaStore;
//    private static HiveLocalServer2 hiveLocalServer2;
//
//    static {
//        try {
//            propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
//            propertyParser.parsePropsFile();
//        } catch (IOException e) {
//            LOG.error("Unable to load property file: {}",
// propertyParser.getProperty(ConfigVars.DEFAULT_PROPS_FILE));
//        }
//    }
//
//    @BeforeClass
//    public static void setUp() throws Exception {
//        zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
//
// .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
//                .setTempDir(propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY))
//
// .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
//                .build();
//        zookeeperLocalCluster.start();
//
//        hiveLocalMetaStore = new HiveLocalMetaStore.Builder()
//
// .setHiveMetastoreHostname(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_HOSTNAME_KEY))
//
// .setHiveMetastorePort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_PORT_KEY)) + 50)
//
// .setHiveMetastoreDerbyDbDir(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_DERBY_DB_DIR_KEY))
//                .setHiveScratchDir(propertyParser.getProperty(ConfigVars.HIVE_SCRATCH_DIR_KEY))
//
// .setHiveWarehouseDir(propertyParser.getProperty(ConfigVars.HIVE_WAREHOUSE_DIR_KEY))
//                .setHiveConf(buildHiveConf())
//                .build();
//        hiveLocalMetaStore.start();
//
//
//
//        hiveLocalServer2 = new HiveLocalServer2.Builder()
//
// .setHiveServer2Hostname(propertyParser.getProperty(ConfigVars.HIVE_SERVER2_HOSTNAME_KEY))
//
// .setHiveServer2Port(Integer.parseInt(propertyParser.getProperty(ConfigVars.HIVE_SERVER2_PORT_KEY)))
//
// .setHiveMetastoreHostname(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_HOSTNAME_KEY))
//
// .setHiveMetastorePort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_PORT_KEY)) + 50)
//
// .setHiveMetastoreDerbyDbDir(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_DERBY_DB_DIR_KEY))
//                .setHiveScratchDir(propertyParser.getProperty(ConfigVars.HIVE_SCRATCH_DIR_KEY))
//
// .setHiveWarehouseDir(propertyParser.getProperty(ConfigVars.HIVE_WAREHOUSE_DIR_KEY))
//                .setHiveConf(buildHiveConf())
//
// .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
//                .build();
//        hiveLocalServer2.start();
//    }
//
//    @AfterClass
//    public static void tearDown() throws Exception {
//        hiveLocalServer2.stop();
//        hiveLocalMetaStore.stop();
//        zookeeperLocalCluster.stop();
//    }
//
//    public static HiveConf buildHiveConf() {
//        HiveConf hiveConf = new HiveConf();
//        hiveConf.set(HiveConf.ConfVars.HIVE_TXN_MANAGER.varname,
// "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
//        hiveConf.set(HiveConf.ConfVars.HIVE_COMPACTOR_INITIATOR_ON.varname, "true");
//        hiveConf.set(HiveConf.ConfVars.HIVE_COMPACTOR_WORKER_THREADS.varname, "5");
//        hiveConf.set("hive.root.logger", "DEBUG,console");
//        hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
//        hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
//        hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
//        System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.varname, " ");
//        System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.varname, " ");
//        return hiveConf;
//    }
//
//    @Test
//    public void testHiveLocalServer2() throws ClassNotFoundException, SQLException {
//
//        // Load the Hive JDBC driver
//        LOG.info("HIVE: Loading the Hive JDBC Driver");
//        Class.forName("org.apache.hive.jdbc.HiveDriver");
//
//        //
//        // Create an ORC table and describe it
//        //
//        // Get the connection
//        String user = System.getenv("USER");
//        Connection con = DriverManager.getConnection("jdbc:hive2://" +
//                        hiveLocalServer2.getHiveServer2Hostname() + ":" +
//                        hiveLocalServer2.getHiveServer2Port() + "/" +
//                        propertyParser.getProperty(ConfigVars.HIVE_TEST_DATABASE_NAME_KEY),
//                user,
//                "");
//
//        // Create the DB
//        Statement stmt;
//        try {
//            String createDbDdl = "CREATE DATABASE IF NOT EXISTS " +
//                    propertyParser.getProperty(ConfigVars.HIVE_TEST_DATABASE_NAME_KEY);
//            stmt = con.createStatement();
//            LOG.info("HIVE: Running Create Database Statement: {}", createDbDdl);
//            stmt.execute(createDbDdl);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        // Drop the table incase it still exists
//        String dropDdl = "DROP TABLE " +
// propertyParser.getProperty(ConfigVars.HIVE_TEST_DATABASE_NAME_KEY) + "." +
//                propertyParser.getProperty(ConfigVars.HIVE_TEST_TABLE_NAME_KEY);
//        stmt = con.createStatement();
//        LOG.info("HIVE: Running Drop Table Statement: {}", dropDdl);
//        stmt.execute(dropDdl);
//
//        // Create the ORC table
//        String createDdl = "CREATE TABLE IF NOT EXISTS " +
//                propertyParser.getProperty(ConfigVars.HIVE_TEST_DATABASE_NAME_KEY) + "." +
//                propertyParser.getProperty(ConfigVars.HIVE_TEST_TABLE_NAME_KEY) + " (id INT, msg
// STRING) " +
//                "PARTITIONED BY (dt STRING) " +
//                "CLUSTERED BY (id) INTO 16 BUCKETS " +
//                "STORED AS ORC tblproperties(\"orc.compress\"=\"NONE\")";
//        stmt = con.createStatement();
//        LOG.info("HIVE: Running Create Table Statement: {}", createDdl);
//        stmt.execute(createDdl);
//
//        // Issue a describe on the new table and display the output
//        LOG.info("HIVE: Validating Table was Created: ");
//        ResultSet resultSet = stmt.executeQuery("DESCRIBE FORMATTED " +
//                propertyParser.getProperty(ConfigVars.HIVE_TEST_TABLE_NAME_KEY));
//        int count = 0;
//        while (resultSet.next()) {
//            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
//            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
//                System.out.print(resultSet.getString(i));
//            }
//            System.out.println();
//            count++;
//        }
//        assertEquals(38, count);
//
//        // Drop the table
//        dropDdl = "DROP TABLE " +
// propertyParser.getProperty(ConfigVars.HIVE_TEST_DATABASE_NAME_KEY) + "." +
//                propertyParser.getProperty(ConfigVars.HIVE_TEST_TABLE_NAME_KEY);
//        stmt = con.createStatement();
//        LOG.info("HIVE: Running Drop Table Statement: {}", dropDdl);
//        stmt.execute(dropDdl);
//    }
//
// }
