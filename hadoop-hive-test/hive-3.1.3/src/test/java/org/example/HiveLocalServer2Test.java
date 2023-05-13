// package org.example;
//
// import static org.junit.Assert.assertEquals;
// import static org.junit.Assert.assertTrue;
//
// import java.io.IOException;
//
// import com.sun.corba.se.spi.orb.PropertyParser;
// import org.apache.hadoop.hive.conf.HiveConf;
// import org.junit.BeforeClass;
// import org.junit.Rule;
// import org.junit.Test;
// import org.junit.rules.ExpectedException;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
//
// public class HiveLocalServer2Test {
//
//    // Logger
//    private static final Logger LOG = LoggerFactory.getLogger(HiveLocalServer2Test.class);
//
//    // Setup the property parser
//    private static PropertyParser propertyParser;
//    static {
//        try {
//            propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
//            propertyParser.parsePropsFile();
//        } catch(IOException e) {
//            LOG.error("Unable to load property file: {}",
// propertyParser.getProperty(ConfigVars.DEFAULT_PROPS_FILE));
//        }
//    }
//
//    @Rule
//    public ExpectedException exception = ExpectedException.none();
//
//    private static HiveLocalServer2 hiveLocalServer2;
//
//    @BeforeClass
//    public static void setUp() {
//        hiveLocalServer2 = new HiveLocalServer2.Builder()
//
// .setHiveServer2Hostname(propertyParser.getProperty(ConfigVars.HIVE_SERVER2_HOSTNAME_KEY))
//
// .setHiveServer2Port(Integer.parseInt(propertyParser.getProperty(ConfigVars.HIVE_SERVER2_PORT_KEY)))
//
// .setHiveMetastoreHostname(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_HOSTNAME_KEY))
//
// .setHiveMetastorePort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_PORT_KEY)))
//
// .setHiveMetastoreDerbyDbDir(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_DERBY_DB_DIR_KEY))
//                .setHiveScratchDir(propertyParser.getProperty(ConfigVars.HIVE_SCRATCH_DIR_KEY))
//
// .setHiveWarehouseDir(propertyParser.getProperty(ConfigVars.HIVE_WAREHOUSE_DIR_KEY))
//                .setHiveConf(buildHiveConf())
//
// .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
//                .build();
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
// }
