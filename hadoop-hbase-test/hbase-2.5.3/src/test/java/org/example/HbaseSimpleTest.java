package org.example;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Objects;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HbaseSimpleTest
 *
 * @author hhoa
 * @since 2023/5/6
 */
public class HbaseSimpleTest {
    protected static final String RESOURCE_DIR =
            Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource("")).getPath();
    protected static final File ROOT_DIR = new File(RESOURCE_DIR).getParentFile();
    protected static final File ZK_TMP_PATH = new File(ROOT_DIR, "zk_tmp");
    private static final Logger LOG = LoggerFactory.getLogger(HbaseSimpleTest.class);
    private static final String TABLE_NAME = "testTable";
    private static final String CF_DEFAULT = "testFamily";
    protected static Configuration hBaseConfig;
    protected static MiniHBaseCluster hBaseCluster;
    protected static MiniYARNCluster yarnCluster;
    protected static MiniDFSCluster dfsCluster;
    protected static YarnClient yarnClient;
    protected static FileSystem fileSystem;
    protected static Configuration config;

    @BeforeClass
    public static void test() throws Exception {
        cleanUp();
        config = new Configuration();
        config.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
        config.setBoolean(JHAdminConfig.MR_HISTORY_MINICLUSTER_FIXED_PORTS, true);
        config.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
        config.set(YarnConfiguration.YARN_MINICLUSTER_NM_PMEM_MB, "8192");
        config.set(MRConfig.FRAMEWORK_NAME, "yarn");
        dfsCluster = new MiniDFSCluster.Builder(config).build();
        dfsCluster.waitClusterUp();
        yarnCluster = new MiniMRYarnCluster("yarn");
        yarnCluster.init(config);
        yarnCluster.start();
        while (!yarnCluster.waitForNodeManagersToConnect(500)) {
            LOG.info("Waiting for Nodemanagers to connect");
        }
        writeConfig(config, "yarn-site.xml");
        writeConfig(config, "core-site.xml");

        //        MiniZooKeeperCluster miniZooKeeperCluster = new MiniZooKeeperCluster();
        //        int zkPort = miniZooKeeperCluster.startup(new File(ZK_TMP_PATH));
        TestingServer testingServer = new TestingServer(12342, ZK_TMP_PATH);
        int zkPort = testingServer.getPort();

        hBaseConfig = HBaseConfiguration.create(config);
        hBaseConfig.set(HConstants.ZOOKEEPER_CLIENT_PORT, String.valueOf(zkPort));
        hBaseConfig.setBoolean(
                LocalHBaseCluster.ASSIGN_RANDOM_PORTS,
                hBaseConfig.getBoolean(LocalHBaseCluster.ASSIGN_RANDOM_PORTS, true));
        hBaseConfig.setBoolean(CommonFSUtils.UNSAFE_STREAM_CAPABILITY_ENFORCE, false);
        if (hBaseConfig.getInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, -1) == -1) {
            hBaseConfig.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 1);
        }
        if (hBaseConfig.getInt(ServerManager.WAIT_ON_REGIONSERVERS_MAXTOSTART, -1) == -1) {
            hBaseConfig.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MAXTOSTART, 1);
        }
        writeConfig(hBaseConfig, "hbase-site.xml");
        hBaseConfig = HBaseConfiguration.create(config);
        hBaseCluster = new MiniHBaseCluster(hBaseConfig, 1, 1, 1, null, null, null);
    }

    public static void writeConfig(Configuration config, String configName) {
        File configFile = new File(RESOURCE_DIR, configName);
        try (FileWriter writer = new FileWriter(configFile)) {
            config.writeXml(writer);
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected static void cleanUp() throws IOException {
        FileUtils.deleteDirectory(ZK_TMP_PATH);
        FileUtils.delete(new File(ROOT_DIR, "test").getAbsoluteFile());
        FileUtils.delete(new File(RESOURCE_DIR, "yarn-site.xml").getAbsoluteFile());
        FileUtils.delete(new File(RESOURCE_DIR, "core-site.xml").getAbsoluteFile());
    }

    @Before
    public void beforeSparkTest() throws IOException {
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(config);
        yarnClient.start();

        fileSystem = FileSystem.get(config);
    }

    @Test
    public void testHBase2() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(hBaseConfig);
                Admin admin = connection.getAdmin()) {
            try (Table t = connection.getTable(TableName.META_TABLE_NAME);
                    ResultScanner s = t.getScanner(new Scan())) {
                for (; ; ) {
                    if (s.next() == null) {
                        break;
                    }
                }
            }
            // Create or override has CF_DEFAULT family table
            TableDescriptorBuilder.ModifyableTableDescriptor table =
                    new TableDescriptorBuilder.ModifyableTableDescriptor(
                            TableName.valueOf(TABLE_NAME));
            table.setColumnFamily(
                    new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(
                                    CF_DEFAULT.getBytes())
                            .setCompressionType(Compression.Algorithm.NONE));
            if (admin.tableExists(table.getTableName())) {
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }
            admin.createTable(table);

            //  Is table exist
            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (!admin.tableExists(tableName)) {
                System.out.println("Table does not exist.");
                System.exit(-1);
            }

            // Update existing table
            ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor newColumn =
                    new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(
                            "NEWCF".getBytes());
            newColumn.setCompactionCompressionType(Compression.Algorithm.GZ);
            newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            admin.addColumnFamily(tableName, newColumn);

            // Update existing column family
            ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor existingColumn =
                    new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(
                            CF_DEFAULT.getBytes());
            existingColumn.setCompactionCompressionType(Compression.Algorithm.GZ);
            existingColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            admin.modifyColumnFamily(tableName, existingColumn);

            // Disable an existing table
            admin.disableTable(tableName);

            // Delete an existing column family
            admin.deleteColumnFamily(tableName, CF_DEFAULT.getBytes());

            // Delete a table (Need to be disabled first)
            admin.deleteTable(tableName);
        }
    }
}
