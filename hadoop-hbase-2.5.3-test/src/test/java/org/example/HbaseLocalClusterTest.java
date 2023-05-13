package org.example;


import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class HbaseLocalClusterTest {
    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(HbaseLocalClusterTest.class);

    // Setup the property parser
    private static PropertyParser propertyParser;

    static {
        try {
            propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
            propertyParser.parsePropsFile();
        } catch (IOException e) {
            LOG.error("Unable to load property file: {}", propertyParser.getProperty(ConfigVars.DEFAULT_PROPS_FILE));
        }
    }

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private HbaseLocalCluster hbaseLocalCluster;

    @Before
    public void setUp() {
        hbaseLocalCluster = new HbaseLocalCluster.Builder()
                .setHbaseMasterPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_PORT_KEY)))
                .setHbaseMasterInfoPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_INFO_PORT_KEY)))
                .setNumRegionServers(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_NUM_REGION_SERVERS_KEY)))
                .setHbaseRootDir(propertyParser.getProperty(ConfigVars.HBASE_ROOT_DIR_KEY))
                .setZookeeperPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .setZookeeperZnodeParent(propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY))
                .setHbaseWalReplicationEnabled(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HBASE_WAL_REPLICATION_ENABLED_KEY)))
                .setHbaseConfiguration(new Configuration())
                .activeRestGateway()
                .setHbaseRestHost(propertyParser.getProperty(ConfigVars.HBASE_REST_HOST_KEY))
                .setHbaseRestPort(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)))
                .setHbaseRestInfoPort(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_INFO_PORT_KEY)))
                .setHbaseRestReadOnly(
                        Boolean.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_READONLY_KEY)))
                .setHbaseRestThreadMax(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMAX_KEY)))
                .setHbaseRestThreadMin(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMIN_KEY)))
                .build()
                .build();
    }

    @Test
    public void testHbaseMasterPort() {
        Assert.assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_PORT_KEY)),
                (int) hbaseLocalCluster.getHbaseMasterPort());
    }

    @Test
    public void testMissingHbaseMasterPort() {
        exception.expect(IllegalArgumentException.class);
        hbaseLocalCluster = new HbaseLocalCluster.Builder()
                .setHbaseMasterInfoPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_INFO_PORT_KEY)))
                .setNumRegionServers(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_NUM_REGION_SERVERS_KEY)))
                .setHbaseRootDir(propertyParser.getProperty(ConfigVars.HBASE_ROOT_DIR_KEY))
                .setZookeeperPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .setZookeeperZnodeParent(propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY))
                .setHbaseWalReplicationEnabled(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HBASE_WAL_REPLICATION_ENABLED_KEY)))
                .setHbaseConfiguration(new Configuration())
                .activeRestGateway()
                .setHbaseRestHost(propertyParser.getProperty(ConfigVars.HBASE_REST_HOST_KEY))
                .setHbaseRestPort(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)))
                .setHbaseRestInfoPort(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_INFO_PORT_KEY)))
                .setHbaseRestReadOnly(
                        Boolean.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_READONLY_KEY)))
                .setHbaseRestThreadMax(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMAX_KEY)))
                .setHbaseRestThreadMin(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMIN_KEY)))
                .build()
                .build();
    }

    @Test
    public void testHbaseMasterInfoPort() {
        Assert.assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_INFO_PORT_KEY)),
                (int) hbaseLocalCluster.getHbaseMasterInfoPort());
    }

    @Test
    public void testMissingHbaseMasterInfoPort() {
        exception.expect(IllegalArgumentException.class);
        hbaseLocalCluster = new HbaseLocalCluster.Builder()
                .setHbaseMasterPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_PORT_KEY)))
                .setNumRegionServers(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_NUM_REGION_SERVERS_KEY)))
                .setHbaseRootDir(propertyParser.getProperty(ConfigVars.HBASE_ROOT_DIR_KEY))
                .setZookeeperPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .setZookeeperZnodeParent(propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY))
                .setHbaseWalReplicationEnabled(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HBASE_WAL_REPLICATION_ENABLED_KEY)))
                .setHbaseConfiguration(new Configuration())
                .activeRestGateway()
                .setHbaseRestHost(propertyParser.getProperty(ConfigVars.HBASE_REST_HOST_KEY))
                .setHbaseRestPort(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)))
                .setHbaseRestInfoPort(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_INFO_PORT_KEY)))
                .setHbaseRestReadOnly(
                        Boolean.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_READONLY_KEY)))
                .setHbaseRestThreadMax(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMAX_KEY)))
                .setHbaseRestThreadMin(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMIN_KEY)))
                .build()
                .build();
    }

    @Test
    public void testHbaseNumRegionServers() {
        Assert.assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_NUM_REGION_SERVERS_KEY)),
                (int) hbaseLocalCluster.getNumRegionServers());
    }

    @Test
    public void testMissingHbaseNumRegionsServers() {
        exception.expect(IllegalArgumentException.class);
        hbaseLocalCluster = new HbaseLocalCluster.Builder()
                .setHbaseMasterPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_PORT_KEY)))
                .setHbaseMasterInfoPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_INFO_PORT_KEY)))
                .setHbaseRootDir(propertyParser.getProperty(ConfigVars.HBASE_ROOT_DIR_KEY))
                .setZookeeperPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .setZookeeperZnodeParent(propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY))
                .setHbaseWalReplicationEnabled(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HBASE_WAL_REPLICATION_ENABLED_KEY)))
                .setHbaseConfiguration(new Configuration())
                .activeRestGateway()
                .setHbaseRestHost(propertyParser.getProperty(ConfigVars.HBASE_REST_HOST_KEY))
                .setHbaseRestPort(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)))
                .setHbaseRestInfoPort(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_INFO_PORT_KEY)))
                .setHbaseRestReadOnly(
                        Boolean.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_READONLY_KEY)))
                .setHbaseRestThreadMax(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMAX_KEY)))
                .setHbaseRestThreadMin(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMIN_KEY)))
                .build()
                .build();
    }

    @Test
    public void testHbaseRootDir() {
        Assert.assertEquals(propertyParser.getProperty(ConfigVars.HBASE_ROOT_DIR_KEY), hbaseLocalCluster.getHbaseRootDir());
    }

    @Test
    public void testMissingHbaseRootDir() {
        exception.expect(IllegalArgumentException.class);
        hbaseLocalCluster = new HbaseLocalCluster.Builder()
                .setHbaseMasterPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_PORT_KEY)))
                .setHbaseMasterInfoPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_INFO_PORT_KEY)))
                .setNumRegionServers(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_NUM_REGION_SERVERS_KEY)))
                .setZookeeperPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .setZookeeperZnodeParent(propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY))
                .setHbaseWalReplicationEnabled(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HBASE_WAL_REPLICATION_ENABLED_KEY)))
                .setHbaseConfiguration(new Configuration())
                .activeRestGateway()
                .setHbaseRestHost(propertyParser.getProperty(ConfigVars.HBASE_REST_HOST_KEY))
                .setHbaseRestPort(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)))
                .setHbaseRestInfoPort(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_INFO_PORT_KEY)))
                .setHbaseRestReadOnly(
                        Boolean.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_READONLY_KEY)))
                .setHbaseRestThreadMax(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMAX_KEY)))
                .setHbaseRestThreadMin(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMIN_KEY)))
                .build()
                .build();
    }

    @Test
    public void testZookeeperPort() {
        Assert.assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)),
                (int) hbaseLocalCluster.getZookeeperPort());
    }

    @Test
    public void testMissingZookeeperPort() {
        exception.expect(IllegalArgumentException.class);
        hbaseLocalCluster = new HbaseLocalCluster.Builder()
                .setHbaseMasterPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_PORT_KEY)))
                .setHbaseMasterInfoPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_INFO_PORT_KEY)))
                .setNumRegionServers(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_NUM_REGION_SERVERS_KEY)))
                .setHbaseRootDir(propertyParser.getProperty(ConfigVars.HBASE_ROOT_DIR_KEY))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .setZookeeperZnodeParent(propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY))
                .setHbaseWalReplicationEnabled(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HBASE_WAL_REPLICATION_ENABLED_KEY)))
                .setHbaseConfiguration(new Configuration())
                .activeRestGateway()
                .setHbaseRestHost(propertyParser.getProperty(ConfigVars.HBASE_REST_HOST_KEY))
                .setHbaseRestPort(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)))
                .setHbaseRestInfoPort(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_INFO_PORT_KEY)))
                .setHbaseRestReadOnly(
                        Boolean.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_READONLY_KEY)))
                .setHbaseRestThreadMax(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMAX_KEY)))
                .setHbaseRestThreadMin(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMIN_KEY)))
                .build()
                .build();
    }

    @Test
    public void testZookeeperConnectionString() {
        Assert.assertEquals(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY),
                hbaseLocalCluster.getZookeeperConnectionString());
    }

    @Test
    public void testMissingZookeeperConnectionString() {
        exception.expect(IllegalArgumentException.class);
        hbaseLocalCluster = new HbaseLocalCluster.Builder()
                .setHbaseMasterPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_PORT_KEY)))
                .setHbaseMasterInfoPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_INFO_PORT_KEY)))
                .setNumRegionServers(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_NUM_REGION_SERVERS_KEY)))
                .setHbaseRootDir(propertyParser.getProperty(ConfigVars.HBASE_ROOT_DIR_KEY))
                .setZookeeperPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setZookeeperZnodeParent(propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY))
                .setHbaseWalReplicationEnabled(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HBASE_WAL_REPLICATION_ENABLED_KEY)))
                .setHbaseConfiguration(new Configuration())
                .activeRestGateway()
                .setHbaseRestHost(propertyParser.getProperty(ConfigVars.HBASE_REST_HOST_KEY))
                .setHbaseRestPort(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)))
                .setHbaseRestInfoPort(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_INFO_PORT_KEY)))
                .setHbaseRestReadOnly(
                        Boolean.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_READONLY_KEY)))
                .setHbaseRestThreadMax(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMAX_KEY)))
                .setHbaseRestThreadMin(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMIN_KEY)))
                .build()
                .build();
    }

    @Test
    public void testZookeeperZnodeParent() {
        Assert.assertEquals(propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY),
                hbaseLocalCluster.getZookeeperZnodeParent());
    }

    @Test
    public void testMissingZookeeperZnodeParent() {
        exception.expect(IllegalArgumentException.class);
        hbaseLocalCluster = new HbaseLocalCluster.Builder()
                .setHbaseMasterPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_PORT_KEY)))
                .setHbaseMasterInfoPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_INFO_PORT_KEY)))
                .setNumRegionServers(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_NUM_REGION_SERVERS_KEY)))
                .setHbaseRootDir(propertyParser.getProperty(ConfigVars.HBASE_ROOT_DIR_KEY))
                .setZookeeperPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .setHbaseWalReplicationEnabled(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HBASE_WAL_REPLICATION_ENABLED_KEY)))
                .setHbaseConfiguration(new Configuration())
                .activeRestGateway()
                .setHbaseRestHost(propertyParser.getProperty(ConfigVars.HBASE_REST_HOST_KEY))
                .setHbaseRestPort(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)))
                .setHbaseRestInfoPort(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_INFO_PORT_KEY)))
                .setHbaseRestReadOnly(
                        Boolean.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_READONLY_KEY)))
                .setHbaseRestThreadMax(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMAX_KEY)))
                .setHbaseRestThreadMin(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMIN_KEY)))
                .build()
                .build();
    }

    @Test
    public void testHbaseWalReplicationEnabled() {
        Assert.assertEquals(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HBASE_WAL_REPLICATION_ENABLED_KEY)),
                (boolean) hbaseLocalCluster.getHbaseWalReplicationEnabled());
    }

    @Test
    public void testMissingHbaseWalReplicationEnabled() {
        exception.expect(IllegalArgumentException.class);
        hbaseLocalCluster = new HbaseLocalCluster.Builder()
                .setHbaseMasterPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_PORT_KEY)))
                .setHbaseMasterInfoPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_INFO_PORT_KEY)))
                .setNumRegionServers(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_NUM_REGION_SERVERS_KEY)))
                .setHbaseRootDir(propertyParser.getProperty(ConfigVars.HBASE_ROOT_DIR_KEY))
                .setZookeeperPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .setZookeeperZnodeParent(propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY))
                .setHbaseConfiguration(new Configuration())
                .activeRestGateway()
                .setHbaseRestHost(propertyParser.getProperty(ConfigVars.HBASE_REST_HOST_KEY))
                .setHbaseRestPort(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)))
                .setHbaseRestInfoPort(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_INFO_PORT_KEY)))
                .setHbaseRestReadOnly(
                        Boolean.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_READONLY_KEY)))
                .setHbaseRestThreadMax(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMAX_KEY)))
                .setHbaseRestThreadMin(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMIN_KEY)))
                .build()
                .build();
    }

    @Test
    public void testHdfsConf() {
        Assert.assertTrue(hbaseLocalCluster.getHbaseConfiguration() instanceof org.apache.hadoop.conf.Configuration);

    }

    @Test
    public void testMissingHdfsConf() {
        exception.expect(IllegalArgumentException.class);
        hbaseLocalCluster = new HbaseLocalCluster.Builder()
                .setHbaseMasterPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_PORT_KEY)))
                .setHbaseMasterInfoPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_INFO_PORT_KEY)))
                .setNumRegionServers(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_NUM_REGION_SERVERS_KEY)))
                .setHbaseRootDir(propertyParser.getProperty(ConfigVars.HBASE_ROOT_DIR_KEY))
                .setZookeeperPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .setZookeeperZnodeParent(propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY))
                .setHbaseWalReplicationEnabled(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HBASE_WAL_REPLICATION_ENABLED_KEY)))
                .activeRestGateway()
                .setHbaseRestHost(propertyParser.getProperty(ConfigVars.HBASE_REST_HOST_KEY))
                .setHbaseRestPort(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)))
                .setHbaseRestInfoPort(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_INFO_PORT_KEY)))
                .setHbaseRestReadOnly(
                        Boolean.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_READONLY_KEY)))
                .setHbaseRestThreadMax(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMAX_KEY)))
                .setHbaseRestThreadMin(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMIN_KEY)))
                .build()
                .build();
    }


    @Test
    public void testHbaseRestPort() {
        Assert.assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)),
                (int) hbaseLocalCluster.getHbaseRestLocalCluster().getHbaseRestPort());
    }

    @Test
    public void testMissingHbaseRestPort() {
        exception.expect(IllegalArgumentException.class);
        hbaseLocalCluster = new HbaseLocalCluster.Builder()
                .setHbaseMasterPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_PORT_KEY)))
                .setHbaseMasterInfoPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_INFO_PORT_KEY)))
                .setNumRegionServers(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_NUM_REGION_SERVERS_KEY)))
                .setHbaseRootDir(propertyParser.getProperty(ConfigVars.HBASE_ROOT_DIR_KEY))
                .setZookeeperPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .setZookeeperZnodeParent(propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY))
                .activeRestGateway()
                .setHbaseRestHost(propertyParser.getProperty(ConfigVars.HBASE_REST_HOST_KEY))
                .setHbaseRestPort(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)))
                .setHbaseRestInfoPort(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_INFO_PORT_KEY)))
                .setHbaseRestReadOnly(
                        Boolean.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_READONLY_KEY)))
                .setHbaseRestThreadMax(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMAX_KEY)))
                .setHbaseRestThreadMin(
                        Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMIN_KEY)))
                .build()
                .build();
    }

    @Test
    public void testHbaseRestInfoPort() {
        Assert.assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_REST_INFO_PORT_KEY)),
                (int) hbaseLocalCluster.getHbaseRestLocalCluster().getHbaseRestInfoPort());
    }

    @Test
    public void testHbaseRestHost() {
        Assert.assertEquals(propertyParser.getProperty(ConfigVars.HBASE_REST_HOST_KEY),
                hbaseLocalCluster.getHbaseRestLocalCluster().getHbaseRestHost());
    }

    @Test
    public void testHbaseRestReadOnly() {
        Assert.assertEquals(Boolean.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_READONLY_KEY)),
                hbaseLocalCluster.getHbaseRestLocalCluster().getHbaseRestReadOnly());
    }

    @Test
    public void testHbaseRestThreadMax() {
        Assert.assertEquals(Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMAX_KEY)),
                hbaseLocalCluster.getHbaseRestLocalCluster().getHbaseRestThreadMax());
    }

    @Test
    public void testHbaseRestThreadMin() {
        Assert.assertEquals(Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMIN_KEY)),
                hbaseLocalCluster.getHbaseRestLocalCluster().getHbaseRestThreadMin());
    }

}
