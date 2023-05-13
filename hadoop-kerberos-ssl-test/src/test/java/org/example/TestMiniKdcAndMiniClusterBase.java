package org.example;

import org.apache.curator.shaded.com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

/**
 * TestMiniKdcAndMiniCluster
 *
 * @author hhoa
 * @since 2023/4/24
 **/

public class TestMiniKdcAndMiniClusterBase {
    protected static final String hostName = "localhost";
    protected static final File baseDir = new File("target/test-classes");
    protected static final File configDir = new File(baseDir, "config");
    protected static final File kdcDir = new File(baseDir, "kdc");
    protected static File keytabFile = new File(kdcDir, "krb5.keytable");
    private static final Logger LOG = LoggerFactory.getLogger(TestMiniKdcAndMiniClusterBase.class);
    protected static File sslStoreDir = new File(baseDir, "ssl_store");
    protected static String hadoopServicePrincipal = "hadoop";
    protected static MiniYARNCluster yarnCluster;
    protected static MiniDFSCluster dfsCluster;
    protected static MiniKdc kdc = null;
    protected static Configuration config = null;
    protected static YarnClient yarnClient = null;

    static {
        baseDir.mkdirs();
        configDir.mkdirs();
        kdcDir.mkdirs();
        sslStoreDir.mkdirs();
    }

    public static void startMiniKdc() throws Exception {
        Properties conf = MiniKdc.createConf();
        if (LOG.isDebugEnabled()) {
            conf.setProperty(MiniKdc.DEBUG, "true");
        }
        conf.setProperty(MiniKdc.KDC_BIND_ADDRESS, hostName);
        MiniKdc miniKdc = new MiniKdc(conf, kdcDir);
        miniKdc.start();
        hadoopServicePrincipal = hadoopServicePrincipal + "/" + hostName;
        miniKdc.createPrincipal(keytabFile, hadoopServicePrincipal);
        hadoopServicePrincipal = hadoopServicePrincipal + "@" + miniKdc.getRealm();
        LOG.info("-------------------------------------------------------------------");
        LOG.info("Test HadoopService Principal: {}", hadoopServicePrincipal);
        LOG.info("Test Keytab: {}", keytabFile);
        LOG.info("-------------------------------------------------------------------");
        kdc = miniKdc;
    }

    private static void setupSSL(Configuration configuration) {
        Class klass = TestMiniKdcAndMiniClusterBase.class;
        String file = klass.getName();
        file = file.replace('.', '/') + ".class";
        URL url = Thread.currentThread().getContextClassLoader().getResource(file);
        String sslConfigDir;
        assert url != null;
        if (url.getProtocol().equals("jar")) {
            File tempDir = com.google.common.io.Files.createTempDir();
            sslConfigDir = tempDir.getAbsolutePath();
            tempDir.deleteOnExit();
        } else {
            try {
                sslConfigDir = url.toURI().getPath();
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
            sslConfigDir = sslConfigDir.substring(0, sslConfigDir.length() - file.length() - 1);
        }
        try {
            KeyStoreTestUtil.setupSSLConfig(sslStoreDir.getAbsolutePath(), sslConfigDir, configuration, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void populateYarnSecureConfigurations(
            Configuration conf, String principal, String keytab) {
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, "true");

        conf.set(YarnConfiguration.RM_KEYTAB, keytab);
        conf.set(YarnConfiguration.RM_PRINCIPAL, principal);
        conf.set(YarnConfiguration.NM_KEYTAB, keytab);
        conf.set(YarnConfiguration.NM_PRINCIPAL, principal);

        conf.set(YarnConfiguration.RM_WEBAPP_SPNEGO_USER_NAME_KEY, principal);
        conf.set(YarnConfiguration.RM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY, keytab);
        conf.set(YarnConfiguration.NM_WEBAPP_SPNEGO_USER_NAME_KEY, principal);
        conf.set(YarnConfiguration.NM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY, keytab);

    }

    @BeforeClass
    public static void setup() throws Exception {
        startMiniKdc();
        config = new Configuration();
        setupSSL(config);

        populateYarnSecureConfigurations(config, hadoopServicePrincipal, keytabFile.getAbsolutePath());
        config.set(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, "true");
        config.set(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY, keytabFile.getAbsolutePath());
        config.set(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, hadoopServicePrincipal);
        config.set(DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY, keytabFile.getAbsolutePath());
        config.set(DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, hadoopServicePrincipal);
        config.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, "HTTPS_ONLY");
        config.set(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, "true");
        config.set(DFSConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication");
        config.set("hadoop.proxyuser.hhoa.hosts", "*");
        config.set("hadoop.proxyuser.hhoa.groups", "*");

        config.set(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, "true");
        config.set(YarnConfiguration.LOG_AGGREGATION_ENABLED, "true");

        config.set(JHAdminConfig.MR_HISTORY_MINICLUSTER_FIXED_PORTS, "true");
        config.set(JHAdminConfig.MR_HISTORY_PRINCIPAL, hadoopServicePrincipal);
        config.set(JHAdminConfig.MR_HISTORY_KEYTAB, keytabFile.getAbsolutePath());

        config.set(MRConfig.FRAMEWORK_NAME, "yarn");

        dfsCluster = new MiniDFSCluster.Builder(config)
                .numDataNodes(1)
                .checkDataNodeAddrConfig(false)
                .build();
        dfsCluster.waitClusterUp();

        yarnCluster = new MiniMRYarnCluster("test");
        yarnCluster.init(config);
        yarnCluster.start();

        File tempDir = Files.createTempDir();
        config.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tempDir.getAbsolutePath());
        while (!yarnCluster.waitForNodeManagersToConnect(500)) {
            LOG.info("Waiting for Nodemanagers to connect");
        }
        writeConfigXML(config, "yarn-site.yml");
        writeConfigXML(config, "hdfs-site.yml");
    }

    public static void writeConfigXML(Configuration yarnConf, String filename)
            throws IOException {
        File yarnSiteXML = new File(configDir, filename);
        try (FileWriter writer = new FileWriter(yarnSiteXML)) {
            yarnConf.writeXml(writer);
            writer.flush();
        }
    }

    @AfterClass
    public static void setDown() throws IOException {
        kdc.stop();
        yarnCluster.close();
        dfsCluster.close();
    }

    @Before
    public void beforeEach() {
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(config);
        yarnClient.start();
    }
}
