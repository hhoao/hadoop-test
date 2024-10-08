package org.hhoao.test.hive.base;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.curator.test.TestingCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.Service;
import org.apache.hive.service.server.HiveServer2;
import org.datanucleus.PropertyNames;
import org.hhoao.hadoop.test.api.MiniHadoopCluster;
import org.hhoao.hadoop.test.api.SecurityContext;
import org.hhoao.hadoop.test.cluster.zookeeper.HadoopZookeeperClusterTest;
import org.hhoao.hadoop.test.utils.Resources;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MiniHiveTest
 *
 * @author xianxing
 * @since 2024/9/5
 */
public abstract class HiveTest extends HadoopZookeeperClusterTest {
    private static final Logger LOG = LoggerFactory.getLogger(HiveTest.class);
    private final File hiveRootDir = new File(Resources.getTargetDir(), "hive");
    private final File hiveDir = new File(hiveRootDir, String.valueOf(System.currentTimeMillis()));
    protected HiveConf hiveConf;
    protected HiveMetaStoreClient metaStoreClient;
    protected HiveServer2 hiveServer2;
    private Connection connection;
    protected String url;

    public HiveMetaStoreClient getMetaStoreClient() {
        return metaStoreClient;
    }

    public HiveServer2 getHiveServer2() {
        return hiveServer2;
    }

    public HiveConf getHiveConf() {
        return hiveConf;
    }

    @Override
    public int getZookeeperClusterCount() {
        return 3;
    }

    @BeforeEach
    public void startHiveServer() throws IOException {
        SecurityContext securityContext = getHadoopCluster().getSecurityContext();
        UserGroupInformation ugi = securityContext.getDefaultUGI();
        ugi.doAs(
                (PrivilegedAction<?>)
                        () -> {
                            try {
                                initDir();
                                TestingCluster testingServer = getZookeeperCluster();

                                System.setProperty("derby.system.home", hiveDir.getAbsolutePath());
                                hiveConf = createHiveConf(testingServer);

                                writeConfig(hiveConf, "hive-site.xml");
                                HiveConf.setHiveSiteLocation(
                                        new File(hiveDir, "hive-site.xml").toURI().toURL());

                                startHiveMetaStore();

                                metaStoreClient = new HiveMetaStoreClient(hiveConf);
                                startHiveServer2();

                                connection = createConnection();

                                LOG.info("Hive start successful!");
                                return null;
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
    }

    private Connection createConnection() throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        String user = System.getenv("USER");
        int port = hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT);
        url = String.format("jdbc:hive2://localhost:%s/default", port);
        return DriverManager.getConnection(
                String.format("jdbc:hive2://localhost:%s/default", port), user, "");
    }

    private void startHiveServer2() throws IOException {
        hiveServer2 = new HiveServer2();
        hiveServer2.init(hiveConf);
        hiveServer2.start();
        Service.STATE serviceState = hiveServer2.getServiceState();

        while (serviceState.compareTo(Service.STATE.STARTED) != 0) {
            LOG.info("HiveServer2 current state is {}, waiting...", serviceState);
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            serviceState = hiveServer2.getServiceState();
        }
    }

    private void startHiveMetaStore() throws InterruptedException, MetaException {
        ReentrantLock reentrantLock = new ReentrantLock();
        AtomicBoolean startedServing = new AtomicBoolean(false);
        Thread thread = Thread.currentThread();
        CompletableFuture.runAsync(
                () -> {
                    try {
                        HiveMetaStore.startMetaStore(
                                MetastoreConf.getIntVar(
                                        hiveConf, MetastoreConf.ConfVars.SERVER_PORT),
                                HadoopThriftAuthBridge.getBridge(),
                                hiveConf,
                                reentrantLock,
                                reentrantLock.newCondition(),
                                startedServing);

                    } catch (Throwable e) {
                        thread.getUncaughtExceptionHandler().uncaughtException(thread, e);
                        thread.interrupt();
                    }
                });

        while (!startedServing.get()) {
            LOG.info("Wait for HiveMetaStore complete start");
            TimeUnit.SECONDS.sleep(1);
        }
        metaStoreClient = new HiveMetaStoreClient(hiveConf);
    }

    private HiveConf createHiveConf(TestingCluster testingServer) {
        HiveConf hiveConf = new HiveConf();
        Configuration config = getHadoopCluster().getConfig();
        hiveConf.addResource(config);

        int freeSocketPort = NetUtils.getFreeSocketPort();
        MetastoreConf.setLongVar(hiveConf, MetastoreConf.ConfVars.SERVER_PORT, freeSocketPort);
        MetastoreConf.setVar(
                hiveConf,
                MetastoreConf.ConfVars.THRIFT_URIS,
                "thrift://localhost:" + freeSocketPort);
        hiveConf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM, testingServer.getConnectString());
        hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_IN_TEST, Boolean.TRUE);
        hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, 20203);
        hiveConf.set(PropertyNames.PROPERTY_SCHEMA_AUTOCREATE_TABLES, "true");
        hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION, false);
        MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.SCHEMA_VERIFICATION, false);
        MetastoreConf.setVar(
                hiveConf,
                MetastoreConf.ConfVars.CONNECT_URL_KEY,
                String.format("jdbc:derby:;databaseName=%s;create=true", "metastore_db"));
        MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.AUTO_CREATE_ALL, true);
        MiniHadoopCluster hadoopCluster = getHadoopCluster();
        SecurityContext securityContext = hadoopCluster.getSecurityContext();
        if (securityContext != null) {
            Map<String, File> principalKeytabMap = securityContext.getPrincipalKeytabMap();
            principalKeytabMap.forEach(
                    (principal, keytab) -> {
                        MetastoreConf.setBoolVar(
                                hiveConf, MetastoreConf.ConfVars.USE_THRIFT_SASL, true);
                        MetastoreConf.setVar(
                                hiveConf,
                                MetastoreConf.ConfVars.CLIENT_KERBEROS_PRINCIPAL,
                                principal);
                        MetastoreConf.setVar(
                                hiveConf, MetastoreConf.ConfVars.KERBEROS_PRINCIPAL, principal);
                        MetastoreConf.setVar(
                                hiveConf,
                                MetastoreConf.ConfVars.KERBEROS_KEYTAB_FILE,
                                keytab.getAbsolutePath());

                        hiveConf.setVar(
                                HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB,
                                keytab.getAbsolutePath());
                        hiveConf.setVar(
                                HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL, principal);
                        hiveConf.set(
                                DFSConfigKeys.HADOOP_SECURITY_AUTH_TO_LOCAL,
                                "RULE:[1:$1] RULE:[2:$1]");
                    });
        }

        return hiveConf;
    }

    private void initDir() throws IOException {
        hiveDir.mkdirs();
    }

    @AfterEach
    public void closeHiveServer() {
        hiveServer2.stop();
        metaStoreClient.close();
    }

    public void writeConfig(Configuration config, String configName) {
        File coreSite = new File(hiveDir, configName);
        try (FileWriter writer = new FileWriter(coreSite)) {
            config.writeXml(writer);
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public String getUrl() {
        return url;
    }
}
