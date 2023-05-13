package org.hhoao.hadoop.test.cluster;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TestMiniKdcAndMiniCluster
 *
 * @author hhoa
 * @since 2023/4/24
 */
public class MiniHadoopSecurityCluster extends MiniHadoopNoSecurityCluster {
    private static final Logger LOG = LoggerFactory.getLogger(MiniHadoopSecurityCluster.class);
    protected final File kdcDir = new File(baseDir, "kdc");
    protected File keytabFile = new File(kdcDir, "krb5.keytable");
    protected File sslStoreDir = new File(baseDir, "ssl_store");

    protected String localUser = System.getProperty("user.name");
    protected static final String hostName = "localhost";
    protected String hadoopServicePrincipalUser = localUser;
    protected String hadoopServicePrincipalUserWithInstance =
            hadoopServicePrincipalUser + "/" + hostName;
    protected String hadoopServicePrincipal;
    protected MiniKdc kdc = null;

    public File getSslStoreDir() {
        return sslStoreDir;
    }

    public File getSslConfigDir() {
        return sslConfigDir;
    }

    public MiniKdc getKdc() {
        return kdc;
    }

    public String getHadoopServicePrincipal() {
        return hadoopServicePrincipal;
    }

    public File getKeytabFile() {
        return keytabFile;
    }

    private void startMiniKdc() throws Exception {
        kdcDir.mkdirs();
        Properties conf = MiniKdc.createConf();
        conf.setProperty(MiniKdc.DEBUG, "false");
        conf.setProperty(MiniKdc.KDC_BIND_ADDRESS, hostName);
        MiniKdc miniKdc = new MiniKdc(conf, kdcDir);
        miniKdc.start();
        miniKdc.createPrincipal(keytabFile, hadoopServicePrincipalUserWithInstance);
        hadoopServicePrincipal = hadoopServicePrincipalUserWithInstance + "@" + miniKdc.getRealm();
        LOG.info("-------------------------------------------------------------------");
        LOG.info("Test HadoopService Principal: {}", hadoopServicePrincipal);
        LOG.info("Test Keytab: {}", keytabFile);
        LOG.info("Test Kdc Port: {}", miniKdc.getPort());
        LOG.info("Test Kdc Config: {}", miniKdc.getKrb5conf());
        LOG.info("-------------------------------------------------------------------");
        kdc = miniKdc;
    }

    private void setupSSL(Configuration configuration) {
        sslStoreDir.mkdirs();
        sslConfigDir.mkdirs();
        try {
            KeyStoreTestUtil.setupSSLConfig(
                    sslStoreDir.getAbsolutePath(),
                    sslConfigDir.getAbsolutePath(),
                    configuration,
                    false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void populateYarnSecureConfigurations(
            Configuration conf, String principal, String keytab) {
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, "true");
        conf.set(YarnConfiguration.RM_KEYTAB, keytab);
        conf.set(YarnConfiguration.RM_PRINCIPAL, principal);
        conf.set(YarnConfiguration.NM_KEYTAB, keytab);
        conf.set(YarnConfiguration.NM_PRINCIPAL, principal);
        conf.set(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, principal);
        conf.set(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY, keytab);
        conf.set(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY, keytab);
        conf.set(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, principal);
        conf.set(DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY, keytab);
        conf.set(DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, principal);
        conf.set(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, "true");
        conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, "HTTPS_ONLY");
        conf.set(YarnConfiguration.RM_WEBAPP_SPNEGO_USER_NAME_KEY, principal);
        conf.set(YarnConfiguration.RM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY, keytab);
        conf.set(YarnConfiguration.NM_WEBAPP_SPNEGO_USER_NAME_KEY, principal);
        conf.set(YarnConfiguration.NM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY, keytab);
        conf.set(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, "true");
        conf.set(DFSConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication");
        conf.set(YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE, "99");
        conf.set(DFSConfigKeys.HADOOP_SECURITY_AUTH_TO_LOCAL, "RULE:[1:$1] RULE:[2:$1]");
        conf.set(String.format("hadoop.proxyuser.%s.hosts", localUser), "*");
        conf.set(String.format("hadoop.proxyuser.%s.groups", localUser), "*");
    }

    private Configuration getSecurityConfiguration(String principal, String keytab) {
        Configuration configuration = new Configuration();
        setupSSL(configuration);
        populateYarnSecureConfigurations(configuration, principal, keytab);
        return configuration;
    }

    public void startInternal(
            Configuration customConfiguration, String yarnClasspath, boolean startHdfsOperator) {
        try {
            startMiniKdc();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Configuration securityConfiguration =
                getSecurityConfiguration(hadoopServicePrincipal, keytabFile.getAbsolutePath());
        if (customConfiguration != null) {
            securityConfiguration.addResource(customConfiguration);
        }
        super.startInternal(securityConfiguration, yarnClasspath, startHdfsOperator);
    }

    @Override
    public void close() throws IOException {
        super.close();
        kdc.stop();
    }

    @Override
    public void startAndWaitForStarted(
            @Nullable Configuration customConfiguration,
            String yarnClasspath,
            boolean startHdfsOperator)
            throws ExecutionException, InterruptedException {
        this.start(null, yarnClasspath, startHdfsOperator);
        while (!hadoopClusterStartedFuture.get()) {
            Thread.sleep(1000);
        }
    }

    @Override
    protected void cleanUp() throws IOException {
        super.cleanUp();
    }
}
