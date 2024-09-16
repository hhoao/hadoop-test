package org.hhoao.hadoop.test.cluster;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.hhoao.hadoop.test.api.MiniHadoopCluster;
import org.hhoao.hadoop.test.api.SecurityContext;
import org.hhoao.hadoop.test.utils.ClassLoaderUtils;
import org.hhoao.hadoop.test.utils.HdfsOperator;
import org.hhoao.hadoop.test.utils.JarUtils;
import org.hhoao.hadoop.test.utils.Resources;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TestBase
 *
 * @author hhoa
 * @since 2023/5/10
 */
public class MiniHadoopNoSecurityCluster implements MiniHadoopCluster {
    private final Logger LOG = LoggerFactory.getLogger(MiniHadoopNoSecurityCluster.class);
    protected final File baseRootDir = new File(Resources.getTargetDir(), "hadoop");
    protected final File baseDir =
            new File(baseRootDir, String.valueOf(System.currentTimeMillis()));
    protected final File configDir = new File(baseDir, "config");
    protected File sslConfigDir = new File(Resources.getResourceRoot().getFile());
    protected MiniDFSCluster dfsCluster;
    protected YarnClient yarnClient;
    protected FileSystem fileSystem;
    protected Configuration config;
    protected CompletableFuture<Boolean> hadoopClusterStartedFuture = new CompletableFuture<>();
    private static final Executor executor = Executors.newSingleThreadExecutor();
    private ResourceManager resourceManager;
    private NodeManager nodeManager;
    private ExecutorService resourceManagerExecutorService;
    private ExecutorService nodeManagerExecutorService;
    private HdfsOperator hdfsOperator;

    public CompletableFuture<Boolean> getHadoopClusterStartedFuture() {
        return hadoopClusterStartedFuture;
    }

    @Override
    public FileSystem getFileSystem() {
        return fileSystem;
    }

    @Override
    public SecurityContext getSecurityContext() {
        return new NoSecurityContext();
    }

    @Override
    public MiniDFSCluster getDfsCluster() {
        return dfsCluster;
    }

    @Override
    public ResourceManager getResourceManager() {
        return resourceManager;
    }

    @Override
    public NodeManager getNodeManager() {
        return nodeManager;
    }

    @Override
    public YarnClient getYarnClient() {
        return yarnClient;
    }

    @Override
    public Configuration getConfig() {
        return config;
    }

    @BeforeEach
    @Disabled
    public void setup() throws IOException {
        cleanUp();
    }

    @Override
    public void start(
            Configuration customConfiguration, String yarnClasspath, boolean startHdfsOperator) {
        try {
            cleanUp();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        startInternal(customConfiguration, yarnClasspath, startHdfsOperator);
    }

    protected void startInternal(
            Configuration customConfiguration, String yarnClasspath, boolean startHdfsOperator) {
        executor.execute(
                () -> {
                    try {
                        baseDir.mkdirs();
                        configDir.mkdirs();
                        String hadoopNativeDir = getHadoopNativeDir();
                        ClassLoaderUtils.addLibraryDir(hadoopNativeDir);
                        LOG.info(
                                "Add library dir: {} end, native code loaded: {}",
                                hadoopNativeDir,
                                NativeCodeLoader.isNativeCodeLoaded());
                        config = getCommonConfiguration(customConfiguration, yarnClasspath);
                        UserGroupInformation.setConfiguration(config);

                        dfsCluster =
                                new MiniDFSCluster.Builder(config)
                                        .nameNodePort(
                                                getAddressPort(
                                                        config.get(
                                                                DFSConfigKeys
                                                                        .DFS_NAMENODE_RPC_ADDRESS_KEY)))
                                        .nameNodeHttpPort(
                                                getAddressPort(
                                                        config.get(
                                                                DFSConfigKeys
                                                                        .DFS_NAMENODE_HTTP_ADDRESS_KEY)))
                                        .build();

                        resourceManagerExecutorService =
                                Executors.newFixedThreadPool(
                                        1, (r) -> new Thread(r, "ResourceManager"));
                        CompletableFuture<ResourceManager> resourceManagerCompleteFuture =
                                startResourceManager(resourceManagerExecutorService, config);
                        resourceManager = resourceManagerCompleteFuture.get();

                        nodeManagerExecutorService =
                                Executors.newFixedThreadPool(
                                        1, (r) -> new Thread(r, "NodeManager"));
                        CompletableFuture<NodeManager> nodeManagerCompletableFuture =
                                startNodeManager(nodeManagerExecutorService, config);
                        nodeManager = nodeManagerCompletableFuture.get();

                        waitForNodeManagersToConnect(5000);

                        writeConfigXML(config, "yarn-site.xml");
                        writeConfigXML(config, "core-site.xml");
                        LOG.info("Cluster set down");
                        yarnClient = YarnClient.createYarnClient();
                        yarnClient.init(config);
                        yarnClient.start();
                        fileSystem = FileSystem.get(config);
                        if (startHdfsOperator) {
                            startHdfsOperator();
                        }
                        hadoopClusterStartedFuture.complete(true);
                    } catch (Exception e) {
                        hadoopClusterStartedFuture.completeExceptionally(e);
                        throw new RuntimeException(e);
                    }
                });
    }

    private void startHdfsOperator() {
        new Thread(
                        () -> {
                            hdfsOperator = new HdfsOperator(config);
                            try {
                                hdfsOperator.start();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        })
                .start();
    }

    private Integer getAddressPort(String s) {
        return Integer.valueOf(s.split(":")[1]);
    }

    private void waitForNodeManagersToConnect(long timeout)
            throws YarnException, InterruptedException {
        GetClusterMetricsRequest req = GetClusterMetricsRequest.newInstance();
        for (int i = 0; i < timeout / 10; i++) {
            ResourceManager rm = resourceManager;
            if (rm == null) {
                throw new YarnException("Can not find the active RM.");
            } else if (rm.getClientRMService()
                            .getClusterMetrics(req)
                            .getClusterMetrics()
                            .getNumNodeManagers()
                    > 0) {
                LOG.info("All Node Managers connected in MiniYARNCluster");
                return;
            }
            Thread.sleep(10);
        }
    }

    private CompletableFuture<NodeManager> startNodeManager(
            ExecutorService nodeManagerExecutorService, Configuration config) {
        return CompletableFuture.supplyAsync(
                () -> {
                    NodeManager nodeManager = new NodeManager();
                    nodeManager.init(config);
                    nodeManager.start();
                    LOG.info("NodeManager address: " + config.get(YarnConfiguration.NM_ADDRESS));
                    LOG.info(
                            "NodeManager web address: "
                                    + WebAppUtils.getNMWebAppURLWithoutScheme(config));
                    return nodeManager;
                },
                nodeManagerExecutorService);
    }

    private CompletableFuture<ResourceManager> startResourceManager(
            ExecutorService resourceManagerExecutorService, Configuration config) {

        return CompletableFuture.supplyAsync(
                () -> {
                    ResourceManager resourceManager = new ResourceManager();
                    resourceManager.init(config);
                    resourceManager.start();
                    LOG.info(
                            "ResourceManager address: " + config.get(YarnConfiguration.RM_ADDRESS));
                    LOG.info(
                            "ResourceManager web address: "
                                    + WebAppUtils.getRMWebAppURLWithoutScheme(config));
                    return resourceManager;
                },
                resourceManagerExecutorService);
    }

    private String getHadoopNativeDir() {
        File nativeDir = new File("lib/native", VersionInfo.getVersion());
        if (System.getProperty("os.name").contains("Mac")) {
            nativeDir = new File(nativeDir, "macos");
        } else {
            nativeDir = new File(nativeDir, "linux");
        }
        if (System.getProperty("os.arch").equals("aarch64")) {
            nativeDir = new File(nativeDir, "arm");
        } else {
            nativeDir = new File(nativeDir, "amd");
        }

        ProtectionDomain protectionDomain = Resources.class.getProtectionDomain();
        CodeSource codeSource = protectionDomain.getCodeSource();
        try {
            URL location = codeSource.getLocation();
            JarUtils.checkJarFile(codeSource.getLocation());
            try (JarFile jarFile = new JarFile(location.getFile())) {
                Path tempDirectory = Files.createTempDirectory("hadoop_native");
                Enumeration<JarEntry> entries = jarFile.entries();
                File root = new File(tempDirectory.toFile(), nativeDir.getPath());
                while (entries.hasMoreElements()) {
                    JarEntry jarEntry = entries.nextElement();
                    File entryFile = new File(jarEntry.getName());
                    if (jarEntry.getName().startsWith(nativeDir.getPath())
                            && !entryFile.equals(nativeDir)) {
                        InputStream resourceAsStream =
                                MiniHadoopNoSecurityCluster.class.getResourceAsStream(
                                        "/" + jarEntry.getName());
                        File file = new File(tempDirectory.toFile(), entryFile.getPath());
                        file.getParentFile().mkdirs();
                        Files.copy(resourceAsStream, file.toPath());
                    }
                }
                return root.getAbsolutePath();
            }
        } catch (IOException e) {
            return new File(Resources.getResourceForRoot(nativeDir.getPath()).getFile())
                    .getAbsolutePath();
        }
    }

    @Override
    public void startAndWaitForStarted(
            @Nullable Configuration customConfiguration,
            String yarnClasspath,
            boolean startHdfsOperator)
            throws ExecutionException, InterruptedException {
        start(customConfiguration, yarnClasspath, startHdfsOperator);
        while (!hadoopClusterStartedFuture.get()) {
            TimeUnit.SECONDS.sleep(1);
        }
    }

    protected Configuration getCommonConfiguration(
            Configuration customConfiguration, String yarnClasspath) {
        Configuration configuration = new YarnConfiguration();
        if (customConfiguration != null) {
            configuration.addResource(customConfiguration);
        }

        configuration.set(
                String.format("hadoop.proxyuser.%s.groups", System.getProperty("user.name")), "*");
        configuration.set(
                String.format("hadoop.proxyuser.%s.hosts", System.getProperty("user.name")), "*");
        configuration.setBoolean("fs.hdfs.impl.disable.cache", true);
        configuration.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
        configuration.setBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, true);
        configuration.set(YarnConfiguration.YARN_MINICLUSTER_NM_PMEM_MB, "8192");
        configuration.set(YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE, "99");
        configuration.set(YarnConfiguration.NM_VCORES, "8");
        configuration.set(YarnConfiguration.NM_PMEM_MB, "8192");
        configuration.set(YarnConfiguration.NM_VMEM_CHECK_ENABLED, "false");
        configuration.set(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, "512");
        configuration.set(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, "1");
        configuration.set("yarn.scheduler.capacity.maximum-am-resource-percent", "1");
        configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.toString());
        configuration.set(
                CommonConfigurationKeys.HADOOP_HTTP_STATIC_USER, System.getProperty("user.name"));
        if (yarnClasspath == null) {
            configuration.set(
                    YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                    System.getProperty("java.class.path"));
        } else {
            configuration.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH, yarnClasspath);
        }
        configuration.set(YarnConfiguration.NM_CLIENT_MAX_NM_PROXIES, "500");
        configuration.set("yarn.client.max-nodemanagers-proxies", "500");
        String localDirsString = prepareDirs("", 1);
        configuration.set(YarnConfiguration.NM_LOCAL_DIRS, localDirsString);
        String logDirsString = prepareDirs("nm-log-dirs", 1);
        configuration.set(YarnConfiguration.NM_LOG_DIRS, logDirsString);

        boolean useFixedPorts =
                configuration.getBoolean(
                        YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS,
                        YarnConfiguration.DEFAULT_YARN_MINICLUSTER_FIXED_PORTS);

        try {
            if (!useFixedPorts) {
                setNonHARMConfigurationWithEphemeralPorts(configuration);
            }
        } catch (IOException e) {
            throw new RuntimeException("Get address error", e);
        }
        return configuration;
    }

    private String prepareDirs(String dirType, int numDirs) {
        File[] dirs = new File[numDirs];
        String dirsString = "";
        for (int i = 0; i < numDirs; i++) {
            dirs[i] = new File(baseDir, dirType + "Dir-nm-" + 0 + "_" + i);
            LOG.info("Created " + dirType + "Dir in " + dirs[i].getAbsolutePath());
            String delimiter = (i > 0) ? "," : "";
            dirsString = dirsString.concat(delimiter + dirs[i].getAbsolutePath());
        }
        return dirsString;
    }

    private void writeConfigXML(Configuration yarnConf, String filename) throws IOException {
        File yarnSiteXML = new File(configDir, filename);
        try (FileWriter writer = new FileWriter(yarnSiteXML)) {
            yarnConf.writeXml(writer);
            writer.flush();
        }
    }

    protected void cleanUp() throws IOException {
        FileUtils.deleteDirectory(baseRootDir);
        Files.deleteIfExists(new File(sslConfigDir, "ssl-client.xml").toPath());
        Files.deleteIfExists(new File(sslConfigDir, "ssl-server.xml").toPath());
    }

    @Override
    public void close() throws IOException {
        if (hdfsOperator != null) {
            hdfsOperator.close();
        }
        if (yarnClient != null) {
            yarnClient.close();
        }
        if (nodeManager != null) {
            nodeManager.close();
            nodeManagerExecutorService.shutdownNow();
        }
        if (resourceManager != null) {
            resourceManager.close();
            resourceManagerExecutorService.shutdownNow();
        }
        if (dfsCluster != null) {
            dfsCluster.shutdown();
        }
    }

    private void setNonHARMConfigurationWithEphemeralPorts(Configuration conf) throws IOException {
        List<String> needSetAddressKey = new ArrayList<>();
        needSetAddressKey.add(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY);
        needSetAddressKey.add(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
        needSetAddressKey.add(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY);
        needSetAddressKey.add(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY);
        needSetAddressKey.add(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTPS_ADDRESS_KEY);
        needSetAddressKey.add(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY);
        needSetAddressKey.add(DFSConfigKeys.DFS_NAMENODE_BACKUP_ADDRESS_KEY);
        needSetAddressKey.add(DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY);
        needSetAddressKey.add(DFSConfigKeys.DFS_NAMENODE_BACKUP_SERVICE_RPC_ADDRESS_KEY);
        needSetAddressKey.add(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY);
        needSetAddressKey.add(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY);
        needSetAddressKey.add(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY);
        needSetAddressKey.add(DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY);
        needSetAddressKey.add(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY);
        needSetAddressKey.add(DFSConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_KEY);
        needSetAddressKey.add(DFSConfigKeys.DFS_JOURNALNODE_RPC_ADDRESS_KEY);
        needSetAddressKey.add(DFSConfigKeys.DFS_JOURNALNODE_HTTPS_ADDRESS_KEY);
        needSetAddressKey.add(YarnConfiguration.RM_ADDRESS);
        needSetAddressKey.add(YarnConfiguration.NM_LOCALIZER_ADDRESS);
        needSetAddressKey.add(YarnConfiguration.RM_ADMIN_ADDRESS);
        needSetAddressKey.add(YarnConfiguration.RM_SCHEDULER_ADDRESS);
        needSetAddressKey.add(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS);
        needSetAddressKey.add(YarnConfiguration.TIMELINE_SERVICE_ADDRESS);
        needSetAddressKey.add(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS);
        needSetAddressKey.add(JHAdminConfig.MR_HISTORY_ADDRESS);
        needSetAddressKey.add(JHAdminConfig.JHS_ADMIN_ADDRESS);
        needSetAddressKey.add(JHAdminConfig.MR_HISTORY_WEBAPP_ADDRESS);
        needSetAddressKey.add(JHAdminConfig.MR_HISTORY_WEBAPP_HTTPS_ADDRESS);
        if (YarnConfiguration.useHttps(conf)) {
            needSetAddressKey.add(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS);
            needSetAddressKey.add(YarnConfiguration.NM_WEBAPP_HTTPS_ADDRESS);
        } else {
            needSetAddressKey.add(YarnConfiguration.RM_WEBAPP_ADDRESS);
            needSetAddressKey.add(YarnConfiguration.NM_WEBAPP_ADDRESS);
        }

        ServerSocket[] sockets = new ServerSocket[needSetAddressKey.size()];
        for (int i = 0; i < sockets.length; i++) {
            ServerSocket serverSocket = new ServerSocket(0);
            sockets[i] = serverSocket;
            LOG.info(
                    "Set {} {}",
                    needSetAddressKey.get(i),
                    getLocalAddress(serverSocket.getLocalPort()));
            conf.set(needSetAddressKey.get(i), getLocalAddress(serverSocket.getLocalPort()));
        }
        for (ServerSocket socket : sockets) {
            socket.close();
        }
    }

    private String getLocalAddress(int port) {
        return "127.0.0.1" + ":" + port;
    }
}
