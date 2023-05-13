package org.hhoao.test.flink;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.*;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptionsInternal;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.hhoao.hadoop.test.utils.YarnUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YarnFlinkTest {
    private static final Logger LOG = LoggerFactory.getLogger(YarnFlinkTest.class);
    private ClusterClient<ApplicationId> clusterClient;
    private ClusterSpecification clusterSpecification;
    private YarnClusterDescriptor yarnClusterDescriptor;
    private final List<String> providedFlinkLibDirs;
    private final String flinkDistPath;
    private final List<String> pipelineJarsPaths;
    private final URL logConfURL = getClass().getResource("/flink/config/log4j.properties");

    public YarnFlinkTest(
            List<String> providedFlinkLibDirs,
            String flinkDistPath,
            List<String> pipelineJarsPaths) {
        this.providedFlinkLibDirs = providedFlinkLibDirs;
        this.flinkDistPath = flinkDistPath;
        this.pipelineJarsPaths = pipelineJarsPaths;
    }

    public void start(
            org.apache.hadoop.conf.Configuration hadoopConfiguration,
            @Nullable Configuration customFlinkConfiguration,
            YarnDeploymentTarget deploymentTarget)
            throws Throwable {
        start(hadoopConfiguration, customFlinkConfiguration, deploymentTarget, null, null);
    }

    public void start(
            org.apache.hadoop.conf.Configuration configuration,
            @Nullable Configuration customFlinkConfiguration,
            YarnDeploymentTarget deploymentTarget,
            String principal,
            String keytab)
            throws Throwable {
        if (principal != null && keytab != null) {
            UserGroupInformation.setConfiguration(configuration);
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
            UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
            currentUser.doAs(
                    (PrivilegedAction<Void>)
                            () -> {
                                try {
                                    startInternal(
                                            customFlinkConfiguration,
                                            configuration,
                                            deploymentTarget);
                                } catch (Throwable e) {
                                    throw new RuntimeException(e);
                                }
                                return null;
                            });
        } else {
            startInternal(customFlinkConfiguration, configuration, deploymentTarget);
        }
    }

    private YarnClient getYarnClient(org.apache.hadoop.conf.Configuration configuration) {
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(configuration);
        yarnClient.start();
        return yarnClient;
    }

    private void startInternal(
            Configuration customFlinkConfiguration,
            org.apache.hadoop.conf.Configuration hadoopConfiguration,
            YarnDeploymentTarget deploymentTarget)
            throws Throwable {
        YarnClient yarnClient = getYarnClient(hadoopConfiguration);
        Configuration configuration = new Configuration();
        if (customFlinkConfiguration != null) {
            configuration.addAll(customFlinkConfiguration);
        }
        init(configuration, hadoopConfiguration);
        try {
            switch (deploymentTarget) {
                case SESSION:
                    startSession(yarnClient, configuration, hadoopConfiguration);
                    break;
                case APPLICATION:
                    startApplication(yarnClient, configuration, hadoopConfiguration);
                    break;
                default:
                    throw new RuntimeException();
            }
        } catch (ClusterDeploymentException e) {
            String appIdStr = configuration.get(HighAvailabilityOptions.HA_CLUSTER_ID);
            String applicationLog = null;
            try {
                String[] parts = appIdStr.substring(12).split("_");
                if (parts.length != 2) {
                    throw new IllegalArgumentException("Invalid ApplicationId format");
                }

                long timestamp = Long.parseLong(parts[0]);
                int id = Integer.parseInt(parts[1]);

                ApplicationId applicationId = ApplicationId.newInstance(timestamp, id);
                applicationLog = YarnUtils.getApplicationLog(hadoopConfiguration, applicationId);
            } catch (Exception e1) {
                LOG.warn("Can't get application log, current HA_CLUSTER_ID: {}", appIdStr);
            }
            if (applicationLog != null) {
                throw new RuntimeException(applicationLog, e);
            } else {
                throw e;
            }
        }
    }

    private void init(
            Configuration configuration, org.apache.hadoop.conf.Configuration hadoopConfiguration)
            throws IOException {
        for (Map.Entry<String, String> stringStringEntry : hadoopConfiguration) {
            if (stringStringEntry.getKey().startsWith("yarn.")) {
                configuration.setString(
                        "flink." + stringStringEntry.getKey(), stringStringEntry.getValue());
            }
        }
        setIfAbsent(configuration, YarnConfigOptions.PROVIDED_LIB_DIRS, providedFlinkLibDirs);
        setIfAbsent(
                configuration, JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1024m"));
        setIfAbsent(
                configuration, TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1024m"));
        setIfAbsent(configuration, YarnConfigOptions.FLINK_DIST_JAR, flinkDistPath);
        if (logConfURL != null) {
            setIfAbsent(
                    configuration,
                    YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE,
                    logConfURL.getPath());
        }
        FileSystem fileSystem = FileSystem.get(hadoopConfiguration);
        for (String dir : providedFlinkLibDirs) {
            File file = new File(dir);
            Files.walkFileTree(
                    file.toPath(),
                    new SimpleFileVisitor<java.nio.file.Path>() {
                        @Override
                        public FileVisitResult visitFile(
                                java.nio.file.Path path, BasicFileAttributes attrs) {
                            File file = path.toFile();
                            Path parent = new Path(file.getParent());
                            try {
                                fileSystem.mkdirs(parent);
                                fileSystem.copyFromLocalFile(
                                        false, true, new Path(file.getAbsolutePath()), parent);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                            return FileVisitResult.CONTINUE;
                        }
                    });
        }
    }

    private void startSession(
            YarnClient yarnClient,
            Configuration configuration,
            org.apache.hadoop.conf.Configuration hadoopConfiguration)
            throws ClusterDeploymentException {
        YarnClientYarnClusterInformationRetriever yarnClientYarnClusterInformationRetriever =
                YarnClientYarnClusterInformationRetriever.create(yarnClient);

        setIfAbsent(
                configuration, DeploymentOptions.TARGET, YarnDeploymentTarget.SESSION.getName());

        YarnClusterDescriptor yarnClusterDescriptor =
                new YarnClusterDescriptor(
                        configuration,
                        new YarnConfiguration(hadoopConfiguration),
                        yarnClient,
                        yarnClientYarnClusterInformationRetriever,
                        true);

        ClusterSpecification clusterSpecification =
                new ClusterSpecification.ClusterSpecificationBuilder()
                        .setMasterMemoryMB(1024)
                        .setTaskManagerMemoryMB(1024)
                        .setSlotsPerTaskManager(1)
                        .createClusterSpecification();

        ClusterClientProvider<ApplicationId> applicationIdClusterClientProvider =
                yarnClusterDescriptor.deploySessionCluster(clusterSpecification);
        clusterClient = applicationIdClusterClientProvider.getClusterClient();
    }

    private void startApplication(
            YarnClient yarnClient,
            Configuration configuration,
            org.apache.hadoop.conf.Configuration hadoopConfiguration)
            throws ClusterDeploymentException {
        setIfAbsent(configuration, PipelineOptions.JARS, pipelineJarsPaths);
        setIfAbsent(
                configuration,
                DeploymentOptions.TARGET,
                YarnDeploymentTarget.APPLICATION.getName());
        YarnConfiguration yarnConfiguration = new YarnConfiguration(hadoopConfiguration);
        YarnClientYarnClusterInformationRetriever yarnClientYarnClusterInformationRetriever =
                YarnClientYarnClusterInformationRetriever.create(yarnClient);

        yarnClusterDescriptor =
                new YarnClusterDescriptor(
                        configuration,
                        yarnConfiguration,
                        yarnClient,
                        yarnClientYarnClusterInformationRetriever,
                        true);

        clusterSpecification =
                new ClusterSpecification.ClusterSpecificationBuilder()
                        .setSlotsPerTaskManager(1)
                        .createClusterSpecification();

        ApplicationConfiguration applicationConfiguration =
                ApplicationConfiguration.fromConfiguration(configuration);
        ClusterClientProvider<ApplicationId> applicationIdClusterClientProvider =
                yarnClusterDescriptor.deployApplicationCluster(
                        clusterSpecification, applicationConfiguration);
        clusterClient = applicationIdClusterClientProvider.getClusterClient();
    }

    public ClusterClient<ApplicationId> getClusterClient() {
        return clusterClient;
    }

    public YarnClusterDescriptor getYarnClusterDescriptor() {
        return yarnClusterDescriptor;
    }

    public ClusterSpecification getClusterSpecification() {
        return clusterSpecification;
    }

    private <T> void setIfAbsent(
            Configuration configuration, ConfigOption<T> configOption, T value) {
        if (!configuration.containsKey(configOption.key())) {
            configuration.set(configOption, value);
        }
    }
}
