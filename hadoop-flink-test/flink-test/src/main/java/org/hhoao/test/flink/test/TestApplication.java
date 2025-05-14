package org.hhoao.test.flink.test;

import java.io.File;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.hhoao.hadoop.test.cluster.MiniHadoopClusterTestContext;
import org.hhoao.test.flink.base.ApplicationTestBase;
import org.hhoao.test.flink.base.YarnFlinkTest;
import org.hhoao.test.flink.utils.FlinkTestUtils;
import org.junit.jupiter.api.Test;

/**
 * TestApplication
 *
 * @author xianxing
 * @since 2024/8/28
 */
public class TestApplication extends ApplicationTestBase {
    @Override
    protected File getFlinkRootDirt() {
        return new File(System.getProperty("user.home") + "/applications");
    }

    @Override
    protected MiniHadoopClusterTestContext getMiniHadoopClusterTestContext() {
        MiniHadoopClusterTestContext miniHadoopClusterTestContext =
                new MiniHadoopClusterTestContext();
        miniHadoopClusterTestContext.setClasspath(FlinkTestUtils.getFlinkHadoopClassPath());
        org.apache.hadoop.conf.Configuration customHadoopConfiguration =
                getCustomHadoopConfiguration();
        miniHadoopClusterTestContext.setConfiguration(customHadoopConfiguration);
        return miniHadoopClusterTestContext;
    }

    public org.apache.hadoop.conf.Configuration getCustomHadoopConfiguration() {
        org.apache.hadoop.conf.Configuration configuration =
                new org.apache.hadoop.conf.Configuration();
        configuration.set(YarnConfiguration.NM_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS, "30000");
        configuration.set(YarnConfiguration.NM_LOCALIZER_CACHE_TARGET_SIZE_MB, "0");
        return configuration;
    }

    @Test
    public void testCheckpoint() throws Throwable {
        Tuple2<List<String>, String> defaultPipelineJarsPathsAndMainClass =
                FlinkTestUtils.getDefaultPipelineJarsPathsAndMainClass();
        int count = 1;
        ArrayList<YarnFlinkTest> yarnFlinkTests = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            List<String> flinkLibDirs = new ArrayList<>();
            flinkLibDirs.add(flinkLibPath.getAbsolutePath());
            YarnFlinkTest yarnFlinkTest =
                    new YarnFlinkTest(
                            flinkLibDirs,
                            flinkDistPath.getAbsolutePath(),
                            defaultPipelineJarsPathsAndMainClass.f0);
            Configuration configuration = new Configuration();
            configuration.set(ApplicationConfiguration.APPLICATION_ARGS, new ArrayList<>());
            configuration.set(CoreOptions.DEFAULT_PARALLELISM, 4);
            configuration.set(TaskManagerOptions.NUM_TASK_SLOTS, 1);
            configuration.set(
                    ApplicationConfiguration.APPLICATION_MAIN_CLASS,
                    defaultPipelineJarsPathsAndMainClass.f1);
            FileSystem fileSystem = getHadoopCluster().getFileSystem();
            Path homeDirectory = fileSystem.getHomeDirectory();
            configuration.set(
                    ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,
                    Duration.of(30, ChronoUnit.SECONDS));
            configuration.set(
                    CheckpointingOptions.SAVEPOINT_DIRECTORY,
                    Path.mergePaths(homeDirectory, new Path("/checkpoints")).toString());
            configuration.set(
                    CheckpointingOptions.SAVEPOINT_DIRECTORY,
                    Path.mergePaths(homeDirectory, new Path("/savepoints")).toString());
            FlinkTestUtils.setJobManagerDebugProperty(configuration, 31231, true);
            yarnFlinkTest.start(
                    getHadoopCluster().getConfig(),
                    configuration,
                    YarnDeploymentTarget.APPLICATION);
            yarnFlinkTests.add(yarnFlinkTest);
        }
        YarnFlinkTest yarnFlinkTest = yarnFlinkTests.get(0);
        ClusterClient<ApplicationId> clusterClient = yarnFlinkTest.getClusterClient();
        Collection<JobStatusMessage> jobStatusMessages = clusterClient.listJobs().get();
        TimeUnit.SECONDS.sleep(5);
        for (JobStatusMessage jobStatusMessage : jobStatusMessages) {
            JobID jobId = jobStatusMessage.getJobId();
            yarnFlinkTest.getClusterClient().cancel(jobId);
        }

        TimeUnit.HOURS.sleep(2);
    }

    @Test
    public void testCancelJob() throws Throwable {
        Tuple2<List<String>, String> defaultPipelineJarsPathsAndMainClass =
                FlinkTestUtils.getDefaultPipelineJarsPathsAndMainClass();
        int count = 1;
        ArrayList<YarnFlinkTest> yarnFlinkTests = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            List<String> flinkLibDirs = new ArrayList<>();
            flinkLibDirs.add(flinkLibPath.getAbsolutePath());
            YarnFlinkTest yarnFlinkTest =
                    new YarnFlinkTest(
                            flinkLibDirs,
                            flinkDistPath.getAbsolutePath(),
                            defaultPipelineJarsPathsAndMainClass.f0);
            Configuration configuration = new Configuration();
            configuration.set(ApplicationConfiguration.APPLICATION_ARGS, new ArrayList<>());
            configuration.set(CoreOptions.DEFAULT_PARALLELISM, 4);
            configuration.set(TaskManagerOptions.NUM_TASK_SLOTS, 1);
            configuration.set(
                    ApplicationConfiguration.APPLICATION_MAIN_CLASS,
                    defaultPipelineJarsPathsAndMainClass.f1);
            yarnFlinkTest.start(
                    getHadoopCluster().getConfig(),
                    configuration,
                    YarnDeploymentTarget.APPLICATION);
            yarnFlinkTests.add(yarnFlinkTest);
        }
        YarnFlinkTest yarnFlinkTest = yarnFlinkTests.get(0);
        ClusterClient<ApplicationId> clusterClient = yarnFlinkTest.getClusterClient();
        Collection<JobStatusMessage> jobStatusMessages = clusterClient.listJobs().get();
        TimeUnit.SECONDS.sleep(5);
        for (JobStatusMessage jobStatusMessage : jobStatusMessages) {
            JobID jobId = jobStatusMessage.getJobId();
            yarnFlinkTest.getClusterClient().cancel(jobId);
        }

        TimeUnit.HOURS.sleep(2);
    }
}
