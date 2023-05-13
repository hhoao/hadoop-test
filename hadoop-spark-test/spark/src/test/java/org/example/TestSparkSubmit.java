package org.example;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkSubmit;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.hhoao.hadoop.test.cluster.MiniHadoopClusterExtension;
import org.hhoao.hadoop.test.utils.Resources;
import org.hhoao.hadoop.test.utils.YarnUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * TestSparkSubmit
 *
 * @author hhoa
 * @since 2023/5/8
 */
public class TestSparkSubmit {
    protected static final File SPARK_JARS_PATH =
            new File("/Users/w/applications/spark-3.5.1-bin-hadoop3/jars");
    protected static final String RUN_JAR_PATH =
            new File(Resources.getResourceRoot().getPath(), "spark-examples-1.0.0-SNAPSHOT.jar")
                    .getAbsolutePath();
    protected static final String RUN_CLASS = "FilterSpaceString";
    protected static final String RUN_ARG = "zhangsan 'li si' 'xiaoming'";

    @RegisterExtension
    private static final MiniHadoopClusterExtension hadoopClusterExtension =
            new MiniHadoopClusterExtension(
                    false, true, getHadoopClassPath(), getCustomHadoopConfiguration());

    private static org.apache.hadoop.conf.Configuration getCustomHadoopConfiguration() {
        org.apache.hadoop.conf.Configuration configuration =
                new org.apache.hadoop.conf.Configuration();
        configuration.set(YarnConfiguration.NM_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS, "30000");
        configuration.set(YarnConfiguration.NM_LOCALIZER_CACHE_TARGET_SIZE_MB, "0");
        return configuration;
    }

    private static String getHadoopClassPath() {
        String property = System.getProperty("java.class.path");
        StringBuilder classPathBuilder = new StringBuilder();
        Arrays.stream(property.split(":"))
                .forEach(
                        (s) -> {
                            String jar = s.substring(s.lastIndexOf('/'));
                            if (!jar.startsWith("/spark") || jar.startsWith("/spark-yarn")) {
                                classPathBuilder.append(s).append(":");
                            }
                        });
        return classPathBuilder.toString();
    }

    @Test
    public void testSparkSubmit() throws IOException, YarnException {
        System.setProperty("spark.testing", "true");
        System.setProperty("HADOOP_CONF_DIR", "true");
        SparkSubmit sparkSubmit = new SparkSubmit();
        FileSystem fileSystem = hadoopClusterExtension.getHadoopCluster().getFileSystem();
        YarnClient yarnClient = hadoopClusterExtension.getHadoopCluster().getYarnClient();
        Path localJarsPath = new Path(SPARK_JARS_PATH.getAbsolutePath());
        Path remoteJarsPath = new Path("/spark/jars");
        fileSystem.copyFromLocalFile(localJarsPath, remoteJarsPath);
        String[] sparkArgs =
                new String[] {
                    "--master",
                    "yarn",
                    "--deploy-mode",
                    "cluster",
                    "--driver-memory",
                    "2g",
                    "--executor-memory",
                    "1g",
                    "--executor-cores",
                    "1",
                    "--conf",
                    "spark.yarn.am.retryInterval=1s",
                    "--conf",
                    "spark.yarn.jars=" + remoteJarsPath,
                    "--conf",
                    "spark.yarn.archive=" + remoteJarsPath,
                    "--verbose",
                    "--class",
                    RUN_CLASS,
                    RUN_JAR_PATH,
                    RUN_ARG,
                };

        sparkSubmit.doSubmit(sparkArgs);
        List<ApplicationReport> applications = yarnClient.getApplications();
        ApplicationReport applicationReport = applications.get(0);
        Assertions.assertEquals(
                FinalApplicationStatus.SUCCEEDED, applicationReport.getFinalApplicationStatus());
    }

    @Test
    public void testSparkClient() throws Throwable {
        FileSystem fileSystem = hadoopClusterExtension.getHadoopCluster().getFileSystem();
        ClientArguments clientArgs =
                new ClientArguments(
                        new String[] {
                            "--class", RUN_CLASS,
                            "--jar", RUN_JAR_PATH,
                            "--arg", RUN_ARG,
                        });

        Path localJarsPath = new Path(SPARK_JARS_PATH.getAbsolutePath());
        Path remoteJarsPath = new Path("/spark/jars");
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.testing", "true");
        sparkConf.set("spark.rpc.askTimeout", "10s");
        sparkConf.set(
                "spark.yarn.archive", fileSystem.makeQualified(remoteJarsPath).toUri().toString());
        sparkConf.set("spark.submitTime", String.valueOf(System.currentTimeMillis()));
        sparkConf.set("spark.app.name", "spark-pi");
        sparkConf.set("spark.executor.cores", "1");
        sparkConf.set("spark.submit.deployMode", "cluster");
        sparkConf.set("spark.submit.pyFiles", "");
        sparkConf.set("spark.driver.memory", "4g");
        sparkConf.set("spark.master", "yarn");
        sparkConf.set("spark.executor.memory", "2g");
        Configuration config = hadoopClusterExtension.getHadoopCluster().getConfig();
        for (Map.Entry<String, String> entry : config) {
            sparkConf.set("spark.hadoop." + entry.getKey(), entry.getValue());
        }
        fileSystem.copyFromLocalFile(localJarsPath, remoteJarsPath);
        Client sparkClient = new Client(clientArgs, sparkConf, null);
        try {
            sparkClient.run();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            ApplicationId applicationId = sparkClient.getApplicationId();
            String applicationLog = YarnUtils.getApplicationLog(config, applicationId);
            System.out.println(applicationLog);
        } catch (Exception e) {
            e.printStackTrace();
        }
        TimeUnit.HOURS.sleep(1);
    }
}
