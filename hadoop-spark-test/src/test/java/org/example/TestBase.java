package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.example.utils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * TestBase
 *
 * @author hhoa
 * @since 2023/5/10
 **/

public class TestBase {
    protected final static String RESOURCE_DIR = ClassLoader.getSystemClassLoader().getResource("").getPath();
    protected final static String ROOT_DIR = new File(RESOURCE_DIR).getParent() + "/";
    protected final static String PARENT_PROJECT_ROOT_DIR = new File(RESOURCE_DIR).getParentFile().getParentFile().getParentFile() + "/";
    protected final static String LOCAL_JARS_DIR = new Path(PARENT_PROJECT_ROOT_DIR, "data/jars").toString();
    protected final static String HDFS_JARS_PATH = "/tmp/jars";
    protected final static String RUN_JAR_PATH = new File(RESOURCE_DIR, "spark-examples_2.12-3.3.1.jar").getAbsolutePath();
    protected final static String RUN_CLASS = "org.apache.spark.examples.SparkPi";
    protected final static String RUN_ARG = "10";
    private static final Logger LOG = LoggerFactory.getLogger(TestClient.class);
    protected static MiniYARNCluster yarnCluster;
    protected static MiniDFSCluster dfsCluster;
    protected static YarnClient yarnClient;
    protected static FileSystem fileSystem;
    protected static Configuration config;

    @BeforeClass
    public static void test() throws IOException, InterruptedException, YarnException {
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
        writeConfig("yarn-site.xml");
        writeConfig("core-site.xml");
        LOG.info("Cluster set down");
    }

    public static void writeConfig(String configName) {
        File coreSite = new File(RESOURCE_DIR, configName);
        try (FileWriter writer = new FileWriter(coreSite)) {
            config.writeXml(writer);
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected static void cleanUp() {
        FileUtils.deleteFile(new File(ROOT_DIR, "test").getAbsoluteFile());
        FileUtils.deleteFile(new File(RESOURCE_DIR, "yarn-site.xml").getAbsoluteFile());
        FileUtils.deleteFile(new File(RESOURCE_DIR, "core-site.xml").getAbsoluteFile());
    }

    @Before
    public void beforeSparkTest() throws IOException {
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(config);
        yarnClient.start();

        File jars = new File(LOCAL_JARS_DIR);
        if (!jars.exists()) {
            LOG.info("you need to copy your spark jars dirs to " + LOCAL_JARS_DIR);
        }
        boolean isHasPackage = Arrays.stream(Objects.requireNonNull(jars.list())).anyMatch((file)
                -> file.matches("hadoop-common-(\\d*|.)*-tests.jar"));
        if (!isHasPackage) {
            LOG.error("please upload hadoop-common-3.y.z-tests.jar package");
            throw new RuntimeException("please upload spark-tags_2.x-3.y.z-tests.jar package");
        }
        System.setProperty("spark.testing", "true");
        fileSystem = FileSystem.get(config);
//        fileSystem.copyFromLocalFile(new Path(LOCAL_JARS_DIR), new Path("/tmp/jars"));
    }

    @After
    public void afterEach() throws IOException {
        dfsCluster.close();
        yarnCluster.close();
        yarnClient.close();
    }
}
