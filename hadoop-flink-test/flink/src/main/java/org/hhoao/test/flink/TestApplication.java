package org.hhoao.test.flink;

import java.io.File;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.hhoao.hadoop.test.cluster.MiniHadoopClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * TestApplication
 *
 * @author xianxing
 * @since 2024/7/4
 */
public class TestApplication {
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
                            if (!jar.startsWith("/flink") || jar.startsWith("/flink-yarn")) {
                                classPathBuilder.append(s).append(":");
                            }
                        });
        return classPathBuilder.toString();
    }

    public TestApplication() {
        super();
    }

    @Test
    public void test() throws Throwable {
        int count = 1;
        for (int i = 0; i < count; i++) {
            List<String> pipelineJarsPaths =
                    Collections.singletonList(
                            Arrays.stream(System.getProperty("java.class.path").split(":"))
                                    .filter(s -> s.contains("flink-examples-streaming"))
                                    .filter(s -> s.contains("1.16.2"))
                                    .findAny()
                                    .get());
            List<String> flinkLibDirs = new ArrayList<>();
            File file = new File("/Users/w/applications/flink-1.16.2/lib");
            Files.walkFileTree(
                    file.toPath(),
                    new SimpleFileVisitor<Path>() {
                        public FileVisitResult visitFile(
                                java.nio.file.Path file, BasicFileAttributes attrs) {
                            flinkLibDirs.add(file.toString());
                            return FileVisitResult.CONTINUE;
                        }
                    });
            YarnFlinkTest yarnFlinkTest =
                    new YarnFlinkTest(
                            flinkLibDirs,
                            "/Users/w/applications/flink-1.16.2/lib/flink-dist-1.16.2.jar",
                            pipelineJarsPaths);
            Configuration configuration = new Configuration();
            configuration.set(ApplicationConfiguration.APPLICATION_ARGS, new ArrayList<>());
            configuration.set(CoreOptions.DEFAULT_PARALLELISM, 2);
            configuration.set(
                    ApplicationConfiguration.APPLICATION_MAIN_CLASS,
                    "org.apache.flink.streaming.examples.join.WindowJoin");
            yarnFlinkTest.start(
                    hadoopClusterExtension.getHadoopCluster().getConfig(),
                    configuration,
                    YarnDeploymentTarget.APPLICATION);
        }
        TimeUnit.HOURS.sleep(2);
    }
}
