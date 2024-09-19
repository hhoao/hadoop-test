package org.hhoao.test.flink.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;

/**
 * TestUtils
 *
 * @author xianxing
 * @since 2024/8/28
 */
public class FlinkTestUtils {
    private static String flinkVersion = getFlinkVersion();

    public static void setJobManagerDebugProperty(
            Configuration flinkConfig, int port, boolean suspended) {
        String orDefault = flinkConfig.get(CoreOptions.FLINK_JM_JVM_OPTIONS);
        flinkConfig.set(
                CoreOptions.FLINK_JM_JVM_OPTIONS,
                String.format(
                        "%s -agentlib:jdwp=transport=dt_socket,server=y,suspend=%s,address=%s",
                        orDefault, suspended ? "y" : "n", port));
    }

    public static void setTaskManagerDebugProperty(
            Configuration flinkConfig, int port, boolean suspended) {
        String orDefault = flinkConfig.get(CoreOptions.FLINK_TM_JVM_OPTIONS);
        flinkConfig.set(
                CoreOptions.FLINK_TM_JVM_OPTIONS,
                String.format(
                        "%s -agentlib:jdwp=transport=dt_socket,server=y,suspend=%s,address=%s",
                        orDefault, suspended ? "y" : "n", port));
    }

    public static String getFlinkVersion() {
        if (flinkVersion == null) {
            String flinkCore =
                    Arrays.stream(System.getProperty("java.class.path").split(":"))
                            .filter(s -> s.contains("flink-core"))
                            .findAny()
                            .get();
            String[] split = flinkCore.split("/");
            flinkVersion = split[split.length - 2];
        }
        return flinkVersion;
    }

    public static String getFlinkHadoopClassPath() {
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

    public static Tuple2<List<String>, String> getDefaultPipelineJarsPathsAndMainClass() {
        return Tuple2.of(
                Collections.singletonList(
                        Arrays.stream(System.getProperty("java.class.path").split(":"))
                                .filter(s -> s.contains("flink-examples-streaming"))
                                .findAny()
                                .get()),
                "org.apache.flink.streaming.examples.join.WindowJoin");
    }
}
