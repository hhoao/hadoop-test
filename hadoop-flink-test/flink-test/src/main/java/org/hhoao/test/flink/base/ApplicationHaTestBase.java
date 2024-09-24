package org.hhoao.test.flink.base;

import java.io.File;
import org.codehaus.commons.nullanalysis.NotNull;
import org.hhoao.hadoop.test.cluster.zookeeper.HadoopZookeeperClusterTest;
import org.hhoao.test.flink.utils.FlinkTestUtils;

/**
 * TestApplication
 *
 * @author xianxing
 * @since 2024/7/4
 */
public abstract class ApplicationHaTestBase extends HadoopZookeeperClusterTest {
    protected final File flinkRootDir;
    protected final File flinkDistPath;
    protected final File flinkLibPath;

    public ApplicationHaTestBase() {
        this.flinkRootDir = getFlinkRootDirt();
        this.flinkDistPath =
                new File(
                        flinkRootDir,
                        String.format(
                                "/flink-%s/lib/flink-dist-%s.jar",
                                FlinkTestUtils.getFlinkVersion(),
                                FlinkTestUtils.getFlinkVersion()));
        this.flinkLibPath =
                new File(
                        flinkRootDir,
                        String.format("/flink-%s/lib", FlinkTestUtils.getFlinkVersion()));
    }

    protected abstract @NotNull File getFlinkRootDirt();
}
