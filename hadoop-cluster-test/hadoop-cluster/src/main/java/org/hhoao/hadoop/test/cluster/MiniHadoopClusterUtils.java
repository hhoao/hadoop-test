package org.hhoao.hadoop.test.cluster;

import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.VersionInfo;
import org.hhoao.hadoop.test.api.MiniHadoopCluster;

/** The type Mini hadoop cluster utils. */
public class MiniHadoopClusterUtils {
    /**
     * Start mini hadoop cluster.
     *
     * @param customHadoopConfiguration the custom hadoop configuration
     * @param yarnClasspath the yarn classpath
     * @param enableSecurity the enable security
     * @param startHdfsOperator the start hdfs operator
     * @return the mini hadoop cluster
     * @throws Exception the exception
     */
    public static MiniHadoopCluster start(
            @Nullable Configuration customHadoopConfiguration,
            @Nullable String yarnClasspath,
            boolean enableSecurity,
            boolean startHdfsOperator)
            throws Exception {
        MiniHadoopNoSecurityCluster hadoopSecurityClusterTest;

        if (enableSecurity) {
            hadoopSecurityClusterTest = new MiniHadoopSecurityCluster();
        } else {
            hadoopSecurityClusterTest = new MiniHadoopNoSecurityCluster();
        }
        hadoopSecurityClusterTest.start(
                customHadoopConfiguration, yarnClasspath, startHdfsOperator);
        if (!hadoopSecurityClusterTest.getHadoopClusterStartedFuture().get()) {
            AtomicReference<Throwable> c = new AtomicReference<>();
            hadoopSecurityClusterTest
                    .getHadoopClusterStartedFuture()
                    .exceptionally(
                            (e) -> {
                                c.set(e);
                                return null;
                            });
            throw new RuntimeException(
                    "Hadoop cluster not start, version: " + VersionInfo.getVersion(), c.get());
        }

        return hadoopSecurityClusterTest;
    }
}
