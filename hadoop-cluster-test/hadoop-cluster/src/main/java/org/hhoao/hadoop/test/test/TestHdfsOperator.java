package org.hhoao.hadoop.test.test;

import java.util.concurrent.TimeUnit;
import org.hhoao.hadoop.test.cluster.MiniHadoopNoSecurityCluster;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class TestHdfsOperator {
    @Test
    @Disabled
    public void test() throws Exception {
        try (MiniHadoopNoSecurityCluster hadoopClusterTest = new MiniHadoopNoSecurityCluster()) {
            hadoopClusterTest.startAndWaitForStarted(null, null, true);
            TimeUnit.HOURS.sleep(2);
        }
    }
}
