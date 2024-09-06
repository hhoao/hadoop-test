package org.hhoao.test.hive.test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hive.beeline.BeeLine;
import org.hhoao.hadoop.test.cluster.MiniHadoopClusterTestContext;
import org.hhoao.test.hive.base.HiveTest;
import org.junit.jupiter.api.Test;

/**
 * TestBeeline
 *
 * @author xianxing
 * @since 2024/9/6
 */
public class TestBeeline extends HiveTest {
    @Override
    protected MiniHadoopClusterTestContext getMiniHadoopClusterTestContext() {
        MiniHadoopClusterTestContext miniHadoopClusterTestContext =
                new MiniHadoopClusterTestContext();
        miniHadoopClusterTestContext.setStartHdfsOperator(false);
        return miniHadoopClusterTestContext;
    }

    @Test
    void test() throws IOException, InterruptedException {
        String url = getUrl();
        try (BeeLine beeLine = new BeeLine()) {
            beeLine.begin(new String[] {"-u", url}, System.in);
            TimeUnit.HOURS.sleep(1);
        }
    }
}
