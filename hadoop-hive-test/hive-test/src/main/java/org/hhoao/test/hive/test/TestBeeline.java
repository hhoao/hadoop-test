package org.hhoao.test.hive.test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hive.beeline.BeeLine;
import org.hhoao.hadoop.test.cluster.MiniHadoopClusterTestContext;
import org.hhoao.hadoop.test.utils.LoggerUtils;
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
    void test() throws IOException {
        LoggerUtils.changeAppendAllLogToFile();
        String url = getUrl();
        try (BeeLine beeLine = new BeeLine()) {
            String user = System.getProperty("user.name");
            beeLine.begin(new String[] {"-u", url, "-n", user}, System.in);
            TimeUnit.HOURS.sleep(1);
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
