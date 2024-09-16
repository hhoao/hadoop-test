package org.hhoao.test.hive.test;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.beeline.BeeLine;
import org.hhoao.hadoop.test.api.SecurityContext;
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
        miniHadoopClusterTestContext.setEnableSecurity(true);
        return miniHadoopClusterTestContext;
    }

    @Test
    void test() throws IOException {
        LoggerUtils.changeAppendAllLogToFile();
        SecurityContext securityContext = getHadoopCluster().getSecurityContext();
        UserGroupInformation ugi = securityContext.getDefaultUGI();
        ugi.doAs(
                (PrivilegedAction<?>)
                        () -> {
                            try (BeeLine beeLine = new BeeLine()) {
                                beeLine.begin(new String[] {"-u", url}, System.in);
                                TimeUnit.HOURS.sleep(1);
                            } catch (InterruptedException | IOException e) {
                                throw new RuntimeException(e);
                            }
                            return null;
                        });
    }
}
