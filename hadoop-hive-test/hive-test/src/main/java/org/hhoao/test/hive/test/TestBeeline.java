package org.hhoao.test.hive.test;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.beeline.BeeLine;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
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
    void test() {
        changeLogger();
        String url = getUrl();
        UserGroupInformation w = UserGroupInformation.createRemoteUser("w");
        w.doAs(
                (PrivilegedAction<?>)
                        () -> {
                            try (BeeLine beeLine = new BeeLine()) {
                                beeLine.begin(new String[] {"-u", url, "-n", "w"}, System.in);
                                TimeUnit.HOURS.sleep(1);
                            } catch (InterruptedException | IOException e) {
                                throw new RuntimeException(e);
                            }
                            return null;
                        });
    }

    private void changeLogger() {
        Configurator.setRootLevel(Level.OFF);
        org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
        rootLogger.setLevel(org.apache.log4j.Level.OFF);
    }
}
