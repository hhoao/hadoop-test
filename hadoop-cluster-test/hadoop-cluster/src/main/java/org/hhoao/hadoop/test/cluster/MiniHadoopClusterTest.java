package org.hhoao.hadoop.test.cluster;

import org.apache.hadoop.conf.Configuration;
import org.hhoao.hadoop.test.api.MiniHadoopCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class MiniHadoopClusterTest {
    private MiniHadoopCluster hadoopClusterTest;
    private final boolean enableSecurity;
    private final boolean startHdfsOperator;
    private final String classpath;
    private final Configuration configuration;

    public MiniHadoopClusterTest() {
        MiniHadoopClusterTestContext miniHadoopClusterTestContext =
                getMiniHadoopClusterTestContext();
        this.enableSecurity = miniHadoopClusterTestContext.isEnableSecurity();
        this.startHdfsOperator = miniHadoopClusterTestContext.isStartHdfsOperator();
        this.classpath = miniHadoopClusterTestContext.classpath;
        this.configuration = miniHadoopClusterTestContext.configuration;
    }

    protected abstract MiniHadoopClusterTestContext getMiniHadoopClusterTestContext();

    @BeforeEach
    public void beforeEach() throws Exception {
        setupHadoopCluster(configuration, classpath);
    }

    @AfterEach
    public void afterEach() throws Exception {
        setDown();
    }

    public void setupHadoopCluster(Configuration configuration, String yarnClasspath)
            throws Exception {
        hadoopClusterTest =
                MiniHadoopClusterUtils.start(
                        configuration, yarnClasspath, enableSecurity, startHdfsOperator);
    }

    public void setDown() throws Exception {
        if (hadoopClusterTest != null) {
            hadoopClusterTest.close();
        }
    }

    public MiniHadoopCluster getHadoopCluster() {
        return hadoopClusterTest;
    }
}
