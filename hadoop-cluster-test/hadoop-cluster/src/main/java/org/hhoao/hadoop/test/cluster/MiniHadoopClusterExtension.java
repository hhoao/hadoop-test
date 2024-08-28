package org.hhoao.hadoop.test.cluster;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.hhoao.hadoop.test.api.MiniHadoopCluster;
import org.junit.jupiter.api.extension.*;

public class MiniHadoopClusterExtension
        implements BeforeAllCallback, AfterEachCallback, AfterAllCallback {
    private MiniHadoopCluster hadoopClusterTest;
    private final boolean enableSecurity;
    private final boolean startHdfsOperator;
    private final String classpath;
    private final Configuration configuration;

    public MiniHadoopClusterExtension() {
        this(false, false, null, null);
    }

    public MiniHadoopClusterExtension(
            boolean enableSecurity,
            boolean startHdfsOperator,
            @Nullable String classpath,
            @Nullable Configuration configuration) {
        this.enableSecurity = enableSecurity;
        this.startHdfsOperator = startHdfsOperator;
        this.classpath = classpath;
        this.configuration = configuration;
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        setDown();
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        setupHadoopCluster(configuration, classpath);
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        initHadoopCluster();
    }

    private void initHadoopCluster() throws IOException, YarnException {
        YarnClient yarnClient = hadoopClusterTest.getYarnClient();
        List<ApplicationReport> applications = yarnClient.getApplications();
        applications.forEach(
                applicationReport -> {
                    if (applicationReport.getYarnApplicationState()
                            != YarnApplicationState.FINISHED) {
                        try {
                            yarnClient.killApplication(applicationReport.getApplicationId());
                        } catch (YarnException | IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
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
