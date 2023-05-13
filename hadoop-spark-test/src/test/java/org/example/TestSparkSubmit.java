package org.example;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.spark.deploy.SparkSubmit;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * TestSparkSubmit
 *
 * @author hhoa
 * @since 2023/5/8
 **/

public class TestSparkSubmit extends TestBase {
    @Test
    public void exampleTest() throws IOException, YarnException {
        // 创建Spark客户端参数
        SparkSubmit sparkSubmit = new SparkSubmit();
        String[] sparkArgs = new String[]{
                "--master", "yarn",
                "--deploy-mode", "cluster",
                "--driver-memory", "2g",
                "--executor-memory", "1g",
                "--executor-cores", "1",
                "--conf", "spark.yarn.am.retryInterval=1s",
                "--conf", "spark.yarn.jars=" + fileSystem.getUri() + HDFS_JARS_PATH,
                "--conf", "spark.yarn.archive=" + fileSystem.getUri() + HDFS_JARS_PATH,
                "--verbose",
                "--class", RUN_CLASS,
                RUN_JAR_PATH,
                RUN_ARG,
        };

        sparkSubmit.doSubmit(sparkArgs);
        List<ApplicationReport> applications = yarnClient.getApplications();
        ApplicationReport applicationReport = applications.get(0);
        assertEquals(FinalApplicationStatus.SUCCEEDED, applicationReport.getFinalApplicationStatus());
    }
}
