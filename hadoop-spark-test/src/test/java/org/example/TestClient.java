package org.example;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * TestMapreduce
 *
 * @author hhoa
 * @since 2023/5/5
 **/

public class TestClient extends TestBase {
    @Test
    public void exampleTest() throws IOException, YarnException {
        // 创建Spark客户端参数
        ClientArguments clientArgs = new ClientArguments(
                new String[]{
                        "--class", RUN_CLASS,
                        "--jar", RUN_JAR_PATH,
                        "--arg", RUN_ARG,
                }
        );

        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.testing", "true");
        sparkConf.set("spark.rpc.askTimeout", "10s");
        sparkConf.set("spark.yarn.archive", fileSystem.getUri() + "/tmp/jars");
        sparkConf.set("spark.submitTime", String.valueOf(System.currentTimeMillis()));
        sparkConf.set("spark.app.name", "spark-pi");
        sparkConf.set("spark.executor.cores", "1");
        sparkConf.set("spark.submit.deployMode", "cluster");
        sparkConf.set("spark.submit.pyFiles", "");
        sparkConf.set("spark.driver.memory", "4g");
        sparkConf.set("spark.master", "yarn");
        sparkConf.set("spark.executor.memory", "2g");

        Client sparkClient = new Client(clientArgs, sparkConf, null);
        sparkClient.run();

        List<ApplicationReport> applications = yarnClient.getApplications();
        ApplicationReport applicationReport = applications.get(0);
        assertEquals(FinalApplicationStatus.SUCCEEDED, applicationReport.getFinalApplicationStatus());
    }
}
