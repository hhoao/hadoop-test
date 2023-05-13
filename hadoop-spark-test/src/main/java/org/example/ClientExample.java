package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class ClientExample {
    private static final Logger LOG = LoggerFactory.getLogger(SparkSubmitExample.class);

    public static void main(String[] args) throws IOException {
        String resourceDir = ClassLoader.getSystemClassLoader().getResource("").getPath();
        String rootPath = System.getProperty("user.dir");
        String hadoopConfDir = new Path(rootPath, "document/docker/hadoop-cluster").toString();
        Configuration conf = new Configuration();
        conf.addResource(new Path(hadoopConfDir, "core-site.xml"));
        conf.addResource(new Path(hadoopConfDir, "yarn-site.xml"));
        conf.addResource(new Path(hadoopConfDir, "hdfs-site.xml"));
        conf.addResource(new Path(hadoopConfDir, "mapred-site.xml"));

        ClientArguments clientArgs = new ClientArguments(
                new String[]{
                        "--class", "org.apache.spark.examples.SparkPi",
                        "--jar", new File(resourceDir, "spark-examples_2.12-3.3.1.jar").toURI().toString(),
                        "--arg", "10",
                }
        );

        FileSystem fileSystem = FileSystem.get(conf);
        Path jarsPath = new Path(rootPath, "data/jars");
        if (!fileSystem.exists(new Path("/tmp/jars"))) {
            if (!new File(jarsPath.toString()).exists()) {
                LOG.error("you need to copy your spark jars dirs to " + jarsPath);
                throw new RuntimeException("you need to copy your spark jars dirs to " + jarsPath);
            }
            fileSystem.copyFromLocalFile(jarsPath, new Path("/tmp/jars"));
        }

        SparkConf sparkConf = new SparkConf();
        for (Map.Entry<String, String> entry : conf) {
            sparkConf.set("spark.hadoop." + entry.getKey(), entry.getValue());
        }
        sparkConf.set("spark.rpc.askTimeout", "10s");
        sparkConf.set("spark.yarn.archive", fileSystem.getUri() + "/tmp/jars");
        sparkConf.set("spark.submitTime", String.valueOf(System.currentTimeMillis()));
        sparkConf.set("spark.app.name", "spark-pi");
        sparkConf.set("spark.executor.cores", "1");
        sparkConf.set("spark.submit.deployMode", "cluster");
        sparkConf.set("spark.submit.pyFiles", "");
        sparkConf.set("spark.driver.memory", "1g");
        sparkConf.set("spark.master", "yarn");
        sparkConf.set("spark.executor.memory", "1g");

        Client sparkClient = new Client(clientArgs, sparkConf, null);
        sparkClient.run();
    }
}
