package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.deploy.SparkSubmit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * SparkSubmitExample
 *
 * @author hhoa
 * @since 2023/5/8
 **/

public class SparkSubmitExample {
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

        File yarnSiteXML = new File(resourceDir, "yarn-site.xml");
        yarnSiteXML.deleteOnExit();
        try (FileWriter writer = new FileWriter(yarnSiteXML)) {
            conf.writeXml(writer);
            writer.flush();
        }

        SparkSubmit sparkSubmit = new SparkSubmit();
        System.setProperty("spark.testing", "true");
        FileSystem fileSystem = FileSystem.get(conf);
        Path jarsPath = new Path(rootPath, "data/jars");

        if (!fileSystem.exists(new Path("/tmp/jars"))) {
            if (!new File(jarsPath.toString()).exists()) {
                LOG.error("you need to copy your spark jars dirs to " + jarsPath);
                throw new RuntimeException("you need to copy your spark jars dirs to " + jarsPath);
            }
            fileSystem.copyFromLocalFile(jarsPath, new Path("/tmp/jars"));
        }

        List<String> list = new ArrayList<>(Arrays.asList("--class", "org.apache.spark.examples.SparkPi",
                "--master", "yarn",
                "--deploy-mode", "cluster",
                "--driver-memory", "1g",
                "--executor-memory", "1g",
                "--executor-cores", "1",
                "--conf", "spark.yarn.archive=" + fileSystem.getUri() + "/tmp/jars"
        ));
        for (Map.Entry<String, String> entry : conf) {
            list.add("--conf");
            list.add("spark.hadoop." + entry.getKey() + "=" + entry.getValue());
        }
        list.add(resourceDir + "spark-examples_2.12-3.3.1.jar");
        list.add("10");
        String[] sparkArgs = list.toArray(new String[]{});

        sparkSubmit.doSubmit(sparkArgs);
    }
}
