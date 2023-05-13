package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TestJobCounters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.List;

/**
 * TestMapreduce
 *
 * @author hhoa
 * @since 2023/5/5
 **/

public class TestSimpleMapreduce {
    private static final Logger LOG = LoggerFactory.getLogger(TestSimpleMapreduce.class);
    protected static MiniYARNCluster yarnCluster;
    protected static MiniDFSCluster dfsCluster;
    protected static YarnClient yarnClient;
    protected static Configuration config;
    protected String localInputFile = "mr_input.txt";
    protected String dfsInputDir = "/tmp/input";
    protected String dfsOutputDir = "/tmp/output";
    protected String localOutputDir = "/target/output";


    @BeforeAll
    public static void beforeAll() throws IOException, InterruptedException, YarnException {
        config = new Configuration();
        config.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
        config.setBoolean(JHAdminConfig.MR_HISTORY_MINICLUSTER_FIXED_PORTS, true);
        config.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
        config.set(MRConfig.FRAMEWORK_NAME, "yarn");
        dfsCluster = new MiniDFSCluster.Builder(config).build();
        dfsCluster.waitClusterUp();
        yarnCluster = new MiniMRYarnCluster("test");
        yarnCluster.init(config);
        yarnCluster.start();
        while (!yarnCluster.waitForNodeManagersToConnect(500)) {
            LOG.info("Waiting for Nodemanagers to connect");
        }
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(config);
        yarnClient.start();
    }
    @Test
    public void testWordCount() throws IOException, InterruptedException, ClassNotFoundException, YarnException {
        Job job = Job.getInstance(config, "word count");
        job.setJarByClass(TestSimpleMapreduce.class);
        job.setMapperClass(TestJobCounters.TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileSystem fileSystem = FileSystem.get(config);
        URL inputUrl = this.getClass().getClassLoader().getResource(localInputFile);
        fileSystem.copyFromLocalFile(new Path(inputUrl.getFile()), new Path(dfsInputDir));
        FileInputFormat.addInputPath(job, new Path(dfsInputDir));
        FileOutputFormat.setOutputPath(job, new Path(dfsOutputDir));
        job.waitForCompletion(true);
        fileSystem.copyToLocalFile(new Path(dfsOutputDir), new Path(localOutputDir));
        List<ApplicationReport> applications = yarnClient.getApplications();
        ApplicationReport applicationReport = applications.get(0);
        Assertions.assertEquals(applicationReport.getFinalApplicationStatus(), FinalApplicationStatus.SUCCEEDED);
    }
    @AfterEach
    public void afterEach() throws IOException{
        dfsCluster.close();
        yarnCluster.close();
        yarnClient.close();
    }
}
