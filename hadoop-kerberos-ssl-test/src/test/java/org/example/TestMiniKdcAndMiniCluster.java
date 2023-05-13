package org.example;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TestJobCounters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.security.UserGroupInformation;
import org.example.utils.FileUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedAction;

import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;

/**
 * TestMiniKdcAndMiniCluster
 *
 * @author hhoa
 * @since 2023/4/24
 **/

public class TestMiniKdcAndMiniCluster extends TestMiniKdcAndMiniClusterBase {
    private static final Logger LOG = LoggerFactory.getLogger(TestMiniKdcAndMiniCluster.class);
    private static final String output = "output";

    public static void cleanUp() {
        File output = new File(TestMiniKdcAndMiniCluster.output);
        if (output.exists()) {
            FileUtils.deleteFile(output);
        }
    }

    @Test
    @Ignore
    public void test() throws IOException {
        System.setProperty("java.library.path", ClassLoader.getSystemClassLoader().getResource("native").getFile());
        if (System.getProperty("java.library.path") == null) {
            LOG.error("-Djava.library.path=${HADOOP_HOME}/lib/native parameter must be added at startup");
        }

        UserGroupInformation.setConfiguration(config);
        UserGroupInformation userGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI(hadoopServicePrincipal, keytabFile.getAbsolutePath());
        userGroupInformation.setAuthenticationMethod(KERBEROS);
        userGroupInformation.doAs((PrivilegedAction<Object>) () -> {
            try {
                Job job = Job.getInstance(config, "word count");
                job.setJarByClass(TestMiniKdcAndMiniCluster.class);
                job.setMapperClass(TestJobCounters.TokenizerMapper.class);
                job.setCombinerClass(IntSumReducer.class);
                job.setReducerClass(IntSumReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                FileSystem fileSystem = FileSystem.get(config);
                fileSystem.copyFromLocalFile(new Path("input"), new Path("input"));
                FileInputFormat.addInputPath(job, new Path("input"));
                FileOutputFormat.setOutputPath(job, new Path("output"));
                job.waitForCompletion(true);
                fileSystem.copyToLocalFile(new Path("output"), new Path("output"));
            } catch (IOException | InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        cleanUp();
    }
}
