package org.hhoao.hadoop.test.test.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature {
    public static void main(String[] args) throws Exception {
        String path = ClassLoader.getSystemClassLoader().getResource("").getPath();
        String rootPath = System.getProperty("user.dir");
        String hadoopConfDir = new Path(rootPath, "document/docker/hadoop-cluster").toString();
        Configuration conf = new Configuration();
        conf.addResource(new Path(hadoopConfDir, "core-site.xml"));
        conf.addResource(new Path(hadoopConfDir, "yarn-site.xml"));
        conf.addResource(new Path(hadoopConfDir, "hdfs-site.xml"));
        conf.addResource(new Path(hadoopConfDir, "mapred-site.xml"));
        Job job = Job.getInstance(conf, "word count");
        job.setJar(path + "hadoop-starter-1.0-SNAPSHOT.jar");
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.delete(new Path("/tmp/input"), true);
        fileSystem.copyFromLocalFile(false, true, new Path(path + "input"), new Path("/tmp/input"));
        FileInputFormat.addInputPath(job, new Path("/tmp/input"));
        FileOutputFormat.setOutputPath(job, new Path("/tmp/output"));
        job.waitForCompletion(true);
        fileSystem.copyToLocalFile(true, new Path("/tmp/output"), new Path(path + "output"));
    }
}
