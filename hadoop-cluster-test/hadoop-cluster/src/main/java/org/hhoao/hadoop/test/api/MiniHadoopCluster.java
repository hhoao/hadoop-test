package org.hhoao.hadoop.test.api;

import java.io.Closeable;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;

/**
 * MiniHadoopCluster
 *
 * @author xianxing
 * @since 2024 /4/12
 */
public interface MiniHadoopCluster extends Closeable {
    /**
     * Gets file system.
     *
     * @return the file system
     */
    FileSystem getFileSystem();

    /**
     * Gets dfs cluster.
     *
     * @return the dfs cluster
     */
    MiniDFSCluster getDfsCluster();

    /**
     * Gets resource manager.
     *
     * @return the resource manager
     */
    ResourceManager getResourceManager();

    /**
     * Gets node manager.
     *
     * @return the node manager
     */
    NodeManager getNodeManager();

    /**
     * Gets yarn client.
     *
     * @return the yarn client
     */
    YarnClient getYarnClient();

    /**
     * Gets config.
     *
     * @return the config
     */
    Configuration getConfig();

    /**
     * Start.
     *
     * @param customConfiguration the custom configuration
     * @param yarnClasspath the yarn classpath
     * @param startHdfsOperator the start hdfs operator
     */
    void start(Configuration customConfiguration, String yarnClasspath, boolean startHdfsOperator)
            throws Exception;

    /**
     * Start and wait for started.
     *
     * @param customConfiguration the custom configuration
     * @param yarnClasspath the yarn classpath
     * @param startHdfsOperator the start hdfs operator
     * @throws ExecutionException the execution exception
     * @throws InterruptedException the interrupted exception
     */
    void startAndWaitForStarted(
            @Nullable Configuration customConfiguration,
            String yarnClasspath,
            boolean startHdfsOperator)
            throws ExecutionException, InterruptedException;
}
