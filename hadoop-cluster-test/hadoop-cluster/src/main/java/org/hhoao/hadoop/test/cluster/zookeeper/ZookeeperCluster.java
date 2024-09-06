package org.hhoao.hadoop.test.cluster.zookeeper;

import java.util.concurrent.CompletableFuture;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingCluster;

/**
 * ZookeeperCluster
 *
 * @author xianxing
 * @since 2024 /9/5
 */
public interface ZookeeperCluster extends AutoCloseable {
    /**
     * Restart.
     *
     * @param perServerRestartInterval the per server restart interval
     * @throws Exception the exception
     */
    void restart(int perServerRestartInterval) throws Exception;

    /**
     * Wait for start.
     *
     * @throws Exception the exception
     */
    void waitForStart() throws Exception;

    /**
     * Gets zookeeper stated.
     *
     * @return the zookeeper stated
     */
    CompletableFuture<Boolean> getZookeeperStated();

    /**
     * Gets cluster.
     *
     * @return the cluster
     */
    TestingCluster getCluster();

    /**
     * Gets client.
     *
     * @return the client
     */
    CuratorFramework getClient();

    void close() throws Exception;

    /** Start. */
    void start();
}
