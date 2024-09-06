package org.hhoao.hadoop.test.cluster.zookeeper;

import java.util.concurrent.CompletableFuture;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingCluster;
import org.hhoao.hadoop.test.cluster.MiniHadoopClusterTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * HadoopZookeeperClusterTest
 *
 * @author xianxing
 * @since 2024/9/5
 */
public abstract class HadoopZookeeperClusterTest extends MiniHadoopClusterTest {
    private final ZookeeperCluster zookeeperCluster;

    public HadoopZookeeperClusterTest() {
        zookeeperCluster = new ZookeeperClusterImpl(getZookeeperClusterCount());
    }

    public abstract int getZookeeperClusterCount();

    public CompletableFuture<Boolean> getZookeeperStartState() {
        return zookeeperCluster.getZookeeperStated();
    }

    public void waitForStart() throws Exception {
        zookeeperCluster.waitForStart();
    }

    public TestingCluster getZookeeperCluster() {
        return zookeeperCluster.getCluster();
    }

    public CuratorFramework getCuratorFramework() {
        return zookeeperCluster.getClient();
    }

    public void restartZookeeperCluster(int perServerRestartInterval) throws Exception {
        zookeeperCluster.restart(perServerRestartInterval);
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        super.beforeEach();
        zookeeperCluster.start();
        zookeeperCluster.waitForStart();
    }

    @AfterEach
    public void afterEach() throws Exception {
        super.afterEach();
        zookeeperCluster.close();
    }
}
