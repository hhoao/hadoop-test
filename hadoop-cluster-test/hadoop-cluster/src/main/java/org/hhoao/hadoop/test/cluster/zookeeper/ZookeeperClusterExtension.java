package org.hhoao.hadoop.test.cluster.zookeeper;

import java.util.concurrent.CompletableFuture;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingCluster;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class ZookeeperClusterExtension implements BeforeAllCallback, AfterAllCallback {
    private final ZookeeperCluster zookeeperCluster;

    public ZookeeperClusterExtension(int count) {
        zookeeperCluster = new ZookeeperClusterImpl(count);
    }

    public CompletableFuture<Boolean> getZookeeperStartState() {
        return zookeeperCluster.getZookeeperStated();
    }

    public void waitForStart() throws Exception {
        zookeeperCluster.waitForStart();
    }

    public TestingCluster getCluster() {
        return zookeeperCluster.getCluster();
    }

    public CuratorFramework getClient() {
        return zookeeperCluster.getClient();
    }

    public void restart(int perServerRestartInterval) throws Exception {
        zookeeperCluster.restart(perServerRestartInterval);
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        zookeeperCluster.close();
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        zookeeperCluster.start();
    }
}
