package org.hhoao.test.zookeeper;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.TestingZooKeeperServer;

public class ZookeeperClusterTest implements AutoCloseable {
    private final CompletableFuture<Boolean> zookeeperStated = new CompletableFuture<>();
    private TestingCluster cluster;
    private CuratorFramework client;

    public CompletableFuture<Boolean> getZookeeperStated() {
        return zookeeperStated;
    }

    public TestingCluster getCluster() {
        return cluster;
    }

    public CuratorFramework getClient() {
        return client;
    }

    public void start() {
        int count = 3;
        cluster = new TestingCluster(count);
        try {
            cluster.start();
            client =
                    CuratorFrameworkFactory.newClient(
                            cluster.getConnectString(), new ExponentialBackoffRetry(1000, 3));
            client.start();
            zookeeperStated.complete(true);
        } catch (Exception e) {
            zookeeperStated.complete(false);
            throw new RuntimeException(e);
        }
    }

    public void restart(int perServerRestartInterval) throws Exception {
        List<TestingZooKeeperServer> servers = cluster.getServers();
        for (TestingZooKeeperServer server : servers) {
            server.restart();
            Thread.sleep(perServerRestartInterval);
        }
    }

    @Override
    public void close() throws Exception {
        client.close();
        cluster.close();
    }
}
