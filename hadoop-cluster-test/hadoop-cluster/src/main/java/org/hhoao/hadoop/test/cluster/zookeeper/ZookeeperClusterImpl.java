package org.hhoao.hadoop.test.cluster.zookeeper;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.TestingZooKeeperServer;
import org.apache.hadoop.net.NetUtils;
import org.hhoao.hadoop.test.utils.Resources;

public class ZookeeperClusterImpl extends Thread implements AutoCloseable, ZookeeperCluster {
    private final CompletableFuture<Boolean> zookeeperStated = new CompletableFuture<>();
    private final int count;
    private TestingCluster cluster;
    private CuratorFramework client;
    private File rootDir = new File(Resources.getTargetDir(), "zookeeper");
    private AtomicInteger id = new AtomicInteger();

    public ZookeeperClusterImpl(int count) {
        this.count = count;
    }

    public CompletableFuture<Boolean> getZookeeperStated() {
        return zookeeperStated;
    }

    public TestingCluster getCluster() {
        return cluster;
    }

    public CuratorFramework getClient() {
        return client;
    }

    @Override
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

    @Override
    public void run() {
        try {
            initProperties();
            InstanceSpec[] instanceSpecs = createInstanceSpecs(count);
            cluster = new TestingCluster(instanceSpecs);
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

    private InstanceSpec[] createInstanceSpecs(int count) throws IOException {
        InstanceSpec[] instanceSpecs = new InstanceSpec[count];
        for (int i = 0; i < count; i++) {
            Integer zookeeperId = getZookeeperId();
            File file = new File(rootDir, String.valueOf(zookeeperId));
            if (file.exists()) {
                FileUtils.forceDelete(file);
            }
            file.mkdirs();
            InstanceSpec instanceSpec =
                    new InstanceSpec(
                            file,
                            NetUtils.getFreeSocketPort(),
                            NetUtils.getFreeSocketPort(),
                            NetUtils.getFreeSocketPort(),
                            false,
                            zookeeperId);
            instanceSpecs[i] = instanceSpec;
        }
        return instanceSpecs;
    }

    private Integer getZookeeperId() {
        return id.getAndIncrement();
    }

    private void initProperties() throws IOException {
        System.setProperty("zookeeper.admin.serverPort", String.valueOf(0));
    }

    @Override
    public void waitForStart() throws Exception {
        zookeeperStated.get();
    }
}
