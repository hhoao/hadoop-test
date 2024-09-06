package org.hhoao.hadoop.test.test.zookeeper;

import java.util.concurrent.TimeUnit;
import org.hhoao.hadoop.test.cluster.zookeeper.ZookeeperClusterImpl;
import org.junit.jupiter.api.Test;

/**
 * TestZookeeperCluster
 *
 * @author xianxing
 * @since 2024/9/6
 */
public class TestZookeeperCluster {
    @Test
    void test() throws InterruptedException {
        ZookeeperClusterImpl zookeeperCluster = new ZookeeperClusterImpl(3);
        zookeeperCluster.start();
        TimeUnit.HOURS.sleep(1);
    }
}
