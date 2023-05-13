package org.example;

import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.junit.Test;

import java.io.File;

/**
 * TestLocal
 *
 * @author hhoa
 * @since 2023/5/9
 **/

public class ZookeeperSimpleTest {
    @Test
    public void test() throws Exception {
        String path = ClassLoader.getSystemClassLoader().getResource("").getPath();
        String rootDir = new File(path).getParent();
        InstanceSpec spec = new InstanceSpec(new File(rootDir, "data"), 20010, 22001,
                22002, false, 100123, 2000, 60);
        TestingServer testingServer = new TestingServer(spec, true);
        testingServer.start();
    }
}
