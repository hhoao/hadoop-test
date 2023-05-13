package org.hhoao.hadoop.test.test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.hhoao.hadoop.test.cluster.MiniHadoopClusterExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * TestFileSystem
 *
 * @author xianxing
 * @since 2024/7/2
 */
public class TestFileSystem {
    Configuration configuration;

    @RegisterExtension
    public static MiniHadoopClusterExtension miniHadoopClusterExtension =
            new MiniHadoopClusterExtension();

    @BeforeEach
    void testUp() {
        configuration = miniHadoopClusterExtension.getHadoopCluster().getConfig();
    }

    @Test
    void testSymlLink() throws IOException {
        FileContext fileContext = FileContext.getFileContext(configuration);
        Path linkTarget = fileContext.getLinkTarget(new Path("/hello"));
        System.out.println(linkTarget);
    }

    @Test
    void testGetLink() throws IOException {
        FileContext fileContext = FileContext.getFileContext(configuration);
        for (int i = 0; i < 6; i++) {
            Path linkTarget = fileContext.getLinkTarget(new Path("/hello" + i));
            System.out.println(linkTarget);
        }
    }

    @Test
    void testGetLinkLink() throws IOException {
        FileContext fileContext = FileContext.getFileContext(configuration);
        for (int i = 0; i < 6; i++) {
            Path linkTarget = fileContext.getLinkTarget(new Path("/hello" + i + i));
            Path linkTarget2 = fileContext.getLinkTarget(linkTarget);
            System.out.println(linkTarget2);
            System.out.println(linkTarget);
        }
    }

    @Test
    void createFile() throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        FileSystem.enableSymlinks();
        fileSystem.create(new Path("/hello"));
    }

    @Test
    void link() throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        FileSystem.enableSymlinks();
        for (int i = 0; i < 6; i++) {
            fileSystem.createSymlink(new Path("/hello"), new Path("/hello" + i), true);
        }
    }

    @Test
    void linklink() throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        FileSystem.enableSymlinks();
        for (int i = 0; i < 6; i++) {
            fileSystem.createSymlink(new Path("/hello" + i), new Path("/hello" + i + "" + i), true);
        }
    }

    @Test
    void delLink() throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        FileSystem.enableSymlinks();
        for (int i = 0; i < 6; i++) {
            fileSystem.delete(new Path("/hello" + i));
        }
    }

    @Test
    void delCheckSum() throws IOException, NoSuchAlgorithmException {
        FileSystem fileSystem = FileSystem.get(configuration);
        MessageDigest md = MessageDigest.getInstance("MD5");
        String path =
                "/Users/w/sync/development/large-scale-data/hadoop/study/hadoop-test/hadoop-cluster-test/hadoop-cluster-2.7.3/src/main/resources/log4j.properties";
        try (InputStream is = Files.newInputStream(Paths.get(path));
                DigestInputStream dis = new DigestInputStream(is, md)) {}
        byte[] digest = md.digest();
        fileSystem.copyFromLocalFile(new Path(path), new Path(path));
        System.out.println(new String(digest));
        FileChecksum fileChecksum = fileSystem.getFileChecksum(new Path(path));
        String algorithmName = fileChecksum.getAlgorithmName();
        System.out.println(algorithmName);
    }

    @Test
    void del() throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);

        fileSystem.delete(new Path("/hello"));
    }
}
