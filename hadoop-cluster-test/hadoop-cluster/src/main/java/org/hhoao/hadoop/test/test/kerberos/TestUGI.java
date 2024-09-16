package org.hhoao.hadoop.test.test.kerberos;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.PrivilegedAction;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.hhoao.hadoop.test.api.SecurityContext;
import org.hhoao.hadoop.test.cluster.MiniHadoopClusterTest;
import org.hhoao.hadoop.test.cluster.MiniHadoopClusterTestContext;
import org.hhoao.hadoop.test.cluster.MiniHadoopSecurityCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * TestUGI
 *
 * @author xianxing
 * @since 2024/9/4
 */
public class TestUGI extends MiniHadoopClusterTest {

    @Test
    @DisplayName(
            "UGI#reset make process kdc error, error: Kerberos principal name does NOT have the expected hostname part: w")
    void test2() throws IOException {
        UserGroupInformation.reset();
        SecurityContext securityContext = getHadoopCluster().getSecurityContext();

        Configuration entries = new Configuration();
        entries.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
        UserGroupInformation.setConfiguration(entries);
        UserGroupInformation ugi = securityContext.getDefaultUGI();
        Assertions.assertThrows(
                Exception.class,
                () ->
                        ugi.doAs(
                                (PrivilegedAction<?>)
                                        () -> {
                                            try {
                                                FileSystem fileSystem =
                                                        FileSystem.get(
                                                                getHadoopCluster().getConfig());
                                                fileSystem.exists(new Path("/"));
                                            } catch (IOException e) {
                                                throw new RuntimeException(e);
                                            }
                                            return null;
                                        }));
    }

    @Test
    void testGetSubject()
            throws IOException, InvocationTargetException, NoSuchMethodException,
                    IllegalAccessException {
        MiniHadoopSecurityCluster hadoopCluster = (MiniHadoopSecurityCluster) getHadoopCluster();
        UserGroupInformation.setConfiguration(hadoopCluster.getConfig());
        UserGroupInformation ugi =
                UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                        hadoopCluster.getHadoopServicePrincipal(),
                        hadoopCluster.getKeytabFile().getAbsolutePath());
        Subject subject = getSubject(ugi);
        System.out.println(subject);
    }

    Subject getSubject(UserGroupInformation userGroupInformation)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method getSubject = UserGroupInformation.class.getDeclaredMethod("getSubject");
        getSubject.setAccessible(true);
        return (Subject) getSubject.invoke(userGroupInformation);
    }

    @Test
    void testCustomUGI() throws LoginException {
        MiniHadoopSecurityCluster hadoopCluster = (MiniHadoopSecurityCluster) getHadoopCluster();
        UserGroupInformation.reset();
        String principal = hadoopCluster.getHadoopServicePrincipal();
        String keytab = hadoopCluster.getKeytabFile().getAbsolutePath();
        Subject subject = CustomUGI.loginUserFromKeytab(principal, keytab);
        Subject.doAs(
                subject,
                (PrivilegedAction<?>)
                        () -> {
                            try {
                                boolean exists =
                                        hadoopCluster.getFileSystem().exists(new Path("/"));
                                System.out.println(exists);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                            return null;
                        });
    }

    @Override
    protected MiniHadoopClusterTestContext getMiniHadoopClusterTestContext() {
        MiniHadoopClusterTestContext miniHadoopClusterTestContext =
                new MiniHadoopClusterTestContext();
        miniHadoopClusterTestContext.setEnableSecurity(true);
        return miniHadoopClusterTestContext;
    }
}
