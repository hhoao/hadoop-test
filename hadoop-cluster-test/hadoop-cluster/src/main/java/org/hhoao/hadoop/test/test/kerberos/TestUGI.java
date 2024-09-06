package org.hhoao.hadoop.test.test.kerberos;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.PrivilegedAction;
import javax.security.auth.Subject;
import org.apache.hadoop.security.UserGroupInformation;
import org.hhoao.hadoop.test.cluster.MiniHadoopClusterTest;
import org.hhoao.hadoop.test.cluster.MiniHadoopClusterTestContext;
import org.hhoao.hadoop.test.cluster.MiniHadoopSecurityCluster;
import org.junit.jupiter.api.Test;

/**
 * TestUGI
 *
 * @author xianxing
 * @since 2024/9/4
 */
public class TestUGI extends MiniHadoopClusterTest {

    @Test
    void test()
            throws IOException, InvocationTargetException, NoSuchMethodException,
                    IllegalAccessException {
        MiniHadoopSecurityCluster hadoopCluster = (MiniHadoopSecurityCluster) getHadoopCluster();
        UserGroupInformation.setConfiguration(hadoopCluster.getConfig());
        //        File krb5conf = hadoopCluster.getKdc().getKrb5conf();
        //        File keytabFile = hadoopCluster.getKeytabFile();
        //
        // UserGroupInformation.loginUserFromKeytab(hadoopCluster.getHadoopServicePrincipal(),
        // keytabFile.getAbsolutePath());
        //        UserGroupInformation.reset();
        //        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        //        UserGroupInformation w = UserGroupInformation.loginUserFromKeytabAndReturnUGI("
        // w/localhost@EXAMPLE.COM", "hadoop-cluster-test/hadoop-cluster/krb5.keytable");
        UserGroupInformation w =
                UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                        hadoopCluster.getHadoopServicePrincipal(),
                        hadoopCluster.getKeytabFile().getAbsolutePath());
        Subject subject = getSubject(w);
        w.doAs(
                (PrivilegedAction<Object>)
                        () -> {
                            try {
                                UserGroupInformation.getCurrentUser();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                            return null;
                        });
        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        System.out.println(subject);
    }

    Subject getSubject(UserGroupInformation userGroupInformation)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method getSubject = UserGroupInformation.class.getDeclaredMethod("getSubject");
        getSubject.setAccessible(true);
        return (Subject) getSubject.invoke(userGroupInformation);
    }

    @Override
    protected MiniHadoopClusterTestContext getMiniHadoopClusterTestContext() {
        MiniHadoopClusterTestContext miniHadoopClusterTestContext =
                new MiniHadoopClusterTestContext();
        miniHadoopClusterTestContext.setEnableSecurity(true);
        return miniHadoopClusterTestContext;
    }
}
