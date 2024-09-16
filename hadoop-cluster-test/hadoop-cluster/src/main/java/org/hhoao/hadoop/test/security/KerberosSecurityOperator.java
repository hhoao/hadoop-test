package org.hhoao.hadoop.test.security;

import java.security.PrivilegedAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.hhoao.hadoop.test.utils.SupplierWithException;

/**
 * KerberosSecurityOperator
 *
 * @author w
 * @since 2024/9/12
 */
public class KerberosSecurityOperator implements SecurityOperator {
    private static final long serialVersionUID = 1L;
    private final String principal;
    private final String keytab;

    public KerberosSecurityOperator(String principal, String keytab) {
        this.principal = principal;
        this.keytab = keytab;
    }

    @Override
    public <T> T login(SupplierWithException<T, Throwable> supplier) throws Throwable {
        Configuration configuration = new Configuration();
        configuration.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
        UserGroupInformation.setConfiguration(configuration);
        UserGroupInformation ugiFromSubject =
                UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
        return ugiFromSubject.doAs(
                (PrivilegedAction<T>)
                        () -> {
                            try {
                                return supplier.get();
                            } catch (Throwable e) {
                                throw new RuntimeException(e);
                            }
                        });
    }
}
