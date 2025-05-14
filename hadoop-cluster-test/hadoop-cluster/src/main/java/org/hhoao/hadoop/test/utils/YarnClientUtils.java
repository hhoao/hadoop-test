package org.hhoao.hadoop.test.utils;

import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.hhoao.hadoop.test.security.SecurityOperator;

/**
 * YarnClientUtils
 *
 * @author xianxing
 * @since 2024/6/27
 */
public class YarnClientUtils {
    public static YarnClient createRecoverableYarnClientProxy(
            YarnClient yarnClient,
            Configuration yarnConfiguration,
            @Nullable SecurityOperator securityOperator,
            int clientRecoveryMaxRetryNum,
            int clientRecoveryRetryInterval) {
        return RecoverableClientProxyEnhance.createClientProviderByCGLIB(
                yarnClient,
                YarnClient.class,
                new Class[] {String.class},
                new Object[] {"ProxyYarnClient"},
                clientRecoveryMaxRetryNum,
                clientRecoveryRetryInterval,
                oldClient -> {
                    if (oldClient != null) {
                        try {
                            oldClient.close();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    if (securityOperator != null) {
                        try {
                            return securityOperator.login(
                                    () -> createYarnClientInternal(yarnConfiguration));
                        } catch (Throwable e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        return createYarnClientInternal(yarnConfiguration);
                    }
                });
    }

    public static YarnClient createRecoverableYarnClientProxy(
            Configuration yarnConfiguration,
            @Nullable SecurityOperator securityOperator,
            int clientRecoveryMaxRetryNum,
            int clientRecoveryRetryInterval) {
        YarnClient yarnClientInternal = createYarnClientInternal(yarnConfiguration);
        return createRecoverableYarnClientProxy(
                yarnClientInternal,
                yarnConfiguration,
                securityOperator,
                clientRecoveryMaxRetryNum,
                clientRecoveryRetryInterval);
    }

    private static YarnClient createYarnClientInternal(Configuration configuration) {
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(configuration);
        yarnClient.start();
        return yarnClient;
    }
}
