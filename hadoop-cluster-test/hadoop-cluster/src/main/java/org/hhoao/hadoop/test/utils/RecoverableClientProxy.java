package org.hhoao.hadoop.test.utils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recoverable client proxy.
 *
 * @param <T> the type parameter
 */
public class RecoverableClientProxy<T> implements InvocationHandler {
    protected static final Logger LOG = LoggerFactory.getLogger(RecoverableClientProxy.class);
    protected T client;
    protected final int clientRecoveryMaxRetryNum;
    protected final int clientRecoveryRetryInterval;
    protected final Function<T, T> failoverGetClientCallback;
    protected int retryNum = 0;

    protected RecoverableClientProxy(
            T client,
            int clientRecoveryMaxRetryNum,
            int clientRecoveryRetryInterval,
            Function<T, T> failoverGetClientCallback) {
        this.client = client;
        this.clientRecoveryMaxRetryNum = clientRecoveryMaxRetryNum;
        this.clientRecoveryRetryInterval = clientRecoveryRetryInterval;
        this.failoverGetClientCallback = failoverGetClientCallback;
    }

    public static <T> T createClientProvider(
            int clientRecoveryMaxRetryNum,
            int clientRecoveryRetryInterval,
            Function<T, T> failoverGetClientCallback) {
        T apply = failoverGetClientCallback.apply(null);
        return createClientProvider(
                apply,
                clientRecoveryMaxRetryNum,
                clientRecoveryRetryInterval,
                failoverGetClientCallback);
    }

    public static <T> T createClientProvider(
            Class<?>[] interfaces,
            int clientRecoveryMaxRetryNum,
            int clientRecoveryRetryInterval,
            Function<T, T> failoverGetClientCallback) {
        T apply = failoverGetClientCallback.apply(null);
        return createClientProvider(
                apply,
                interfaces,
                clientRecoveryMaxRetryNum,
                clientRecoveryRetryInterval,
                failoverGetClientCallback);
    }

    /**
     * Create client provider t.
     *
     * @param <T> Interface of client.
     * @param client Implement of client T.
     * @param clientRecoveryMaxRetryNum the client recovery max retry num
     * @param clientRecoveryRetryInterval the client recovery retry interval
     * @param failoverGetClientCallback the failover get client callback, when client invoke failed
     *     and exceed max retry num,then invoke this to do some failover operator and get new
     *     client.
     * @return the client proxy.
     */
    public static <T> T createClientProvider(
            T client,
            int clientRecoveryMaxRetryNum,
            int clientRecoveryRetryInterval,
            Function<T, T> failoverGetClientCallback) {
        return createClientProvider(
                client,
                client.getClass().getInterfaces(),
                clientRecoveryMaxRetryNum,
                clientRecoveryRetryInterval,
                failoverGetClientCallback);
    }

    @SuppressWarnings("unchecked")
    public static <T> T createClientProvider(
            T client,
            Class<?>[] interfaces,
            int clientRecoveryMaxRetryNum,
            int clientRecoveryRetryInterval,
            Function<T, T> failoverGetClientCallback) {
        if (client == null) {
            throw new RuntimeException("The client cannot be null");
        }
        return (T)
                Proxy.newProxyInstance(
                        client.getClass().getClassLoader(),
                        interfaces,
                        new RecoverableClientProxy<T>(
                                client,
                                clientRecoveryMaxRetryNum,
                                clientRecoveryRetryInterval,
                                failoverGetClientCallback));
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        while (retryNum < clientRecoveryMaxRetryNum) {
            try {
                return method.invoke(client, args);
            } catch (Exception e) {
                LOG.warn("{} client invoke error", client.getClass().getName(), e);
                retryNum++;
            }
            TimeUnit.MILLISECONDS.sleep(clientRecoveryRetryInterval);
        }
        T newClient = failoverGetClientCallback.apply(client);
        if (newClient != null) {
            client = newClient;
            return method.invoke(client, args);
        }
        throw new RuntimeException(
                String.format("Failover and get new %s failed", client.getClass().getName()));
    }
}
