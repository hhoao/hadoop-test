package org.hhoao.hadoop.test.utils;

import java.lang.reflect.Method;
import java.util.function.Function;
import org.springframework.cglib.proxy.Enhancer;
import org.springframework.cglib.proxy.MethodInterceptor;
import org.springframework.cglib.proxy.MethodProxy;

public class RecoverableClientProxyEnhance<T> extends RecoverableClientProxy<T>
        implements MethodInterceptor {
    protected RecoverableClientProxyEnhance(
            T client,
            int clientRecoveryMaxRetryNum,
            int clientRecoveryRetryInterval,
            Function<T, T> failoverGetClientCallback) {
        super(
                client,
                clientRecoveryMaxRetryNum,
                clientRecoveryRetryInterval,
                failoverGetClientCallback);
    }

    @SuppressWarnings("unchecked")
    public static <T> T createClientProviderByCGLIB(
            T client,
            Class<?> superClass,
            Class<?>[] argumentTypes,
            Object[] arguments,
            int clientRecoveryMaxRetryNum,
            int clientRecoveryRetryInterval,
            Function<T, T> failoverGetClientCallback) {
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(superClass);
        enhancer.setCallback(
                new RecoverableClientProxyEnhance<T>(
                        client,
                        clientRecoveryMaxRetryNum,
                        clientRecoveryRetryInterval,
                        failoverGetClientCallback));
        return (T) enhancer.create(argumentTypes, arguments);
    }

    @Override
    public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy)
            throws Throwable {
        method.setAccessible(true);
        return invoke(obj, method, args);
    }
}
