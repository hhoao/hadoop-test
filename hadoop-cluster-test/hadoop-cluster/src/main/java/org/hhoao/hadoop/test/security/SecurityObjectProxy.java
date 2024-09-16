package org.hhoao.hadoop.test.security;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * SecurityObjectProxy
 *
 * @author w
 * @since 2024/9/13
 */
public class SecurityObjectProxy<T> implements InvocationHandler, Serializable {
    private static final long serialVersionUID = 1L;
    private final SecurityOperator operator;
    private final T instance;

    public SecurityObjectProxy(SecurityOperator operator, T instance) {
        this.operator = operator;
        this.instance = instance;
    }

    public static Object createInstance(
            SecurityOperator securityOperator, Object object, Class<?>[] interfaces) {
        SecurityObjectProxy<Object> securityObjectProxy =
                new SecurityObjectProxy<>(securityOperator, object);
        return Proxy.newProxyInstance(
                SecurityObjectProxy.class.getClassLoader(), interfaces, securityObjectProxy);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        return operator.login(() -> method.invoke(instance, args));
    }
}
