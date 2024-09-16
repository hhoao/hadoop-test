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
public class SecurityReturnObjectProxy<T> implements InvocationHandler, Serializable {
    private static final long serialVersionUID = 1L;
    private final SecurityOperator operator;
    private final T instance;

    public SecurityReturnObjectProxy(SecurityOperator operator, T instance) {
        this.operator = operator;
        this.instance = instance;
    }

    public static Object createInstance(
            SecurityOperator securityOperator, Object object, Class<?>[] interfaces) {
        SecurityReturnObjectProxy<Object> securityObjectProxy =
                new SecurityReturnObjectProxy<>(securityOperator, object);
        return Proxy.newProxyInstance(
                SecurityReturnObjectProxy.class.getClassLoader(), interfaces, securityObjectProxy);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Object invoke = method.invoke(instance, args);
        if (invoke != null) {
            Class<?> returnType = method.getReturnType();
            return SecurityObjectProxy.createInstance(operator, invoke, new Class[] {returnType});
        }
        return null;
    }
}
