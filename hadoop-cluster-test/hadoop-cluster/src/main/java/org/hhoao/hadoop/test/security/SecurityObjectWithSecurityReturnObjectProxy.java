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
public class SecurityObjectWithSecurityReturnObjectProxy<T>
        implements InvocationHandler, Serializable {
    private static final long serialVersionUID = 1L;
    private final SecurityOperator operator;
    private final T instance;

    public SecurityObjectWithSecurityReturnObjectProxy(SecurityOperator operator, T instance) {
        this.operator = operator;
        this.instance = instance;
    }

    public static Object createInstance(
            SecurityOperator securityOperator, Object object, Class<?>[] interfaces) {
        SecurityObjectWithSecurityReturnObjectProxy<Object> securityObjectProxy =
                new SecurityObjectWithSecurityReturnObjectProxy<>(securityOperator, object);
        return Proxy.newProxyInstance(
                SecurityObjectWithSecurityReturnObjectProxy.class.getClassLoader(),
                interfaces,
                securityObjectProxy);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        return operator.login(
                () -> {
                    Object invoke = method.invoke(instance, args);
                    if (invoke != null) {
                        Class<?> returnType = method.getReturnType();
                        return SecurityObjectProxy.createInstance(
                                operator, invoke, new Class[] {returnType});
                    }
                    return null;
                });
    }
}
