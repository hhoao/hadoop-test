package org.hhoao.hadoop.test.security;

import org.hhoao.hadoop.test.utils.SupplierWithException;

/**
 * KerberosSecurityOperator
 *
 * @author w
 * @since 2024/9/12
 */
public class NoSecurityOperator implements SecurityOperator {
    private static final long serialVersionUID = 1L;

    @Override
    public <T> T login(SupplierWithException<T, Throwable> supplier) throws Throwable {
        return supplier.get();
    }
}
