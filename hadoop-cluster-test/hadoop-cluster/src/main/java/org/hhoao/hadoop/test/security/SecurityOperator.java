package org.hhoao.hadoop.test.security;

import java.io.Serializable;
import org.hhoao.hadoop.test.utils.SupplierWithException;

/** The interface Security operator. */
public interface SecurityOperator extends Serializable {
    /**
     * Login t.
     *
     * @param <T> the type parameter
     * @param supplier the supplier
     * @return the t
     */
    <T> T login(SupplierWithException<T, Throwable> supplier) throws Throwable;
}
