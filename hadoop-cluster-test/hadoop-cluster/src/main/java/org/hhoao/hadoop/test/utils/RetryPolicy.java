package org.hhoao.hadoop.test.utils;

/**
 * Specifies a policy for retrying method failures. Implementations of this interface should be
 * immutable.
 */
public interface RetryPolicy {

    /** Returned by {@link RetryPolicy#shouldRetry(Exception, int)}. */
    class RetryAction {

        // A few common retry policies, with no delays.
        public static final RetryAction FAIL = new RetryAction(RetryDecision.FAIL);
        public static final RetryAction RETRY = new RetryAction(RetryDecision.RETRY);

        public final RetryDecision action;
        public final long delayMillis;
        public final String reason;

        public RetryAction(RetryDecision action) {
            this(action, 0, null);
        }

        public RetryAction(RetryDecision action, long delayTime) {
            this(action, delayTime, null);
        }

        public RetryAction(RetryDecision action, long delayTime, String reason) {
            this.action = action;
            this.delayMillis = delayTime;
            this.reason = reason;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName()
                    + "(action="
                    + action
                    + ", delayMillis="
                    + delayMillis
                    + ", reason="
                    + reason
                    + ")";
        }

        public enum RetryDecision {
            FAIL,
            RETRY
        }
    }

    /**
     * Determines whether the framework should retry a method for the given exception, and the
     * number of retries that have been made for that operation so far.
     *
     * @param e The exception that caused the method to fail
     * @param retries The number of times the method has been retried
     * @return <code>true</code> if the method should be retried, <code>false</code> if the method
     *     should not be retried but shouldn't fail with an exception (only for void methods)
     * @throws Exception The re-thrown exception <code>e</code> indicating that the method failed
     *     and should not be retried further
     */
    RetryAction shouldRetry(Exception e, int retries) throws Exception;
}
