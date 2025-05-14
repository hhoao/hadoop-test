package org.hhoao.hadoop.test.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RetryPolicies {

    public static final Log LOG = LogFactory.getLog(RetryPolicies.class);

    private static final ThreadLocal<Random> RANDOM = ThreadLocal.withInitial(Random::new);

    /**
     * Try once, and fail by re-throwing the exception. This corresponds to having no retry
     * mechanism in place.
     */
    public static final RetryPolicy TRY_ONCE_THEN_FAIL = new TryOnceThenFail();

    /** Keep trying forever. */
    public static final RetryPolicy RETRY_FOREVER = new RetryForever();

    public static RetryPolicy intervalRetryForever(long sleepTime, TimeUnit timeUnit) {
        return new IntervalRetryForever(sleepTime, timeUnit);
    }
    /**
     * Keep trying a limited number of times, waiting a fixed time between attempts, and then fail
     * by re-throwing the exception.
     */
    public static RetryPolicy retryUpToMaximumCountWithFixedSleep(
            int maxRetries, long sleepTime, TimeUnit timeUnit) {
        return new RetryUpToMaximumCountWithFixedSleep(maxRetries, sleepTime, timeUnit);
    }

    /**
     * Keep trying for a maximum time, waiting a fixed time between attempts, and then fail by
     * re-throwing the exception.
     */
    public static RetryPolicy retryUpToMaximumTimeWithFixedSleep(
            long maxTime, long sleepTime, TimeUnit timeUnit) {
        return new RetryUpToMaximumTimeWithFixedSleep(maxTime, sleepTime, timeUnit);
    }

    /**
     * Keep trying a limited number of times, waiting a growing amount of time between attempts, and
     * then fail by re-throwing the exception. The time between attempts is <code>sleepTime</code>
     * mutliplied by the number of tries so far.
     */
    public static RetryPolicy retryUpToMaximumCountWithProportionalSleep(
            int maxRetries, long sleepTime, TimeUnit timeUnit) {
        return new RetryUpToMaximumCountWithProportionalSleep(maxRetries, sleepTime, timeUnit);
    }

    /**
     * Keep trying a limited number of times, waiting a growing amount of time between attempts, and
     * then fail by re-throwing the exception. The time between attempts is <code>sleepTime</code>
     * mutliplied by a random number in the range of [0, 2 to the number of retries)
     */
    public static RetryPolicy exponentialBackoffRetry(
            int maxRetries, long sleepTime, TimeUnit timeUnit) {
        return new ExponentialBackoffRetry(maxRetries, sleepTime, timeUnit);
    }

    public static RetryPolicy exponentialForeverRetry(long sleepTime, TimeUnit timeUnit) {
        return new ExponentialForeverRetry(sleepTime, timeUnit);
    }

    /** Set a default policy with some explicit handlers for specific exceptions. */
    public static RetryPolicy retryByException(
            RetryPolicy defaultPolicy,
            Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap) {
        return new ExceptionDependentRetry(defaultPolicy, exceptionToPolicyMap);
    }

    static class TryOnceThenFail implements RetryPolicy {
        @Override
        public RetryAction shouldRetry(Exception e, int retries) throws Exception {
            return RetryAction.FAIL;
        }
    }

    static class RetryForever implements RetryPolicy {
        @Override
        public RetryAction shouldRetry(Exception e, int retries) throws Exception {
            return RetryAction.RETRY;
        }
    }

    static class IntervalRetryForever implements RetryPolicy {
        final long sleepTime;
        final TimeUnit timeUnit;

        IntervalRetryForever(long sleepTime, TimeUnit timeUnit) {
            if (sleepTime < 0) {
                throw new IllegalArgumentException("sleepTime = " + sleepTime + " < 0");
            }

            this.sleepTime = sleepTime;
            this.timeUnit = timeUnit;
        }

        @Override
        public RetryAction shouldRetry(Exception e, int retries) throws Exception {
            return new RetryAction(RetryAction.RetryDecision.RETRY, timeUnit.toMillis(sleepTime));
        }
    }

    /**
     * Retry up to maxRetries. The actual sleep time of the n-th retry is f(n, sleepTime), where f
     * is a function provided by the subclass implementation. The object of the subclasses should be
     * immutable; otherwise, the subclass must override hashCode(), equals(..) and toString().
     */
    abstract static class RetryLimited implements RetryPolicy {
        final int maxRetries;
        final long sleepTime;
        final TimeUnit timeUnit;

        private String myString;

        RetryLimited(int maxRetries, long sleepTime, TimeUnit timeUnit) {
            if (maxRetries < 0) {
                throw new IllegalArgumentException("maxRetries = " + maxRetries + " < 0");
            }
            if (sleepTime < 0) {
                throw new IllegalArgumentException("sleepTime = " + sleepTime + " < 0");
            }

            this.maxRetries = maxRetries;
            this.sleepTime = sleepTime;
            this.timeUnit = timeUnit;
        }

        @Override
        public RetryAction shouldRetry(Exception e, int retries) throws Exception {
            if (retries >= maxRetries) {
                return RetryAction.FAIL;
            }
            return new RetryAction(
                    RetryAction.RetryDecision.RETRY,
                    timeUnit.toMillis(calculateSleepTime(retries)));
        }

        protected abstract long calculateSleepTime(int retries);

        @Override
        public int hashCode() {
            return toString().hashCode();
        }

        @Override
        public boolean equals(final Object that) {
            if (this == that) {
                return true;
            } else if (that == null || this.getClass() != that.getClass()) {
                return false;
            }
            return this.toString().equals(that.toString());
        }

        @Override
        public String toString() {
            if (myString == null) {
                myString =
                        getClass().getSimpleName()
                                + "(maxRetries="
                                + maxRetries
                                + ", sleepTime="
                                + sleepTime
                                + " "
                                + timeUnit
                                + ")";
            }
            return myString;
        }
    }

    static class RetryUpToMaximumCountWithFixedSleep extends RetryLimited {
        public RetryUpToMaximumCountWithFixedSleep(
                int maxRetries, long sleepTime, TimeUnit timeUnit) {
            super(maxRetries, sleepTime, timeUnit);
        }

        @Override
        protected long calculateSleepTime(int retries) {
            return sleepTime;
        }
    }

    static class RetryUpToMaximumTimeWithFixedSleep extends RetryUpToMaximumCountWithFixedSleep {
        public RetryUpToMaximumTimeWithFixedSleep(long maxTime, long sleepTime, TimeUnit timeUnit) {
            super((int) (maxTime / sleepTime), sleepTime, timeUnit);
        }
    }

    static class RetryUpToMaximumCountWithProportionalSleep extends RetryLimited {
        public RetryUpToMaximumCountWithProportionalSleep(
                int maxRetries, long sleepTime, TimeUnit timeUnit) {
            super(maxRetries, sleepTime, timeUnit);
        }

        @Override
        protected long calculateSleepTime(int retries) {
            return sleepTime * (retries + 1);
        }
    }

    /**
     * Given pairs of number of retries and sleep time (n0, t0), (n1, t1), ..., the first n0 retries
     * sleep t0 milliseconds on average, the following n1 retries sleep t1 milliseconds on average,
     * and so on. For all the sleep, the actual sleep time is randomly uniform distributed in the
     * close interval [0.5t, 1.5t], where t is the sleep time specified. The objects of this class
     * are immutable.
     */
    public static class MultipleLinearRandomRetry implements RetryPolicy {
        /** Pairs of numRetries and sleepSeconds */
        public static class Pair {
            final int numRetries;
            final int sleepMillis;

            public Pair(final int numRetries, final int sleepMillis) {
                if (numRetries < 0) {
                    throw new IllegalArgumentException("numRetries = " + numRetries + " < 0");
                }
                if (sleepMillis < 0) {
                    throw new IllegalArgumentException("sleepMillis = " + sleepMillis + " < 0");
                }

                this.numRetries = numRetries;
                this.sleepMillis = sleepMillis;
            }

            @Override
            public String toString() {
                return numRetries + "x" + sleepMillis + "ms";
            }
        }

        private final List<Pair> pairs;
        private String myString;

        public MultipleLinearRandomRetry(List<Pair> pairs) {
            if (pairs == null || pairs.isEmpty()) {
                throw new IllegalArgumentException("pairs must be neither null nor empty.");
            }
            this.pairs = Collections.unmodifiableList(pairs);
        }

        @Override
        public RetryAction shouldRetry(Exception e, int curRetry) throws Exception {
            final Pair p = searchPair(curRetry);
            if (p == null) {
                // no more retries.
                return RetryAction.FAIL;
            }

            // calculate sleep time and return.
            final double ratio = RANDOM.get().nextDouble() + 0.5; // 0.5 <= ratio <=1.5
            final long sleepTime = Math.round(p.sleepMillis * ratio);
            return new RetryAction(RetryAction.RetryDecision.RETRY, sleepTime);
        }

        /**
         * Given the current number of retry, search the corresponding pair.
         *
         * @return the corresponding pair, or null if the current number of retry > maximum number
         *     of retry.
         */
        private Pair searchPair(int curRetry) {
            int i = 0;
            for (; i < pairs.size() && curRetry > pairs.get(i).numRetries; i++) {
                curRetry -= pairs.get(i).numRetries;
            }
            return i == pairs.size() ? null : pairs.get(i);
        }

        @Override
        public int hashCode() {
            return toString().hashCode();
        }

        @Override
        public boolean equals(final Object that) {
            if (this == that) {
                return true;
            } else if (that == null || this.getClass() != that.getClass()) {
                return false;
            }
            return this.toString().equals(that.toString());
        }

        @Override
        public String toString() {
            if (myString == null) {
                myString = getClass().getSimpleName() + pairs;
            }
            return myString;
        }

        /**
         * Parse the given string as a MultipleLinearRandomRetry object. The format of the string is
         * "t_1, n_1, t_2, n_2, ...", where t_i and n_i are the i-th pair of sleep time and number
         * of retires. Note that the white spaces in the string are ignored.
         *
         * @return the parsed object, or null if the parsing fails.
         */
        public static MultipleLinearRandomRetry parseCommaSeparatedString(String s) {
            final String[] elements = s.split(",");
            if (elements.length == 0) {
                LOG.warn("Illegal value: there is no element in \"" + s + "\".");
                return null;
            }
            if (elements.length % 2 != 0) {
                LOG.warn(
                        "Illegal value: the number of elements in \""
                                + s
                                + "\" is "
                                + elements.length
                                + " but an even number of elements is expected.");
                return null;
            }

            final List<Pair> pairs = new ArrayList<>();

            for (int i = 0; i < elements.length; ) {
                // parse the i-th sleep-time
                final int sleep = parsePositiveInt(elements, i++, s);
                if (sleep == -1) {
                    return null; // parse fails
                }

                // parse the i-th number-of-retries
                final int retries = parsePositiveInt(elements, i++, s);
                if (retries == -1) {
                    return null; // parse fails
                }

                pairs.add(new RetryPolicies.MultipleLinearRandomRetry.Pair(retries, sleep));
            }
            return new RetryPolicies.MultipleLinearRandomRetry(pairs);
        }

        /**
         * Parse the i-th element as an integer.
         *
         * @return -1 if the parsing fails or the parsed value <= 0; otherwise, return the parsed
         *     value.
         */
        private static int parsePositiveInt(
                final String[] elements, final int i, final String originalString) {
            final String s = elements[i].trim();
            final int n;
            try {
                n = Integer.parseInt(s);
            } catch (NumberFormatException nfe) {
                LOG.warn(
                        "Failed to parse \""
                                + s
                                + "\", which is the index "
                                + i
                                + " element in \""
                                + originalString
                                + "\"",
                        nfe);
                return -1;
            }

            if (n <= 0) {
                LOG.warn(
                        "The value "
                                + n
                                + " <= 0: it is parsed from the string \""
                                + s
                                + "\" which is the index "
                                + i
                                + " element in \""
                                + originalString
                                + "\"");
                return -1;
            }
            return n;
        }
    }

    static class ExceptionDependentRetry implements RetryPolicy {

        RetryPolicy defaultPolicy;
        Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap;

        public ExceptionDependentRetry(
                RetryPolicy defaultPolicy,
                Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap) {
            this.defaultPolicy = defaultPolicy;
            this.exceptionToPolicyMap = exceptionToPolicyMap;
        }

        @Override
        public RetryAction shouldRetry(Exception e, int retries) throws Exception {
            RetryPolicy policy = exceptionToPolicyMap.get(e.getClass());
            if (policy == null) {
                policy = defaultPolicy;
            }
            return policy.shouldRetry(e, retries);
        }
    }

    static class ExponentialBackoffRetry extends RetryLimited {

        public ExponentialBackoffRetry(int maxRetries, long sleepTime, TimeUnit timeUnit) {
            super(maxRetries, sleepTime, timeUnit);

            if (maxRetries < 0) {
                throw new IllegalArgumentException("maxRetries = " + maxRetries + " < 0");
            } else if (maxRetries >= Long.SIZE - 1) {
                // calculateSleepTime may overflow.
                throw new IllegalArgumentException(
                        "maxRetries = " + maxRetries + " >= " + (Long.SIZE - 1));
            }
        }

        @Override
        protected long calculateSleepTime(int retries) {
            return calculateExponentialTime(sleepTime, retries);
        }
    }

    static class ExponentialForeverRetry extends RetryForever {
        final long sleepTime;
        final TimeUnit timeUnit;

        public ExponentialForeverRetry(long sleepTime, TimeUnit timeUnit) {
            if (sleepTime < 0) {
                throw new IllegalArgumentException("sleepTime = " + sleepTime + " < 0");
            }

            this.sleepTime = sleepTime;
            this.timeUnit = timeUnit;
        }

        @Override
        public RetryAction shouldRetry(Exception e, int retries) throws Exception {
            return new RetryAction(
                    RetryAction.RetryDecision.RETRY,
                    timeUnit.toMillis(calculateSleepTime(retries)));
        }

        private long calculateSleepTime(int retries) {
            return calculateExponentialTime(sleepTime, retries);
        }
    }

    /**
     * Return a value which is <code>time</code> increasing exponentially as a function of <code>
     * retries</code>.
     *
     * @param time the base amount of time to work with
     * @param retries the number of retries that have so occurred so far
     * @param cap value at which to cap the base sleep time
     * @return an amount of time to sleep
     */
    private static long calculateExponentialTime(long time, int retries, long cap) {
        return Math.min(time * (1L << retries), cap);
    }

    private static long calculateExponentialTime(long time, int retries) {
        return calculateExponentialTime(time, retries, Long.MAX_VALUE);
    }
}
