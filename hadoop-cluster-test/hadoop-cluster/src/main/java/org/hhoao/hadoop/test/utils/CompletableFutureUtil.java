package org.hhoao.hadoop.test.utils;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class CompletableFutureUtil {
    private static final ScheduledThreadPoolExecutor SCHEDULED_EXECUTOR_SERVICE;

    static {
        SCHEDULED_EXECUTOR_SERVICE =
                new ScheduledThreadPoolExecutor(
                        Integer.MAX_VALUE, (r) -> new Thread(r, "Timeout Timer"));
        SCHEDULED_EXECUTOR_SERVICE.setKeepAliveTime(30, TimeUnit.SECONDS);
        SCHEDULED_EXECUTOR_SERVICE.allowCoreThreadTimeOut(true);
    }

    public static <T> T timeoutGetWithStack(
            Supplier<T> supplier,
            long timeout,
            TimeUnit timeUnit,
            Consumer<String> stackTraceConsumer)
            throws Exception {
        return retryTimeoutInterrupt(
                () -> {
                    try {
                        return supplier.get();
                    } catch (Exception e) {
                        throw new RuntimeException("Get error", e);
                    }
                },
                timeout,
                timeUnit,
                (t) -> {
                    StackTraceElement[] stackTrace = t.getStackTrace();
                    StringBuilder trace = new StringBuilder();
                    for (StackTraceElement traceElement : stackTrace) {
                        trace.append("\n").append("\tat ").append(traceElement);
                    }
                    stackTraceConsumer.accept(trace.toString());
                },
                3);
    }

    public static <T> T retryTimeoutInterrupt(
            Supplier<T> supplier, long timeout, TimeUnit unit, int retryTimes) throws Exception {
        return retryTimeoutInterrupt(supplier, timeout, unit, null, retryTimes);
    }

    public static <T> T retryTimeoutInterrupt(
            Supplier<T> supplier,
            long timeout,
            TimeUnit unit,
            Consumer<Thread> actionBeforeInterrupt,
            int retryTimes)
            throws Exception {
        RetryPolicy policy = RetryPolicies.exponentialBackoffRetry(retryTimes, timeout, unit);
        Exception finalException = new Exception();
        int i = 0;
        RetryPolicy.RetryAction retryAction;
        while (RetryPolicy.RetryAction.RetryDecision.RETRY
                == (retryAction = policy.shouldRetry(finalException, i++)).action) {
            try {
                return timeOutInterrupt(
                        supplier,
                        retryAction.delayMillis,
                        TimeUnit.MILLISECONDS,
                        actionBeforeInterrupt);
            } catch (ExecutionException | InterruptedException e) {
                finalException = e;
                if (Thread.interrupted()
                        || (e instanceof ExecutionException && !causeByInterruptedException(e))) {
                    break;
                }
            }
        }
        throw finalException;
    }

    private static boolean causeByInterruptedException(Throwable e) {
        if (e.getCause() != null) {
            if (e.getCause() instanceof InterruptedException) {
                return true;
            } else {
                return causeByInterruptedException(e.getCause());
            }
        } else {
            return false;
        }
    }

    public static <T> T timeOutInterrupt(Supplier<T> supplier, long timeout, TimeUnit unit)
            throws ExecutionException, InterruptedException {
        return timeOutInterrupt(supplier, timeout, unit, null, null);
    }

    public static <T> T timeOutInterrupt(
            Supplier<T> supplier, long timeout, TimeUnit unit, String threadName)
            throws ExecutionException, InterruptedException {
        return timeOutInterrupt(supplier, timeout, unit, null, threadName);
    }

    public static <T> T timeOutInterrupt(
            Supplier<T> supplier,
            long timeout,
            TimeUnit unit,
            Consumer<Thread> actionBeforeInterrupt)
            throws ExecutionException, InterruptedException {
        return timeOutInterrupt(supplier, timeout, unit, actionBeforeInterrupt, null);
    }

    public static <T> T timeOutInterrupt(
            Supplier<T> supplier,
            long timeout,
            TimeUnit unit,
            Consumer<Thread> actionBeforeInterrupt,
            String threadName)
            throws ExecutionException, InterruptedException {
        AtomicReference<Thread> currentThread = new AtomicReference<>();
        ExecutorService executorService =
                Executors.newSingleThreadExecutor(
                        (r) -> {
                            Thread thread =
                                    threadName != null ? new Thread(r, threadName) : new Thread(r);
                            currentThread.set(thread);
                            return thread;
                        });
        ScheduledFuture<List<Runnable>> schedule =
                SCHEDULED_EXECUTOR_SERVICE.schedule(
                        () -> {
                            if (currentThread.get() != null) {
                                actionBeforeInterrupt.accept(currentThread.get());
                            }
                            return executorService.shutdownNow();
                        },
                        timeout,
                        unit);

        CompletableFuture<T> future =
                CompletableFuture.supplyAsync(supplier, executorService)
                        .whenComplete(
                                (r, t) -> {
                                    schedule.cancel(true);
                                });
        return future.get();
    }
}
