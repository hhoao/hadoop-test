package org.hhoao.hadoop.test.extension;

import java.util.Optional;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TestWatcherExtension
 *
 * @author xianxing
 * @since 2024/4/9
 */
public class TestWatcherExtension implements TestWatcher {
    private static final Logger LOG = LoggerFactory.getLogger(TestWatcherExtension.class);

    @Override
    public void testDisabled(ExtensionContext context, Optional<String> reason) {
        LOG.info(
                "Test class: {}, method: {}, instance: {} is Disabled",
                context.getTestClass(),
                context.getTestMethod().get(),
                context.getTestInstance());
    }

    @Override
    public void testSuccessful(ExtensionContext context) {
        LOG.info(
                "Test class: {}, method: {}, instance: {} is Successful",
                context.getTestClass(),
                context.getTestMethod().get(),
                context.getTestInstance());
    }

    @Override
    public void testAborted(ExtensionContext context, Throwable cause) {
        LOG.info(
                "Test class: {}, method: {}, instance: {} is Aborted",
                context.getTestClass(),
                context.getTestMethod().get(),
                context.getTestInstance());
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        LOG.info(
                "Test class: {}, method: {}, instance: {} is Failed",
                context.getTestClass(),
                context.getTestMethod().get(),
                context.getTestInstance());
    }
}
