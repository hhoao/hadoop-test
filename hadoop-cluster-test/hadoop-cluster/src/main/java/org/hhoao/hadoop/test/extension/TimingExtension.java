package org.hhoao.hadoop.test.extension;

import java.lang.reflect.Method;
import org.junit.jupiter.api.extension.*;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimingExtension
        implements BeforeTestExecutionCallback,
                AfterTestExecutionCallback,
                BeforeAllCallback,
                AfterAllCallback {

    private static final Logger LOG = LoggerFactory.getLogger(TimingExtension.class);

    private static final String START_TIME = "START_TIME";

    @Override
    public void beforeTestExecution(ExtensionContext context) throws Exception {
        Method testMethod = context.getRequiredTestMethod();
        Class<?> aClass = context.getTestClass().get();
        LOG.info(
                String.format(
                        "Class %s method [%s] start test", aClass.getName(), testMethod.getName()));
        getStore(context).put(START_TIME, System.currentTimeMillis());
    }

    @Override
    public void afterTestExecution(ExtensionContext context) throws Exception {
        Method testMethod = context.getRequiredTestMethod();
        Class<?> aClass = context.getTestClass().get();
        long startTime = getStore(context).remove(START_TIME, long.class);
        long duration = System.currentTimeMillis() - startTime;

        LOG.info(
                String.format(
                        "Class %s method [%s] test took %s ms.",
                        aClass.getName(), testMethod.getName(), duration));
        LOG.info(
                String.format(
                        "Class %s method [%s] down test", aClass.getName(), testMethod.getName()));
    }

    private Store getStore(ExtensionContext context) {
        return context.getStore(Namespace.create(getClass(), context.getRequiredTestMethod()));
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        Class<?> aClass = context.getTestClass().get();
        LOG.info(String.format("Class %s down test", aClass.getName()));
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        Class<?> aClass = context.getTestClass().get();
        LOG.info(String.format("Class %s start test", aClass.getName()));
    }
}
