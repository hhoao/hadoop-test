package org.hhoao.hadoop.test.utils;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.log4j.RollingFileAppender;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.core.layout.PatternLayout;

/**
 * LoggerUtils
 *
 * @author w
 * @since 2024/9/10
 */
public class LoggerUtils {
    public static void closeLogger() {
        Configurator.setRootLevel(Level.OFF);
        org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
        rootLogger.setLevel(org.apache.log4j.Level.OFF);
    }

    public static void changeAppendAllLogToFile() throws IOException {
        File loggerDir = new File(Resources.getTargetDir(), "log");
        loggerDir.mkdirs();
        File logFile = new File(loggerDir, "logFile.log");
        String size = "2MB";
        logAppendToFile1(logFile, size);
        logAppendToFile2(logFile, size);
    }

    private static void logAppendToFile2(File logFile, String size) throws IOException {
        org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
        rootLogger.removeAllAppenders();

        RollingFileAppender rollingFileAppender = new RollingFileAppender();
        org.apache.log4j.PatternLayout patternLayout = new org.apache.log4j.PatternLayout();
        patternLayout.setConversionPattern("[%d{yyyy-MM-dd HH:mm:ss}] [%p] (%c:%L) [%t] - %m%n");
        rollingFileAppender.setLayout(patternLayout);
        rollingFileAppender.setName("GeneratedFileAppender");
        rollingFileAppender.setFile(logFile.getAbsolutePath(), true, true, 1024);
        rollingFileAppender.setMaxFileSize(size);
        rootLogger.addAppender(rollingFileAppender);
    }

    private static void logAppendToFile1(File logFile, String size) {
        final LoggerContext loggerContext = LoggerContext.getContext(false);
        final LoggerConfig loggerConfig = loggerContext.getConfiguration().getRootLogger();
        Map<String, Appender> appenders = loggerConfig.getAppenders();
        appenders.forEach(
                (name, appender) -> {
                    loggerConfig.removeAppender(name);
                });

        PatternLayout layout =
                PatternLayout.newBuilder()
                        .withPattern("[%d{yyyy-MM-dd HH:mm:ss}] [%p] (%c:%L) [%t] - %m%n")
                        .build();

        org.apache.logging.log4j.core.appender.RollingFileAppender fileAppender =
                org.apache.logging.log4j.core.appender.RollingFileAppender.newBuilder()
                        .setLayout(layout)
                        .setName("GeneratedFileAppender")
                        .withFileName(logFile.getAbsolutePath())
                        .withFilePattern("logFile")
                        .withPolicy(SizeBasedTriggeringPolicy.createPolicy(size))
                        .withAppend(true)
                        .build();
        fileAppender.start();
        loggerConfig.addAppender(
                fileAppender,
                Level.INFO,
                new AbstractFilter() {
                    @Override
                    protected boolean equalsImpl(Object obj) {
                        return super.equalsImpl(obj);
                    }
                });
        loggerContext.updateLoggers();
    }
}
