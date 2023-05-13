package org.hhoao.hadoop.test.utils;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRequest;
import org.apache.hadoop.yarn.logaggregation.LogCLIHelpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YarnUtils
 *
 * @author xianxing
 * @since 2024 /4/9
 */
public class YarnUtils {
    private static final Logger LOG = LoggerFactory.getLogger(YarnUtils.class);

    /**
     * Gets application log.
     *
     * @param yarnConfiguration the yarn configuration
     * @param applicationId the application id
     * @return the application log
     * @throws Throwable the throwable
     */
    public static String getApplicationLog(
            Configuration yarnConfiguration, ApplicationId applicationId) throws Throwable {
        UserGroupInformation userGroupInformation = UserGroupInformation.getCurrentUser();
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            getApplicationLog(
                    yarnConfiguration,
                    applicationId,
                    userGroupInformation.getUserName(),
                    byteArrayOutputStream);
            return byteArrayOutputStream.toString();
        }
    }

    /**
     * Gets application log.
     *
     * @param yarnConfiguration the yarn configuration
     * @param applicationId the application id
     * @param appOwner the app owner
     * @return the application log
     * @throws Throwable the throwable
     */
    public static String getApplicationLog(
            Configuration yarnConfiguration, ApplicationId applicationId, String appOwner)
            throws Throwable {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            getApplicationLog(yarnConfiguration, applicationId, appOwner, byteArrayOutputStream);
            return byteArrayOutputStream.toString();
        }
    }

    /**
     * Gets application log.
     *
     * @param yarnConfiguration the yarn configuration
     * @param applicationId the application id
     * @param appOwner the app owner
     * @param outputStream the output stream
     * @throws Throwable the throwable
     */
    public static void getApplicationLog(
            Configuration yarnConfiguration,
            ApplicationId applicationId,
            String appOwner,
            OutputStream outputStream)
            throws Throwable {
        LOG.info("Try to get application log {}", applicationId);
        try (PrintStream printStream = new PrintStream(outputStream)) {
            LogCLIHelpers logCliHelper = new LogCLIHelpers();
            logCliHelper.setConf(yarnConfiguration);
            for (int times = 0; times < 20; times++) {
                Path tempFile = Files.createTempFile("log_agg", "log");
                ContainerLogsRequest containerLogsRequest = new ContainerLogsRequest();
                containerLogsRequest.setAppId(applicationId);
                containerLogsRequest.setAppOwner(appOwner);
                containerLogsRequest.setOutputLocalDir(tempFile.toFile().getAbsolutePath());
                int i = logCliHelper.dumpAllContainersLogs(containerLogsRequest);
                if (i == -1) {
                    TimeUnit.SECONDS.sleep(5);
                    continue;
                }
                Files.copy(tempFile, printStream);
                break;
            }
        }
    }
}
