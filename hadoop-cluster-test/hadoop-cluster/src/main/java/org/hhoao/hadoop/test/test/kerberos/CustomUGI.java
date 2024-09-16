package org.hhoao.hadoop.test.test.kerberos;

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import org.apache.hadoop.util.PlatformName;

/**
 * TestAccessController
 *
 * @author w
 * @since 2024/9/11
 */
public class CustomUGI {
    private static final boolean windows = System.getProperty("os.name").startsWith("Windows");
    private static final boolean is64Bit =
            System.getProperty("os.arch").contains("64")
                    || System.getProperty("os.arch").contains("s390x");
    private static final boolean aix = System.getProperty("os.name").equals("AIX");
    private static final Logger LOG = Logger.getLogger(String.valueOf(CustomUGI.class));
    private static final String OS_LOGIN_MODULE_NAME;
    private static final Class<? extends Principal> OS_PRINCIPAL_CLASS;

    static {
        OS_LOGIN_MODULE_NAME = getOSLoginModuleName();
        OS_PRINCIPAL_CLASS = getOsPrincipalClass();
    }

    public static synchronized Subject loginUserFromKeytab(
            String keytabPrincipal, String keytabFile) throws LoginException {
        Subject subject = new Subject();
        LoginContext login;
        login =
                newLoginContext(
                        ConfigNameEnum.KEYTAB_KERBEROS.configName,
                        subject,
                        new KerberosConfiguration(keytabFile, keytabPrincipal));
        login.login();
        LOG.info(
                "Login successful for user "
                        + keytabPrincipal
                        + " using keytab file "
                        + keytabFile);
        return subject;
    }

    private static LoginContext newLoginContext(
            String appName, Subject subject, javax.security.auth.login.Configuration loginConf)
            throws LoginException {
        // Temporarily switch the thread's ContextClassLoader to match this
        // class's classloader, so that we can properly load HadoopLoginModule
        // from the JAAS libraries.
        Thread t = Thread.currentThread();
        ClassLoader oldCCL = t.getContextClassLoader();
        t.setContextClassLoader(CustomUGI.class.getClassLoader());
        try {
            return new LoginContext(appName, subject, null, loginConf);
        } finally {
            t.setContextClassLoader(oldCCL);
        }
    }

    enum ConfigNameEnum {
        SIMPLE("simple"),
        USER_KERBEROS("user-kerberos"),
        KEYTAB_KERBEROS("keytab-kerberos");

        private final String configName;

        ConfigNameEnum(String configName) {
            this.configName = configName;
        }

        public String getConfigName() {
            return configName;
        }
    }

    private static class KerberosConfiguration extends javax.security.auth.login.Configuration {

        private static final Map<String, String> BASIC_JAAS_OPTIONS = new HashMap<>();

        static {
            String jaasEnvVar = System.getenv("HADOOP_JAAS_DEBUG");
            if ("true".equalsIgnoreCase(jaasEnvVar)) {
                BASIC_JAAS_OPTIONS.put("debug", "true");
            }
        }

        private static final AppConfigurationEntry OS_SPECIFIC_LOGIN =
                new AppConfigurationEntry(
                        OS_LOGIN_MODULE_NAME,
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        BASIC_JAAS_OPTIONS);
        private static final AppConfigurationEntry HADOOP_LOGIN =
                new AppConfigurationEntry(
                        CustomLoginModule.class.getName(),
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        BASIC_JAAS_OPTIONS);
        private static final Map<String, String> USER_KERBEROS_OPTIONS = new HashMap<>();

        static {
            if (PlatformName.IBM_JAVA) {
                USER_KERBEROS_OPTIONS.put("useDefaultCcache", "true");
            } else {
                USER_KERBEROS_OPTIONS.put("doNotPrompt", "true");
                USER_KERBEROS_OPTIONS.put("useTicketCache", "true");
            }
            String ticketCache = System.getenv("KRB5CCNAME");
            if (ticketCache != null) {
                if (PlatformName.IBM_JAVA) {
                    // The first value searched when "useDefaultCcache" is used.
                    System.setProperty("KRB5CCNAME", ticketCache);
                } else {
                    USER_KERBEROS_OPTIONS.put("ticketCache", ticketCache);
                }
            }
            USER_KERBEROS_OPTIONS.put("renewTGT", "true");
            USER_KERBEROS_OPTIONS.putAll(BASIC_JAAS_OPTIONS);
        }

        private static final AppConfigurationEntry USER_KERBEROS_LOGIN =
                new AppConfigurationEntry(
                        getKrb5LoginModuleName(),
                        AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL,
                        USER_KERBEROS_OPTIONS);
        private static final Map<String, String> KEYTAB_KERBEROS_OPTIONS = new HashMap<>();

        static {
            if (PlatformName.IBM_JAVA) {
                KEYTAB_KERBEROS_OPTIONS.put("credsType", "both");
            } else {
                KEYTAB_KERBEROS_OPTIONS.put("doNotPrompt", "true");
                KEYTAB_KERBEROS_OPTIONS.put("useKeyTab", "true");
                KEYTAB_KERBEROS_OPTIONS.put("storeKey", "true");
            }
            KEYTAB_KERBEROS_OPTIONS.put("refreshKrb5Config", "true");
            KEYTAB_KERBEROS_OPTIONS.putAll(BASIC_JAAS_OPTIONS);
        }

        private static final AppConfigurationEntry KEYTAB_KERBEROS_LOGIN =
                new AppConfigurationEntry(
                        getKrb5LoginModuleName(),
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        KEYTAB_KERBEROS_OPTIONS);

        private static final AppConfigurationEntry[] SIMPLE_CONF =
                new AppConfigurationEntry[] {OS_SPECIFIC_LOGIN, HADOOP_LOGIN};

        private static final AppConfigurationEntry[] USER_KERBEROS_CONF =
                new AppConfigurationEntry[] {OS_SPECIFIC_LOGIN, USER_KERBEROS_LOGIN, HADOOP_LOGIN};

        private static final AppConfigurationEntry[] KEYTAB_KERBEROS_CONF =
                new AppConfigurationEntry[] {KEYTAB_KERBEROS_LOGIN, HADOOP_LOGIN};

        private final String keytabFile;
        private final String keytabPrincipal;

        public KerberosConfiguration(String keytabFile, String keytabPrincipal) {
            this.keytabFile = keytabFile;
            this.keytabPrincipal = keytabPrincipal;
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
            if (ConfigNameEnum.SIMPLE.getConfigName().equals(appName)) {
                return SIMPLE_CONF;
            } else if (ConfigNameEnum.USER_KERBEROS.getConfigName().equals(appName)) {
                return USER_KERBEROS_CONF;
            } else if (ConfigNameEnum.KEYTAB_KERBEROS.getConfigName().equals(appName)) {
                if (PlatformName.IBM_JAVA) {
                    KEYTAB_KERBEROS_OPTIONS.put("useKeytab", prependFileAuthority(keytabFile));
                } else {
                    KEYTAB_KERBEROS_OPTIONS.put("keyTab", keytabFile);
                }
                KEYTAB_KERBEROS_OPTIONS.put("principal", keytabPrincipal);
                return KEYTAB_KERBEROS_CONF;
            }
            return null;
        }
    }

    private static String prependFileAuthority(String keytabPath) {
        return keytabPath.startsWith("file://") ? keytabPath : "file://" + keytabPath;
    }

    private static String getOSLoginModuleName() {
        if (PlatformName.IBM_JAVA) {
            if (windows) {
                return is64Bit
                        ? "com.ibm.security.auth.module.Win64LoginModule"
                        : "com.ibm.security.auth.module.NTLoginModule";
            } else if (aix) {
                return is64Bit
                        ? "com.ibm.security.auth.module.AIX64LoginModule"
                        : "com.ibm.security.auth.module.AIXLoginModule";
            } else {
                return "com.ibm.security.auth.module.LinuxLoginModule";
            }
        } else {
            return windows
                    ? "com.sun.security.auth.module.NTLoginModule"
                    : "com.sun.security.auth.module.UnixLoginModule";
        }
    }

    private static Class<? extends Principal> getOsPrincipalClass() {
        ClassLoader cl = ClassLoader.getSystemClassLoader();
        try {
            String principalClass = null;
            if (PlatformName.IBM_JAVA) {
                if (is64Bit) {
                    principalClass = "com.ibm.security.auth.UsernamePrincipal";
                } else {
                    if (windows) {
                        principalClass = "com.ibm.security.auth.NTUserPrincipal";
                    } else if (aix) {
                        principalClass = "com.ibm.security.auth.AIXPrincipal";
                    } else {
                        principalClass = "com.ibm.security.auth.LinuxPrincipal";
                    }
                }
            } else {
                principalClass =
                        windows
                                ? "com.sun.security.auth.NTUserPrincipal"
                                : "com.sun.security.auth.UnixPrincipal";
            }
            return (Class<? extends Principal>) cl.loadClass(principalClass);
        } catch (ClassNotFoundException e) {
            LOG.warning("Unable to find JAAS classes:" + e.getMessage());
        }
        return null;
    }

    public static String getKrb5LoginModuleName() {
        return System.getProperty("java.vendor").contains("IBM")
                ? "com.ibm.security.auth.module.Krb5LoginModule"
                : "com.sun.security.auth.module.Krb5LoginModule";
    }

    public static class CustomLoginModule implements javax.security.auth.spi.LoginModule {
        private Subject subject;

        @Override
        public boolean abort() throws LoginException {
            return true;
        }

        private <T extends Principal> T getCanonicalUser(Class<T> cls) {
            for (T user : subject.getPrincipals(cls)) {
                return user;
            }
            return null;
        }

        @Override
        public boolean commit() throws LoginException {
            LOG.info("login commit");
            Principal user = getCanonicalUser(KerberosPrincipal.class);
            if (user != null) {
                LOG.info("Using kerberos user: " + user);
            }

            // use the OS user
            if (user == null) {
                user = getCanonicalUser(OS_PRINCIPAL_CLASS);
                LOG.info("Using local user: {}" + user);
            }
            if (user != null) {
                LOG.info("Using local user: {}" + user);
                return true;
            }
            throw new LoginException("Failed to find user in name " + subject);
        }

        @Override
        public void initialize(
                Subject subject,
                CallbackHandler callbackHandler,
                Map<String, ?> sharedState,
                Map<String, ?> options) {
            this.subject = subject;
        }

        @Override
        public boolean login() throws LoginException {
            LOG.info("login");
            return true;
        }

        @Override
        public boolean logout() {
            LOG.info("logout");
            return true;
        }
    }
}
