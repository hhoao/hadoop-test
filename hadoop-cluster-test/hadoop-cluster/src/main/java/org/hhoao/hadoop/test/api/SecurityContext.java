package org.hhoao.hadoop.test.api;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * SecurityContext
 *
 * @author w
 * @since 2024/9/10
 */
public interface SecurityContext {
    boolean isEnableSecurity();

    Map<String, File> getPrincipalKeytabMap();

    MiniKdc getKdc();

    File getDefaultKeytab();

    String getDefaultPrincipal();

    UserGroupInformation getDefaultUGI() throws IOException;
}
