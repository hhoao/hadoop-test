package org.hhoao.hadoop.test.cluster;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.hhoao.hadoop.test.api.SecurityContext;

/**
 * DefaultSecurityContext
 *
 * @author w
 * @since 2024/9/10
 */
public class NoSecurityContext implements SecurityContext {
    @Override
    public boolean isEnableSecurity() {
        return false;
    }

    @Override
    public Map<String, File> getPrincipalKeytabMap() {
        return Collections.emptyMap();
    }

    @Override
    public MiniKdc getKdc() {
        return null;
    }

    @Override
    public File getDefaultKeytab() {
        return null;
    }

    @Override
    public String getDefaultPrincipal() {
        return "";
    }

    @Override
    public UserGroupInformation getDefaultUGI() throws IOException {
        return UserGroupInformation.getCurrentUser();
    }
}
