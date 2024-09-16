package org.hhoao.hadoop.test.cluster;

import java.io.File;
import java.io.IOException;
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
public class DefaultSecurityContext implements SecurityContext {

    private final MiniKdc kdc;
    private final Map<String, File> principalKeytabMap;
    private final String defaultPrincipal;
    private final File defaultKeytab;

    public DefaultSecurityContext(
            MiniKdc kdc,
            Map<String, File> principalKeytabMap,
            String defaultPrincipal,
            File defaultKeytab) {
        this.kdc = kdc;
        this.principalKeytabMap = principalKeytabMap;
        this.defaultPrincipal = defaultPrincipal;
        this.defaultKeytab = defaultKeytab;
    }

    @Override
    public boolean isEnableSecurity() {
        return true;
    }

    @Override
    public Map<String, File> getPrincipalKeytabMap() {
        return principalKeytabMap;
    }

    @Override
    public MiniKdc getKdc() {
        return kdc;
    }

    @Override
    public File getDefaultKeytab() {
        return defaultKeytab;
    }

    @Override
    public String getDefaultPrincipal() {
        return defaultPrincipal;
    }

    @Override
    public UserGroupInformation getDefaultUGI() throws IOException {
        return UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                defaultPrincipal, defaultKeytab.getAbsolutePath());
    }
}
