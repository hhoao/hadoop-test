package org.hhoao.hadoop.test.cluster;

import org.apache.hadoop.conf.Configuration;

/**
 * MiniHadoopClusterTestOptions
 *
 * @author xianxing
 * @since 2024/8/28
 */
public class MiniHadoopClusterTestContext {
    boolean enableSecurity;
    boolean startHdfsOperator;
    String classpath;
    Configuration configuration;

    public MiniHadoopClusterTestContext() {
        enableSecurity = false;
        startHdfsOperator = true;
        classpath = null;
        configuration = null;
    }

    public boolean isEnableSecurity() {
        return enableSecurity;
    }

    public void setEnableSecurity(boolean enableSecurity) {
        this.enableSecurity = enableSecurity;
    }

    public boolean isStartHdfsOperator() {
        return startHdfsOperator;
    }

    public void setStartHdfsOperator(boolean startHdfsOperator) {
        this.startHdfsOperator = startHdfsOperator;
    }

    public String getClasspath() {
        return classpath;
    }

    public void setClasspath(String classpath) {
        this.classpath = classpath;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }
}
