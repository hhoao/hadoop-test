package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.example.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HdfsLocalCluster implements MiniCluster {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(HdfsLocalCluster.class);

    MiniDFSCluster miniDFSCluster;

    private Integer hdfsNamenodePort;
    private Integer hdfsNamenodeHttpPort;
    private String hdfsTempDir;
    private Integer hdfsNumDatanodes;
    private Boolean hdfsEnablePermissions;
    private Boolean hdfsFormat;
    private Boolean hdfsEnableRunningUserAsProxyUser;
    private Configuration hdfsConfig;

    private HdfsLocalCluster(Builder builder) {
        this.hdfsNamenodePort = builder.hdfsNamenodePort;
        this.hdfsNamenodeHttpPort = builder.hdfsNamenodeHttpPort;
        this.hdfsTempDir = builder.hdfsTempDir;
        this.hdfsNumDatanodes = builder.hdfsNumDatanodes;
        this.hdfsEnablePermissions = builder.hdfsEnablePermissions;
        this.hdfsFormat = builder.hdfsFormat;
        this.hdfsEnableRunningUserAsProxyUser = builder.hdfsEnableRunningUserAsProxyUser;
        this.hdfsConfig = builder.hdfsConfig;
    }

    public Integer getHdfsNamenodePort() {
        return hdfsNamenodePort;
    }

    public String getHdfsTempDir() {
        return hdfsTempDir;
    }

    public Integer getHdfsNumDatanodes() {
        return hdfsNumDatanodes;
    }

    public Boolean getHdfsEnablePermissions() {
        return hdfsEnablePermissions;
    }

    public Boolean getHdfsFormat() {
        return hdfsFormat;
    }

    public Boolean getHdfsEnableRunningUserAsProxyUser() {
        return hdfsEnableRunningUserAsProxyUser;
    }

    public Configuration getHdfsConfig() {
        return hdfsConfig;
    }

    @Override
    public void start() throws Exception {

        LOG.info("HDFS: Starting MiniDfsCluster");
        configure();
        miniDFSCluster = new MiniDFSCluster.Builder(hdfsConfig)
                .nameNodePort(hdfsNamenodePort)
                .nameNodeHttpPort(hdfsNamenodeHttpPort == null ? 0 : hdfsNamenodeHttpPort.intValue())
                .numDataNodes(hdfsNumDatanodes)
                .format(hdfsFormat)
                .racks(null)
                .build();

    }

    @Override
    public void stop() throws Exception {
        stop(true);
    }

    @Override
    public void stop(boolean cleanUp) throws Exception {
        LOG.info("HDFS: Stopping MiniDfsCluster");
        miniDFSCluster.shutdown();
        if (cleanUp) {
            cleanUp();
        }

    }

    @Override
    public void configure() throws Exception {
        if (null != hdfsEnableRunningUserAsProxyUser && hdfsEnableRunningUserAsProxyUser) {
            hdfsConfig.set("hadoop.proxyuser." + System.getProperty("user.name") + ".hosts", "*");
            hdfsConfig.set("hadoop.proxyuser." + System.getProperty("user.name") + ".groups", "*");
        }

        hdfsConfig.setBoolean("dfs.permissions", hdfsEnablePermissions);
        System.setProperty("test.build.data", hdfsTempDir);
    }

    @Override
    public void cleanUp() throws Exception {
        FileUtils.deleteFolder(hdfsTempDir);
    }

    public FileSystem getHdfsFileSystemHandle() throws Exception {
        return miniDFSCluster.getFileSystem();
    }

    public static class Builder {
        private Integer hdfsNamenodePort;
        private Integer hdfsNamenodeHttpPort;
        private String hdfsTempDir;
        private Integer hdfsNumDatanodes;
        private Boolean hdfsEnablePermissions;
        private Boolean hdfsFormat;
        private Boolean hdfsEnableRunningUserAsProxyUser;
        private Configuration hdfsConfig;


        public Builder setHdfsNamenodePort(Integer hdfsNameNodePort) {
            this.hdfsNamenodePort = hdfsNameNodePort;
            return this;
        }

        public Builder setHdfsNamenodeHttpPort(Integer hdfsNameNodeHttpPort) {
            this.hdfsNamenodeHttpPort = hdfsNameNodeHttpPort;
            return this;
        }

        public Builder setHdfsTempDir(String hdfsTempDir) {
            this.hdfsTempDir = hdfsTempDir;
            return this;
        }

        public Builder setHdfsNumDatanodes(Integer hdfsNumDatanodes) {
            this.hdfsNumDatanodes = hdfsNumDatanodes;
            return this;
        }

        public Builder setHdfsEnablePermissions(Boolean hdfsEnablePermissions) {
            this.hdfsEnablePermissions = hdfsEnablePermissions;
            return this;
        }

        public Builder setHdfsFormat(Boolean hdfsFormat) {
            this.hdfsFormat = hdfsFormat;
            return this;
        }

        public Builder setHdfsEnableRunningUserAsProxyUser(Boolean hdfsEnableRunningUserAsProxyUser) {
            this.hdfsEnableRunningUserAsProxyUser = hdfsEnableRunningUserAsProxyUser;
            return this;
        }

        public Builder setHdfsConfig(Configuration hdfsConfig) {
            this.hdfsConfig = hdfsConfig;
            return this;
        }

        public HdfsLocalCluster build() {
            HdfsLocalCluster hdfsLocalCluster = new HdfsLocalCluster(this);
            validateObject(hdfsLocalCluster);
            return hdfsLocalCluster;
        }

        public void validateObject(HdfsLocalCluster hdfsLocalCluster) {
            if (hdfsLocalCluster.hdfsNamenodePort == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HDFS Namenode Port");
            }

            if (hdfsLocalCluster.hdfsTempDir == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HDFS Temp Dir");
            }

            if (hdfsLocalCluster.hdfsNumDatanodes == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HDFS Num Datanodes");
            }

            if (hdfsLocalCluster.hdfsEnablePermissions == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HDFS Enable Permissions");
            }

            if (hdfsLocalCluster.hdfsFormat == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HDFS Format");
            }

            if (hdfsLocalCluster.hdfsConfig == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HDFS Config");
            }

        }
    }
}
