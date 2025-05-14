package org.hhoao.test.clickhouse.test;

public class DatabaseProperties {
    String url;
    String userName;
    String password;
    String database;
    String tableName;

    public DatabaseProperties() {}

    public DatabaseProperties(
            String url, String database, String password, String tableName, String userName) {
        this.url = url;
        this.database = database;
        this.password = password;
        this.tableName = tableName;
        this.userName = userName;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }
}
