package org.hhoao.test.common.extension;

import java.sql.*;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class MysqlContainerExtension implements BeforeEachCallback, AfterEachCallback {
    private final GenericContainer genericContainer =
            new GenericContainer(DockerImageName.parse("mysql:8.0.28"))
                    .withExposedPorts(3306)
                    .withEnv("MYSQL_ROOT_PASSWORD", "root");
    private MysqlProperties properties;
    private Connection connection;
    private Statement statement = null;

    public Statement getStatement() {
        return statement;
    }

    public Connection getConnection() {
        return connection;
    }

    public MysqlProperties getMysqlProperties() {
        return properties;
    }

    public void useDatabase(String database) {
        try {
            statement.execute(String.format("USE %s", database));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void recreateDefaultDatabase() {
        dropDatabase(properties.defaultDatabase);
        createDatabase(properties.defaultDatabase);
    }

    public void dropDefaultDatabase() {
        dropDatabase(properties.defaultDatabase);
    }

    public void dropDatabase(String database) {
        try {
            statement = connection.createStatement();
            statement.execute(String.format("DROP DATABASE IF EXISTS %s", database));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void createDatabase(String database) {
        try {
            statement.execute(String.format("CREATE DATABASE IF NOT EXISTS %s", database));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void selectDefaultDatabaseTables(String table) {
        selectTables(properties.defaultDatabase, table);
    }

    public void selectTables(String database, String tableName) {
        try {
            statement.execute(String.format("USE %s", database));
            statement.execute(String.format("SELECT * FROM %s", tableName));
            ResultSet resultSet = statement.getResultSet();
            ResultSetMetaData metaData = resultSet.getMetaData();

            System.out.printf("| \t %s\t | \t", "");
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                System.out.printf("%s\t | \t", metaData.getColumnName(i + 1));
            }
            System.out.println();
            int count = 0;
            while (resultSet.next()) {
                System.out.printf("| \t %s\t | \t", count++);
                for (int i = 0; i < metaData.getColumnCount(); i++) {
                    System.out.printf("%s\t | \t", resultSet.getObject(i + 1));
                }
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        genericContainer.start();
        String host = genericContainer.getHost();
        Integer firstMappedPort = genericContainer.getFirstMappedPort();
        Map envMap = genericContainer.getEnvMap();
        Object o = envMap.get("MYSQL_ROOT_PASSWORD");
        this.properties =
                new MysqlProperties(
                        String.format("jdbc:mysql://%s:%s", host, firstMappedPort),
                        "com.mysql.cj.jdbc.Driver",
                        (String) o,
                        "root",
                        host,
                        firstMappedPort);
        try {
            connection =
                    DriverManager.getConnection(
                            properties.url, properties.username, properties.password);
            statement = connection.createStatement();
            createDatabase(properties.defaultDatabase);
            useDatabase(properties.defaultDatabase);
            createTable(properties.defaultUserTable);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createTable(String defaultUserTable) throws SQLException {
        statement.execute(
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s \n"
                                + "(\n"
                                + "    id         INT,\n"
                                + "    name       VARCHAR(127),\n"
                                + "    money      DECIMAL,\n"
                                + "    age        INT,\n"
                                + "    birthday   Date,\n"
                                + "    create_time TIMESTAMP DEFAULT NOW(),\n"
                                + "    PRIMARY KEY (`id`)\n"
                                + ");",
                        defaultUserTable));
    }

    public Thread asyncInsertUserData(int count, TimeUnit timeUnit, long internal) {
        Thread userProducerThread =
                new Thread(
                        () -> {
                            Random random = new Random();
                            for (int i = 0; i < count; i++) {
                                try {
                                    if (internal != 0) {
                                        timeUnit.sleep(internal);
                                    }
                                    String sql =
                                            String.format(
                                                    "INSERT INTO user(id, name, money, age, birthday) VALUES(%s, 'test%s', %s, %s, DATE('2001-5-21'));",
                                                    i,
                                                    i,
                                                    random.nextDouble()
                                                            * random.nextInt(10000)
                                                            * 100,
                                                    random.nextInt(50));
                                    statement.execute(sql);
                                    if (i % 10 == 0) {
                                        System.out.println("Execute Sql: " + sql);
                                    }
                                } catch (SQLException | InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });
        userProducerThread.start();
        return userProducerThread;
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        connection.close();
    }

    public static class MysqlProperties {
        private final String url;
        private final String driver;
        private final String password;
        private final String username;
        private final String host;
        private final Integer port;
        private final String defaultDatabase = "mysqldb";
        private final String defaultUserTable = "user";

        public MysqlProperties(
                String url,
                String driver,
                String password,
                String username,
                String host,
                Integer port) {
            this.url = url;
            this.driver = driver;
            this.password = password;
            this.username = username;
            this.host = host;
            this.port = port;
        }

        public String getUrl() {
            return url;
        }

        public String getDriver() {
            return driver;
        }

        public String getPassword() {
            return password;
        }

        public String getHost() {
            return host;
        }

        public Integer getPort() {
            return port;
        }

        public String getUsername() {
            return username;
        }

        public String getDefaultDatabase() {
            return defaultDatabase;
        }

        public String getDefaultUserTable() {
            return defaultUserTable;
        }
    }
}
