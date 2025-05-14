package org.hhoao.test.clickhouse.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ClickHouseUtil {
    private static Connection connection;

    public static Connection getConn(String host, int port, String database)
            throws SQLException, ClassNotFoundException {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        String address = "jdbc:clickhouse://" + host + ":" + port + "/" + database;
        connection = DriverManager.getConnection(address);
        return connection;
    }

    public static Connection getConn(String host, int port)
            throws SQLException, ClassNotFoundException {
        return getConn(host, port, "default");
    }

    public static Connection getConn() throws SQLException, ClassNotFoundException {
        return getConn("node-01", 8123);
    }

    public void close() throws SQLException {
        connection.close();
    }
}
