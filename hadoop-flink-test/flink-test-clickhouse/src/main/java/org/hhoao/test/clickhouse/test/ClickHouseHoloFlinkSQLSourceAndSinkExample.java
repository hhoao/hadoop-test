package org.hhoao.test.clickhouse.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/** A Flink data stream example and SQL sinking data to Hologres. */
public class ClickHouseHoloFlinkSQLSourceAndSinkExample {

    /**
     * Hologres DDL. create table source_table(user_id bigint, user_name text, price decimal(38,
     * 2),sale_timestamp timestamptz); insert into source_table values(123,'Adam',123.11,'2022-05-19
     * 14:33:05.418+08'); insert into source_table values(456,'Bob',123.45,'2022-05-19
     * 14:33:05.418+08');
     *
     * <p>Hologres DDL. create table sink_table(user_id bigint, user_name text, price decimal(38,
     * 2),sale_timestamp timestamptz);
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        DatabaseProperties clickHouseProperties =
                new DatabaseProperties(
                        "clickhouse://127.0.0.1:18123", "puyun", "123456", "test1", "default");
        DatabaseProperties holoProperties =
                new DatabaseProperties(
                        "",
                        "",
                        "",
                        "",
                        "");

        EnvironmentSettings.Builder streamBuilder =
                EnvironmentSettings.newInstance().inStreamingMode();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, streamBuilder.build());

        String createHologresSourceTable =
                String.format(
                        "create table source("
                                + "  user_id bigint,"
                                + "  user_name string,"
                                + "  price decimal(38,2),"
                                + "  sale_timestamp timestamp"
                                + ") with ("
                                + "  'connector'='clickhouse',"
                                + "  'database-name' = '%s',"
                                + "  'table-name' = '%s',"
                                + "  'username' = '%s',"
                                + "  'password' = '%s',"
                                + "  'url' = '%s'"
                                + ")",
                        clickHouseProperties.database,
                        clickHouseProperties.tableName,
                        clickHouseProperties.userName,
                        clickHouseProperties.password,
                        clickHouseProperties.url);
        tEnv.executeSql(createHologresSourceTable);

        String createHologresTable =
                String.format(
                        "create table sink("
                                + "  user_id bigint,"
                                + "  user_name string,"
                                + "  price decimal(38,2),"
                                + "  sale_timestamp timestamp"
                                + ") with ("
                                + "  'connector'='hologres',"
                                + "  'jdbcRetryCount'='20',"
                                + "  'dbname' = '%s',"
                                + "  'tablename' = '%s',"
                                + "  'username' = '%s',"
                                + "  'password' = '%s',"
                                + "  'jdbcCopyWriteMode' = 'true',"
                                + "  'endpoint' = '%s'"
                                + ")",
                        holoProperties.database,
                        holoProperties.tableName,
                        holoProperties.userName,
                        holoProperties.password,
                        holoProperties.url);
        tEnv.executeSql(createHologresTable);

        tEnv.executeSql("insert into sink select * from source");
    }
}
