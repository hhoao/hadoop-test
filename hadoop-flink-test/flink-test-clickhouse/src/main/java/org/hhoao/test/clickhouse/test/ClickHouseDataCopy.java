package org.hhoao.test.clickhouse.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/** A Flink data stream example and SQL sinking data to Hologres. */
public class ClickHouseDataCopy {

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
        DatabaseProperties sourceProperties =
                new DatabaseProperties(
                        "clickhouse://127.0.0.1:18123", "test", "123456", "source", "default");

        DatabaseProperties sinkProperties =
                new DatabaseProperties(
                    "clickhouse://127.0.0.1:18123", "test", "123456", "sink", "default");

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
                        sourceProperties.getDatabase(),
                        sourceProperties.getTableName(),
                        sourceProperties.getUserName(),
                        sourceProperties.getPassword(),
                        sourceProperties.getUrl());
        tEnv.executeSql(createHologresSourceTable);

        String createClickhouseTable =
                String.format(
                        "create table sink("
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
                        sinkProperties.getDatabase(),
                        sinkProperties.getTableName(),
                        sinkProperties.getUserName(),
                        sinkProperties.getPassword(),
                        sinkProperties.getUrl());
        tEnv.executeSql(createClickhouseTable);

        tEnv.executeSql("insert into sink select * from source");
    }
}
