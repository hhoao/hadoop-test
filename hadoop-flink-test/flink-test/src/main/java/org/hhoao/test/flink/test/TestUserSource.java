package org.hhoao.test.flink.test;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.hhoao.test.flink.source.user.User;
import org.hhoao.test.flink.source.user.UserSource;
import org.junit.jupiter.api.Test;

/**
 * TestUserSource
 *
 * @author w
 * @since 2024/10/10
 */
public class TestUserSource {
    @Test
    void test() {
        StreamExecutionEnvironment executionEnvironment =
                StreamContextEnvironment.getExecutionEnvironment();
        DataStreamSource<User> userDataStreamSource =
                executionEnvironment.fromSource(
                        new UserSource(), WatermarkStrategy.noWatermarks(), "user-source");
        StreamTableEnvironment streamTableEnvironment =
                StreamTableEnvironment.create(executionEnvironment);
        streamTableEnvironment.executeSql(
                "CREATE TABLE sink (\n"
                        + "  id INT,\n"
                        + "  name STRING,\n"
                        + "  age INT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'print'\n"
                        + ");");
        Table table = streamTableEnvironment.fromDataStream(userDataStreamSource);
        streamTableEnvironment.createTemporaryView("T", table);
        TableResult tableResult =
                streamTableEnvironment.executeSql(
                        "INSERT INTO sink " + "SELECT id, name, age FROM T");
        tableResult.print();
    }
}
