package org.hhoao.test.elasticsearch.test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.hhoao.hadoop.test.utils.Resources;
import org.hhoao.test.flink.source.user.User;
import org.hhoao.test.flink.source.user.UserSource;
import org.junit.jupiter.api.Test;

/**
 * TestElasticSearchConnector
 *
 * @author w
 * @since 2024/10/10
 */
public class TestElasticSearchConnector {
    @Test
    void test() throws IOException {
        Configuration configuration = new Configuration();
        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        StreamTableEnvironment streamTableEnvironment =
                StreamTableEnvironment.create(executionEnvironment);
        String address = "";
        DataStreamSource<User> userDataStreamSource =
                executionEnvironment.fromSource(
                        new UserSource(), WatermarkStrategy.noWatermarks(), "user-source");
        Table table = streamTableEnvironment.fromDataStream(userDataStreamSource);
        streamTableEnvironment.createTemporaryView("T", table);
        String tableName = "esSink";
        streamTableEnvironment.executeSql(
                String.format(
                        FileUtils.readFileToString(
                                new File(Resources.getResource("elasticsearch-sink.sql").getFile()),
                                Charset.defaultCharset()),
                        tableName,
                        address,
                        0));

        streamTableEnvironment.executeSql(
                "CREATE TABLE sink (\n"
                        + "  id INT,\n"
                        + "  name STRING,\n"
                        + "  age INT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'print'\n"
                        + ");");
        streamTableEnvironment.executeSql("INSERT INTO sink SELECT id, name, age FROM T;");
        TableResult tableResult =
                streamTableEnvironment.executeSql(
                        "INSERT INTO esSink SELECT id, name, age FROM T;");
        tableResult.print();
    }
}
