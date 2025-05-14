package org.hhoao.test.flink.connector.mysql;

import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.hhoao.test.common.extension.MysqlContainerExtension;
import org.hhoao.test.flink.source.user.User;
import org.hhoao.test.flink.source.user.UserSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class SimpleStreamSqlTest {
    @RegisterExtension
    MysqlContainerExtension mysqlContainerExtension = new MysqlContainerExtension();

    @Test
    public void test() {
        Configuration configuration = new Configuration();
        configuration.set(
                ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10));
        configuration.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 2);
        configuration.set(StateBackendOptions.STATE_BACKEND, "jobmanager");
        configuration.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        StreamTableEnvironment streamTableEnvironment =
                StreamTableEnvironment.create(executionEnvironment);

        DataStreamSource<User> userIteratorSource =
                executionEnvironment.fromSource(
                        new UserSource(), WatermarkStrategy.noWatermarks(), "user_iterator_source");
        Table table = streamTableEnvironment.fromDataStream(userIteratorSource);
        streamTableEnvironment.createTemporaryView("T", table);
        MysqlContainerExtension.MysqlProperties mysqlProperties =
                mysqlContainerExtension.getMysqlProperties();
        streamTableEnvironment.executeSql(
                String.format(
                        "CREATE TABLE MyUserTable (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + "  age INT,\n"
                                + "  PRIMARY KEY (id) NOT ENFORCED\n"
                                + ") WITH (\n"
                                + "   'connector' = 'jdbc',\n"
                                + "   'driver' = 'com.mysql.cj.jdbc.Driver',\n"
                                + " 'username'='%s',\n"
                                + " 'password' = '%s',\n"
                                + "   'url' = '%s',\n"
                                + "   'table-name' = '%s'\n"
                                + ");",
                        mysqlProperties.getUsername(),
                        mysqlProperties.getPassword(),
                        mysqlProperties.getUrl() + "/" + mysqlProperties.getDefaultDatabase(),
                        mysqlProperties.getDefaultUserTable()));

        TableResult tableResult =
                streamTableEnvironment.executeSql(
                        "INSERT INTO MyUserTable SELECT id, name, age FROM T");
        tableResult.print();
    }
}
