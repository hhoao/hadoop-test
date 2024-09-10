package org.hhoao.test.flink.hive;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hive.beeline.BeeLine;
import org.hhoao.hadoop.test.cluster.MiniHadoopClusterTestContext;
import org.hhoao.hadoop.test.utils.LoggerUtils;
import org.hhoao.hadoop.test.utils.Resources;
import org.hhoao.test.hive.base.HiveTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * TestHiveConnector
 *
 * @author w
 * @since 2024/9/9
 */
public class TestHiveConnector extends HiveTest {
    private static final String TABLE_NAME = "user_test";

    @Override
    protected MiniHadoopClusterTestContext getMiniHadoopClusterTestContext() {
        MiniHadoopClusterTestContext miniHadoopClusterTestContext =
                new MiniHadoopClusterTestContext();
        miniHadoopClusterTestContext.setStartHdfsOperator(false);
        return miniHadoopClusterTestContext;
    }

    @BeforeEach
    void beforeTest() throws SQLException, IOException {
        LoggerUtils.changeAppendAllLogToFile();
        Connection connection = getConnection();
        Statement statement = connection.createStatement();
        String createTablePrepareStatement =
                String.format(
                        "CREATE TABLE %s(\n"
                                + "    id         INT,\n"
                                + "    name       VARCHAR(127),\n"
                                + "    money      DECIMAL,\n"
                                + "    age        INT\n"
                                + ") partitioned by (pt string) STORED AS PARQUET",
                        TABLE_NAME);
        statement.execute(createTablePrepareStatement);
        CompletableFuture.runAsync(
                () -> {
                    String user = System.getProperty("user.name");
                    try (BeeLine beeLine = new BeeLine()) {
                        beeLine.begin(new String[] {"-u", url, "-n", user}, System.in);
                        TimeUnit.HOURS.sleep(1);
                    } catch (InterruptedException | IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Test
    public void test() {
        Configuration configuration = new Configuration();
        configuration.set(
                ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(30));
        configuration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                new File(Resources.getTargetDir(), "checkpoints").toURI().toString());

        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        StreamTableEnvironment streamTableEnvironment =
                StreamTableEnvironment.create(executionEnvironment);
        DataStreamSource<User> userDataStreamSource =
                executionEnvironment.addSource(new UserIteratorSource());

        String uri = MetastoreConf.getAsString(hiveConf, MetastoreConf.ConfVars.THRIFT_URIS);
        streamTableEnvironment.executeSql(
                String.format(
                        "CREATE TABLE user_test (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + "  money DECIMAL,\n"
                                + "  age INT,\n"
                                + "  pt STRING,\n"
                                + "  PRIMARY KEY (id) NOT ENFORCED\n"
                                + ") PARTITIONED BY (pt) WITH (\n"
                                + "   'connector' = 'hive',\n"
                                + "   'table-name' = '%s',\n"
                                + "   'database' = 'default',\n"
                                + "   'metastore.thrift.uris' = '%s',\n"
                                + "   'sink.partition-commit.policy.kind' = 'metastore', \n"
                                + "   'properties.hive.in.test' = 'true'"
                                + ");",
                        TABLE_NAME, uri));
        Table table = streamTableEnvironment.fromDataStream(userDataStreamSource);

        streamTableEnvironment.createTemporaryView("T", table);
        TableResult tableResult =
                streamTableEnvironment.executeSql(
                        "INSERT INTO user_test " + "SELECT id, name, money, age, 'hello' FROM T");
        tableResult.print();
    }
}
