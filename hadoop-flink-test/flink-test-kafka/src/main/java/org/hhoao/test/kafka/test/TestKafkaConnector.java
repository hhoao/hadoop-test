package org.hhoao.test.kafka.test;

import java.time.Duration;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.hhoao.test.kafka.base.KafkaExtension;
import org.hhoao.test.kafka.utils.KafkaUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * TestKafkaConnector
 *
 * @author w
 * @since 2024/9/19
 */
public class TestKafkaConnector {
    @RegisterExtension private static final KafkaExtension kafkaExtension = new KafkaExtension();

    @Test
    void test() {
        Configuration configuration = new Configuration();
        configuration.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10));
        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        StreamTableEnvironment streamTableEnvironment =
                StreamTableEnvironment.create(executionEnvironment);
        String defaultTopic = kafkaExtension.getDefaultTopic();
        String kafkaAddress = kafkaExtension.getKafkaAddress();
        KafkaUtils.asyncStartUserProducer(defaultTopic, kafkaExtension.getDefaultProducer(), 0);
        streamTableEnvironment.executeSql(
                String.format(
                        "CREATE TABLE source (\n"
                                + "  `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',\n"
                                + "  `partition` BIGINT METADATA VIRTUAL,\n"
                                + "  `offset` BIGINT METADATA VIRTUAL,\n"
                                + "  `id` INT,\n"
                                + "  `name` STRING,\n"
                                + "  `age` INT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'value.format' = 'json'\n"
                                + ");",
                        defaultTopic, kafkaAddress, 0));

        streamTableEnvironment.executeSql(
                "CREATE TABLE sink (\n"
                        + "  id INT,\n"
                        + "  name STRING,\n"
                        + "  age INT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'print'\n"
                        + ");");
        TableResult tableResult =
                streamTableEnvironment.executeSql(
                        "INSERT INTO sink SELECT id, name, age FROM source;");
        tableResult.print();
    }

    @Test
    void testBatch() throws InterruptedException {
        Configuration configuration = new Configuration();
        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        StreamTableEnvironment streamTableEnvironment =
                StreamTableEnvironment.create(executionEnvironment);
        String defaultTopic = kafkaExtension.getDefaultTopic();
        String kafkaAddress = kafkaExtension.getKafkaAddress();
        KafkaUtils.asyncStartUserProducer(defaultTopic, kafkaExtension.getDefaultProducer(), 0);
        KafkaUtils.asyncPrintTopicRecordsInstance(
                OffsetResetStrategy.LATEST,
                true,
                kafkaExtension.getKafkaAddress(),
                defaultTopic,
                Duration.ofSeconds(10));
        String groupId = "0";
        streamTableEnvironment.executeSql(
                String.format(
                        "CREATE TABLE source (\n"
                                + "  `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',\n"
                                + "  `partition` BIGINT METADATA VIRTUAL,\n"
                                + "  `offset` BIGINT METADATA VIRTUAL,\n"
                                + "  `id` INT,\n"
                                + "  `name` STRING,\n"
                                + "  `age` INT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'properties.auto.offset.reset' = 'earliest',\n"
                                + "  'scan.startup.mode' = 'group-offsets',\n"
                                + "  'value.format' = 'json',\n"
                                + "  'scan.bounded.mode' = 'latest-offset'\n"
                                + ");",
                        defaultTopic, kafkaAddress, groupId));

        streamTableEnvironment.executeSql(
                "CREATE TABLE sink (\n"
                        + "  id INT,\n"
                        + "  name STRING,\n"
                        + "  age INT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'print'\n"
                        + ");");
        TableResult tableResult =
                streamTableEnvironment.executeSql(
                        "INSERT INTO sink SELECT id, name, age FROM source;");
        tableResult.print();
    }
}
