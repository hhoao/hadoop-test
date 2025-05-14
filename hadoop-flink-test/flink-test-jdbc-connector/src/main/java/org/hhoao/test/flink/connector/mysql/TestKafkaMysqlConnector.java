package org.hhoao.test.flink.connector.mysql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.hhoao.test.common.extension.MysqlContainerExtension;
import org.hhoao.test.kafka.base.KafkaExtension;
import org.hhoao.test.kafka.utils.KafkaUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * TestKafkaMysqlConnector
 *
 * @author xianxing
 * @since 2024/11/28
 */
public class TestKafkaMysqlConnector {
    @RegisterExtension static KafkaExtension kafkaExtension = new KafkaExtension();

    @RegisterExtension
    MysqlContainerExtension mysqlContainerExtension = new MysqlContainerExtension();

    @Test
    public void test() {
        Configuration configuration = new Configuration();
        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        StreamTableEnvironment streamTableEnvironment =
                StreamTableEnvironment.create(executionEnvironment);
        MysqlContainerExtension.MysqlProperties mysqlProperties =
                mysqlContainerExtension.getMysqlProperties();
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
                String.format(
                        "CREATE TABLE sink (\n"
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
                        "INSERT INTO sink SELECT id, name, age FROM source;");
        tableResult.print();
    }
}
