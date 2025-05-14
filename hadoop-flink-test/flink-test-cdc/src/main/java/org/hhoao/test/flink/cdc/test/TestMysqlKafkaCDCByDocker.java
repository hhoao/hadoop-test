package org.hhoao.test.flink.cdc.test;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.text.StringSubstitutor;
import org.apache.flink.cdc.cli.parser.PipelineDefinitionParser;
import org.apache.flink.cdc.cli.parser.YamlPipelineDefinitionParser;
import org.apache.flink.cdc.cli.utils.FlinkEnvironmentUtils;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.flink.FlinkPipelineComposer;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.hhoao.hadoop.test.utils.Resources;
import org.hhoao.test.common.extension.MysqlContainerExtension;
import org.hhoao.test.kafka.base.KafkaExtension;
import org.hhoao.test.kafka.utils.KafkaUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * TestMysqlKakfa
 *
 * @author w
 * @since 2024/9/25
 */
public class TestMysqlKafkaCDCByDocker {
    @RegisterExtension static KafkaExtension kafkaExtension = new KafkaExtension();

    @RegisterExtension
    MysqlContainerExtension mysqlContainerExtension = new MysqlContainerExtension();

    private Thread mysqlUserProducerThread;
    private Thread kafkaConsumerThread;

    @Test
    public void test() throws Exception {
        String script =
                new String(
                        Files.readAllBytes(
                                Paths.get(Resources.getResource("mysql-kafka.yml").getPath())));
        Map<String, Object> configMap = getScriptConfigMap();
        String filledScript = StringSubstitutor.replace(script, configMap);

        Path tempFile = Files.createTempFile("mysql-kafka", ".yml");
        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(tempFile)) {
            bufferedWriter.write(filledScript);
            bufferedWriter.flush();
        }

        Configuration cdcConfig = new Configuration();
        org.apache.flink.configuration.Configuration flinkConfiguration =
                new org.apache.flink.configuration.Configuration();
        PipelineDefinitionParser pipelineDefinitionParser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = pipelineDefinitionParser.parse(tempFile, cdcConfig);

        SavepointRestoreSettings savepointRestoreSettings =
                SavepointRestoreSettings.fromConfiguration(flinkConfiguration);
        ArrayList<Path> paths = new ArrayList<>();
        FlinkPipelineComposer composer =
                FlinkEnvironmentUtils.createComposer(
                        true, cdcConfig, paths, savepointRestoreSettings);
        PipelineExecution execution = composer.compose(pipelineDef);

        int internal = 2;
        mysqlUserProducerThread =
                mysqlContainerExtension.asyncInsertUserData(
                        Integer.MAX_VALUE, TimeUnit.MILLISECONDS, 10);
        kafkaConsumerThread =
                KafkaUtils.asyncPrintTopicRecordsInstance(
                        OffsetResetStrategy.LATEST,
                        true,
                        kafkaExtension.getKafkaAddress(),
                        kafkaExtension.getDefaultTopic(),
                        Duration.ofSeconds(internal * 10));
        execution.execute();
    }

    private Map<String, Object> getScriptConfigMap() {
        HashMap<String, Object> configMap = new HashMap<>();
        MysqlContainerExtension.MysqlProperties mysqlProperties =
                mysqlContainerExtension.getMysqlProperties();
        String kafkaAddress = kafkaExtension.getKafkaAddress();

        configMap.put("mysql.hostname", mysqlProperties.getHost());
        configMap.put("mysql.port", mysqlProperties.getPort());
        configMap.put("mysql.username", mysqlProperties.getUsername());
        configMap.put("mysql.password", mysqlProperties.getPassword());
        configMap.put(
                "mysql.tables",
                mysqlProperties.getDefaultDatabase() + "." + mysqlProperties.getDefaultUserTable());
        configMap.put("kafka.bootstrap.servers", kafkaAddress);
        configMap.put("kafka.topic", kafkaExtension.getDefaultTopic());
        return configMap;
    }

    @AfterEach
    void afterEach() {
        kafkaConsumerThread.interrupt();
        mysqlUserProducerThread.interrupt();
    }
}
