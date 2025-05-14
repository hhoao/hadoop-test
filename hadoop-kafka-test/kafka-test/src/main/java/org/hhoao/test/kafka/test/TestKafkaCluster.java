package org.hhoao.test.kafka.test;

import java.time.Duration;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.hhoao.test.kafka.base.KafkaExtension;
import org.hhoao.test.kafka.utils.KafkaUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * TestKafkaCluster
 *
 * @author w
 * @since 2024/9/19
 */
public class TestKafkaCluster {
    @RegisterExtension private static final KafkaExtension kafkaExtension = new KafkaExtension();
    private Thread asyncPrintTopicRecordsInstanceThread;
    private Thread asyncStartUserProducerThread;

    @Test
    public void getTopicLatestRecordsInstance() {
        asyncStartUserProducerThread =
                KafkaUtils.asyncStartUserProducer(
                        kafkaExtension.getDefaultTopic(), kafkaExtension.getDefaultProducer(), 0);
        asyncPrintTopicRecordsInstanceThread =
                KafkaUtils.asyncPrintTopicRecordsInstance(
                        OffsetResetStrategy.LATEST,
                        true,
                        kafkaExtension.getKafkaAddress(),
                        kafkaExtension.getDefaultTopic(),
                        Duration.ofSeconds(2));
    }

    @AfterEach
    void afterEach() {
        asyncStartUserProducerThread.interrupt();
        asyncPrintTopicRecordsInstanceThread.interrupt();
    }
}
