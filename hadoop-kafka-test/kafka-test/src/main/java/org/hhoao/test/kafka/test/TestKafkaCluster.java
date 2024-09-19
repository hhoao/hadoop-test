package org.hhoao.test.kafka.test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.hhoao.test.kafka.base.KafkaExtension;
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

    private static final String TOPIC = "test_topic";

    @Test
    public void getTopicLatestRecordsInstance() throws ExecutionException, InterruptedException {
        createTopics();
        startUserProducer();
        getTopicRecordsInstance("latest", true);
    }

    void createTopics() throws ExecutionException, InterruptedException {
        AdminClient adminClient = kafkaExtension.getAdminClient();
        CreateTopicsResult topics =
                adminClient.createTopics(
                        Collections.singletonList(new NewTopic(TOPIC, 10, (short) 1)),
                        new CreateTopicsOptions());
        topics.all().get();
    }

    public void getTopicRecordsInstance(String type, boolean rotation) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaExtension.getKafkaAddress());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, type);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(TOPIC));
        ConsumerRecords<String, String> poll = consumer.poll(Duration.ofSeconds(1));
        while (!poll.isEmpty() || rotation) {
            for (ConsumerRecord<String, String> stringStringConsumerRecord : poll) {
                System.out.printf(
                        "[Record: partition=%s, key=%s, value=%s\n]",
                        stringStringConsumerRecord.partition(),
                        stringStringConsumerRecord.key(),
                        stringStringConsumerRecord.value());
            }
            poll = consumer.poll(Duration.ofSeconds(1));
        }
        consumer.close();
    }

    private void startUserProducer() {
        new Thread(
                        () -> {
                            KafkaProducer defaultProducer = kafkaExtension.getDefaultProducer();
                            int i = 0;
                            while (true) {
                                ProducerRecord<String, String> producerRecord =
                                        new ProducerRecord<>(
                                                TOPIC,
                                                1,
                                                "testKey",
                                                String.format(
                                                        "{\n"
                                                                + "  \"id\":%s,\n"
                                                                + "  \"name\":\"test%s\",\n"
                                                                + "  \"money\":%s,\n"
                                                                + "  \"age\":%s,\n"
                                                                + "  \"create_time\":\"%s\"\n"
                                                                + "}",
                                                        i,
                                                        i,
                                                        i * 100,
                                                        i * 2 % 53,
                                                        DateTimeFormatter.ofPattern(
                                                                        "yyyy-MM-dd HH:mm:ss",
                                                                        Locale.ROOT)
                                                                .format(LocalDateTime.now())));
                                i++;
                                defaultProducer.send(producerRecord);
                                defaultProducer.flush();
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        })
                .start();
    }
}