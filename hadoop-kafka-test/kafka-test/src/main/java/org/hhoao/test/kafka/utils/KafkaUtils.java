package org.hhoao.test.kafka.utils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Locale;
import java.util.Properties;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * KafkaUtils
 *
 * @author xianxing
 * @since 2024/10/11
 */
public class KafkaUtils {
    public static Thread asyncPrintTopicRecordsInstance(
            OffsetResetStrategy type,
            boolean rotation,
            String address,
            String topic,
            Duration timeout) {
        Thread thread =
                new Thread(
                        () -> {
                            Properties properties = new Properties();
                            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, address);
                            properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
                            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
                            properties.put(
                                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                                    StringDeserializer.class);
                            properties.put(
                                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                    StringDeserializer.class);
                            properties.put(
                                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, type.toString());
                            KafkaConsumer<String, String> consumer =
                                    new KafkaConsumer<>(properties);
                            consumer.subscribe(Collections.singleton(topic));
                            ConsumerRecords<String, String> poll = consumer.poll(timeout);
                            while (!poll.isEmpty() || rotation) {
                                for (ConsumerRecord<String, String> stringStringConsumerRecord :
                                        poll) {
                                    System.out.printf(
                                            "[Record: partition=%s, key=%s, value=%s\n]",
                                            stringStringConsumerRecord.partition(),
                                            stringStringConsumerRecord.key(),
                                            stringStringConsumerRecord.value());
                                }
                                poll = consumer.poll(timeout);
                            }
                            consumer.close();
                        });
        thread.start();
        return thread;
    }

    public static Thread asyncStartUserProducer(String topic, Producer producer, int partition) {
        Thread thread =
                new Thread(
                        () -> {
                            int i = 0;
                            while (true) {
                                ProducerRecord<String, String> producerRecord =
                                        new ProducerRecord<>(
                                                topic,
                                                partition,
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
                                producer.send(producerRecord);
                                producer.flush();
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });
        thread.start();
        return thread;
    }
}
