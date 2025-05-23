package org.hhoao.test.kafka.base;

import io.github.embeddedkafka.EmbeddedK;
import io.github.embeddedkafka.EmbeddedKafka;
import io.github.embeddedkafka.EmbeddedKafkaConfig;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import scala.collection.immutable.HashMap;

public class KafkaExtension implements BeforeAllCallback, AfterAllCallback {
    private EmbeddedK kafka;
    private int kafkaPort;
    private int zkPort;
    private KafkaProducer defaultProducer;
    private AdminClient adminClient;
    private String defaultTopic = "default_topic";

    public AdminClient getAdminClient() {
        return adminClient;
    }

    public String getDefaultTopic() {
        return defaultTopic;
    }

    @Override
    public void afterAll(ExtensionContext context) {
        if (kafka != null) {
            kafka.stop(false);
        }
    }

    public KafkaProducer getDefaultProducer() {
        return defaultProducer;
    }

    public int getKafkaPort() {
        return kafkaPort;
    }

    public String getKafkaAddress() {
        return "localhost:" + kafkaPort;
    }

    public int getZkPort() {
        return zkPort;
    }

    public EmbeddedK getKafka() {
        return kafka;
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        ServerSocket kafkaSocket = new ServerSocket(0);
        ServerSocket zkServerSocket = new ServerSocket(0);
        kafkaPort = kafkaSocket.getLocalPort();
        zkPort = zkServerSocket.getLocalPort();
        kafkaSocket.close();
        zkServerSocket.close();
        EmbeddedKafkaConfig kafkaConfig =
                EmbeddedKafkaConfig.apply(
                        kafkaPort, zkPort, new HashMap<>(), new HashMap<>(), new HashMap<>());
        kafka = EmbeddedKafka.start(kafkaConfig);
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaAddress());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        defaultProducer = new KafkaProducer<>(props);
        adminClient = KafkaAdminClient.create(props);
        CreateTopicsResult topics =
                adminClient.createTopics(
                        Collections.singletonList(new NewTopic(defaultTopic, 10, (short) 1)),
                        new CreateTopicsOptions());
        topics.all().get();
    }
}
