package com.processing.logs.config;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerConfig {
    public static Properties buildProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "log-processing-cluster-kafka-bootstrap.log-processing.svc:9092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, System.getenv().getOrDefault("KAFKA_GROUP_ID", "log-processing-group"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, System.getenv().getOrDefault("KAFKA_MAX_POLL_RECORDS", "3"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, System.getenv().getOrDefault("KAFKA_AUTO_OFFSET_RESET", "earliest"));
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, System.getenv().getOrDefault("KAFKA_REQUEST_TIMEOUT_MS", "30000"));
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, System.getenv().getOrDefault("KAFKA_API_TIMEOUT_MS", "30000"));

        return props;
    }
}
