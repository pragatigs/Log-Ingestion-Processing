package com.tcp.ingestor.service;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String topic, int partition, Object messagePayload) {
        ProducerRecord<String,Object> record = new ProducerRecord<>(topic, partition, null, messagePayload);
        kafkaTemplate.send(record);
    }
}
