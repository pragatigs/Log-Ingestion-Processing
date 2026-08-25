package com.processing.logs.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LogPoller {

    public record PollResult(
            KafkaConsumer<String, String> consumer,
            List<ConsumerRecord<String, String>> records) {
    }

    public static PollResult pollLogs(Properties consumerProps) {

        final Logger logger = LogManager.getLogger(LogPoller.class);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "log-processing");
        consumer.subscribe(Collections.singletonList(topic));
        logger.info("Subscribed to topic--------------: " + topic);

        List<ConsumerRecord<String, String>> collected = new ArrayList<>();
        int maxRecords = 3;
        try {
            maxRecords = Integer.parseInt(System.getenv().getOrDefault("KAFKA_MAX_POLL_RECORDS", "3"));
            logger.info("Max records to poll----------: " + maxRecords);
        } catch (NumberFormatException ignored) {
            logger.info("Invalid value for KAFKA_MAX_POLL_RECORDS, using default: " + maxRecords);
        }
        try {
            int pollAttempts = 0;
            // int maxPollAttempts = 4;
            long deadline = System.currentTimeMillis() + 60_000;

            while (collected.size() < maxRecords && System.currentTimeMillis() < deadline) {
                pollAttempts++;
                logger.info("Polling Kafka for logs, attempt: " + pollAttempts);

                ConsumerRecords<String, String> batch = consumer.poll(Duration.ofSeconds(3));
                for (ConsumerRecord<String, String> record : batch) {
                    logger.info("Polled record: " + record.value() + " from partition: " + record.partition() + " with offset: " + record.offset());
                    collected.add(record);
                    if (collected.size() == maxRecords) {
                        break;
                    }
                }
            }
            logger.info("=================\n" + collected + "===================\n");
            return new PollResult(consumer, collected);
        }

        catch (Exception e) {
            consumer.close();
            throw new RuntimeException("Failed while polling Kafka", e);
        }
    }

}
