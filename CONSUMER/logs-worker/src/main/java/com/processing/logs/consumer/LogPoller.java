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

    public static List<ConsumerRecord<String, String>> pollLogs(Properties consumerProps) {

        final Logger logger = LogManager.getLogger(LogPoller.class);

        long start = System.currentTimeMillis();
        long timeLimit = 10000;

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "log-processing");
        consumer.subscribe(Collections.singletonList(topic));

        List<ConsumerRecord<String, String>> collected = new ArrayList<>();
        int maxRecords = 3;
        try {
            maxRecords = Integer.parseInt(System.getenv().getOrDefault("KAFKA_MAX_POLL_RECORDS", "3"));
        } catch (NumberFormatException ignored) {
        }
        try {
            while (collected.size() < maxRecords && (System.currentTimeMillis() - start) < timeLimit) {

                ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(3000));
                for (ConsumerRecord<String, String> record : batch) {
                    collected.add(record);
                    if (collected.size() == maxRecords)
                        break;
                }
            }
            logger.info("=================\n" + collected + "===================\n");
            return collected;
        }

        catch (Exception e) {
            throw new RuntimeException("Failed while polling Kafka", e);
        }

        finally {
            consumer.close();
        }
    }

}
