package com.processing.logs.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class OffsetCommitter {

    public static void commitOffsets(List<ConsumerRecord<String, String>> records, Properties consumerProps) {
          
        final Logger logger = LogManager.getLogger(OffsetCommitter.class);


        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();

        for(ConsumerRecord<String,String> record : records){
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
            OffsetAndMetadata currentHighest = offsetsToCommit.get(topicPartition);
            if (currentHighest == null || record.offset() >= currentHighest.offset()) {
                offsetsToCommit.put(topicPartition, new OffsetAndMetadata(record.offset() + 1));
            }

        }

         KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
         try{
           consumer.commitSync(offsetsToCommit);
         }
         catch (Exception e){
            logger.error("Failed to commit offset ", e);
            throw new RuntimeException("Offset commit failed", e);
         }
         finally{
            consumer.close();
         }
    }
}