package com.processing.logs;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.processing.logs.config.KafkaConsumerConfig;
import com.processing.logs.consumer.LogPoller;
import com.processing.logs.consumer.OffsetCommitter;
import com.processing.logs.executor.BatchExecutor;
import com.processing.logs.processor.OffsetGuard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SpringBootApplication
public class LogsApplication {

	public static void main(String[] args) {

		final Logger logger = LogManager.getLogger(LogsApplication.class);

		Properties props =KafkaConsumerConfig.buildProperties();

		LogPoller.PollResult pollResult = LogPoller.pollLogs(props);
		List<ConsumerRecord<String,String>> records = pollResult.records();

		Integer batchSize = Integer.parseInt(System.getenv().getOrDefault("KAFKA_MAX_POLL_RECORDS", "3"));

		BatchExecutor batchExecutor = new BatchExecutor();
		boolean allSucceeded = batchExecutor.processAll(records, batchSize);

		try {
			if (allSucceeded){
				OffsetCommitter.commitOffsets(records, pollResult.consumer());
				for (ConsumerRecord<String, String> record : records) {
					OffsetGuard.clearProcessed("offset-marker:partition-" + record.partition(), record.offset());
				}
				System.exit(0);
			}
			else{
				logger.error("All messages not succeeded, hence offsets not committed");
				System.exit(1);
			}
		} finally {
			pollResult.consumer().close();
		}
		

	}

}
