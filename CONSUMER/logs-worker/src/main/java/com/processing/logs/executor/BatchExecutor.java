package com.processing.logs.executor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.processing.logs.processor.LogMessageProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class BatchExecutor {

    public boolean processAll(List<ConsumerRecord<String, String>> records, int poolSize) {

        ExecutorService executor = Executors.newFixedThreadPool(poolSize);
        List<Future<Boolean>> futures = new ArrayList<>();

        final Logger logger = LogManager.getLogger(BatchExecutor.class);

        try {
            for (ConsumerRecord<String, String> record : records) {
                Future<Boolean> future = executor.submit(() -> LogMessageProcessor.process(record));
                futures.add(future);
            }

            boolean allSucceeded = true;

            for (int i = 0; i<futures.size(); i++){
                Future<Boolean> future = futures.get(i);
                try{
                    boolean res = future.get();
                    if (!res){
                        allSucceeded = false;
                        logger.info("Failed to process record: {}", records.get(i));
                    }
                    else{
                        logger.info("Successfully processed record: {}", records.get(i));
                    }
                }
                catch (Exception e){
                    allSucceeded = false;
                    logger.error("Exception while processing record: {}", records.get(i), e);
                }
            }

            return allSucceeded;

        } finally {
            executor.shutdown();
        }
    }
}