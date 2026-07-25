package com.processing.logs.processor;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class LogMessageProcessor {

    private static final Object INFO_LOCK = new Object();
    private static final Object DEBUG_LOCK = new Object();
    private static final Object ERROR_LOCK = new Object();
    private static final Object WARN_LOCK = new Object();
    private static final Object ANONYMOUS_LOCK = new Object();

    private static final Path INFO_FILE = Path.of("/tmp/info-tmp.log");
    private static final Path DEBUG_FILE = Path.of("/tmp/debug-tmp.log");
    private static final Path ERROR_FILE = Path.of("/tmp/error-tmp.log");
    private static final Path WARN_FILE = Path.of("/tmp/warn-tmp.log");
    private static final Path ANONYMOUS_FILE = Path.of("/tmp/anonymous-tmp.log");

    public static void writeToFile(String value, Path filePath){
        try {
                    Files.writeString(filePath, value + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to write info log", e);
                }
    }

    public static boolean process(ConsumerRecord<String, String> record) {

        final Logger logger = LogManager.getLogger(LogMessageProcessor.class);

        int partition = record.partition();
        String value = record.value();
        long offset = record.offset();
        String partitionKey = "offset-marker:partition-" + partition;

        Object lock = switch (partition){
            case 0 -> INFO_LOCK;
            case 1 -> DEBUG_LOCK;
            case 2 -> ERROR_LOCK;
            case 3 -> WARN_LOCK;
            default -> ANONYMOUS_LOCK;
        };

        Path file = switch (partition){
            case 0 -> INFO_FILE;
            case 1 -> DEBUG_FILE;
            case 2 -> ERROR_FILE;
            case 3 -> WARN_FILE;
            default -> ANONYMOUS_FILE;
        };

       synchronized(lock){
        if (OffsetGuard.isAlreadyProcessed(partitionKey, offset)){
            logger.info("Offset {} on partition {} already processed, skipping ------", offset, partition);
            return true;
        }
        if (partition != 0 && partition != 1 && partition != 2 && partition != 3) {
                logger.warn("Unrecognized partition {} for record, writing to anonymous file: {} ---------", partition, value);
            }
        writeToFile(value, file);
        OffsetGuard.markProcessed(partitionKey, offset);
       }
        return true;
    }
}