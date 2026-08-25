package com.processedlogs.aggregator.cron;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.net.URI;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class ScheduledAggregator {

    private static final String S3_ENDPOINT = System.getenv().getOrDefault("S3_ENDPOINT", "http://localhost:4566");
    private static final String S3_ACCESS_KEY = System.getenv().getOrDefault("S3_ACCESS_KEY", "test");
    private static final String S3_SECRET_KEY = System.getenv().getOrDefault("S3_SECRET_KEY", "test");
    private static final String S3_REGION = System.getenv().getOrDefault("S3_REGION", "us-east-1");

    private static final S3Client s3Client = S3Client.builder()
            .endpointOverride(URI.create(S3_ENDPOINT))
            .region(Region.of(S3_REGION))
            .credentialsProvider(
                    StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(S3_ACCESS_KEY, S3_SECRET_KEY)
                    )
            )
            .forcePathStyle(true)
            .build();

    private static final Path OUTPUT_DIR = Path.of("/data/log-processing/processed-logs");
    private static final String BUCKET = System.getenv().getOrDefault("S3_BUCKET", "processed-logs");
    private static final Logger logger = LogManager.getLogger(ScheduledAggregator.class);

    public static void main(String[] args) throws Exception {

        Files.createDirectories(OUTPUT_DIR);
        String filePath = "/data/log-processing/";

        Path infoPath = Path.of(filePath + "info-tmp.log");
        Path errorPath = Path.of(filePath + "error-tmp.log");
        Path debugPath = Path.of(filePath + "debug-tmp.log");
        Path warnPath = Path.of(filePath + "warn-tmp.log");
        Path anonymousPath = Path.of(filePath + "anonymous-tmp.log");

        ArrayList<Path> inputFiles = new ArrayList<>();
        inputFiles.add(infoPath);
        inputFiles.add(errorPath);
        inputFiles.add(debugPath);
        inputFiles.add(warnPath);
        inputFiles.add(anonymousPath);

        ExecutorService executor = Executors.newFixedThreadPool(5);
        List<Future<Boolean>> futures = new ArrayList<>();
        try {
            for (Path inputFile : inputFiles) {
                logger.info("Submitting aggregation task for {}", inputFile.getFileName());
                Future<Boolean> future = executor.submit(() -> aggregateOne(inputFile));
                futures.add(future);
            }

            boolean allSucceeded = true;
            for (Future<Boolean> future : futures) {
                try {
                    if (!future.get()) {
                        allSucceeded = false;
                    }
                } catch (Exception e) {
                    logger.error("Task threw an exception", e);
                    allSucceeded = false;
                }
            }

            if (allSucceeded) {
                logger.info("Aggregation run completed successfully");
                System.exit(0);
            } else {
                logger.error("One or more log types failed to aggregate");
                System.exit(1);
            }

        } finally {
            executor.shutdown();
        }
    }

    private static boolean aggregateOne(Path inputFile) {

        logger.info("------------------Starting aggregation for {}-----------------", inputFile.getFileName());

        if (!Files.exists(inputFile)) {
            logger.info("No pending data for {}, skipping", inputFile.getFileName());
            return true;
        }

        Path snapshotFile = renameFile(inputFile);
        if (snapshotFile == null) {
            return false;
        }

        Path outputFile = processFile(snapshotFile);
        if (outputFile == null) {
            return false;
        }

        try {
            uploadS3(outputFile);
        } catch (Exception e) {
            logger.error("Upload failed for " + outputFile.getFileName(), e);
            return false;
        }

        try {
            Files.deleteIfExists(snapshotFile);
            logger.info("Deleted snapshot file: " + snapshotFile.getFileName());
        } catch (IOException e) {
            logger.error("Upload succeeded but failed to delete snapshot " + snapshotFile.getFileName(), e);
        }

        return true;
    }

    private static Path renameFile(Path inputFile) {
        String fileName = inputFile.getFileName().toString();
        String renamedFileName = fileName + ".processing";
        Path renamedFile = inputFile.resolveSibling(renamedFileName);
        try {
            Files.move(inputFile, renamedFile, StandardCopyOption.ATOMIC_MOVE);
            logger.info("Renamed file: " + inputFile.getFileName() + " to " + renamedFile.getFileName());
            return renamedFile;
        } catch (IOException e) {
            logger.error("Error renaming file: " + inputFile.getFileName(), e);
            return null;
        }
    }

    private static Path processFile(Path snapshotFile) {
        String opfileName = snapshotFile.getFileName().toString().replace("-tmp.processing", "-processed");
        Path outputFile = OUTPUT_DIR.resolve(opfileName);

        try (
                BufferedReader reader = Files.newBufferedReader(snapshotFile);
                BufferedWriter writer = Files.newBufferedWriter(outputFile);) {
            String line;
            while ((line = reader.readLine()) != null) {
                // process here as you require - I am currently doing nothing. I still need to decide.
                writer.write(line);
                writer.newLine();
            }
            logger.info(
                    Thread.currentThread().getName() + " finished reading " + snapshotFile.getFileName().toString());
            logger.info("-------Uploading " + outputFile.getFileName().toString() + " to S3 ------");

            return outputFile;

        } catch (IOException e) {
            logger.error("Error processing file: " + snapshotFile.getFileName(), e);
            return null;
        }
    }

    private static void uploadS3(Path outputFile) {
        String fileName = outputFile.getFileName().toString();
        String folder = fileName.substring(0, fileName.lastIndexOf('.'));
        String key = "processed-logs/" + folder + "/" + fileName;

        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(BUCKET + "")
                .key(key)
                .build();

        s3Client.putObject(request, outputFile);

        logger.info("Uploaded " + outputFile.getFileName().toString() + " to S3 path " + key);

    }

}
