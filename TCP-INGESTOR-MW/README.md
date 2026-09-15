# TCP ingestor

The TCP ingestor is the entry point for incoming log traffic. It accepts socket connections, reads log lines, and forwards them to Kafka for downstream processing.

## Purpose

This module sits between the simulator and the consumer worker. It receives messages from clients over TCP, determines the correct Kafka partition, and publishes the payload to the `log-processing` topic.

## How it works

- `TcpServerService` starts a `ServerSocket` and accepts inbound client connections.
- `ConnectionHandler` reads data from each socket connection line by line.
- `LogRouterService` decides which partition each log should be sent to.
- `KafkaProducerService` sends the payload to Kafka.

The ingestion layer is designed to handle multiple connections concurrently by delegating each accepted socket to the task executor.

## Main components

- `IngestorApplication` — bootstraps the Spring Boot application
- `TcpServerService` — opens the TCP listener and accepts connections
- `ConnectionHandler` — reads incoming data and routes it
- `LogRouterService` — maps log data to Kafka partitions
- `KafkaProducerService` — publishes messages to Kafka

## Configuration

The ingestor uses the `TCP_PORT` environment variable to determine which port to bind to. In the local flow, it typically listens on a port such as `5001`.

## Local testing flow

1. Start the ingestor application.
2. Run the simulator.
3. The simulator connects to the ingestor and sends sample log lines.
4. The ingestor routes them to Kafka.
5. The consumer worker picks the records up and processes them.

## Related docs

- [../CONSUMER/README.md](../CONSUMER/README.md)
- [../README.md](../README.md)
- [resources/how-it-works.txt](resources/how-it-works.txt)

---

This module is the bridge between raw TCP log input and the Kafka-backed processing pipeline.
