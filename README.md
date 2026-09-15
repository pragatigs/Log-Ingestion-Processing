# Log Ingestion Processing

This repository contains an end-to-end log ingestion pipeline that simulates real-time log traffic, ingests it over TCP, routes it to Kafka, and processes it through the consumer worker before writing results to files.

The project is split into three main areas:

- Consumer processing: [CONSUMER/README.md](CONSUMER/README.md)
- Simulator: [simulator/README.md](simulator/README.md)
- TCP ingestor: [TCP-INGESTOR-MW/README.md](TCP-INGESTOR-MW/README.md)

## Project brief

The flow is intentionally simple and easy to reason about:

1. The simulator starts a TCP client and emits JSON log records - this is built just for testing
2. The TCP ingestor listens for incoming connections, reads log lines, and sends them to Kafka. this is too optional
3. The consumer worker reads from Kafka, batches messages, processes them, and writes them to output files. - heart of the project
4. The pipeline is designed to run in a containerized or Kubernetes environment with clear separation between ingestion, processing, and storage.

This repository is useful for understanding how log ingestion, partitioning, Kafka offsets, and batch processing fit together in a practical streaming setup.

## Module overview

### Consumer

The consumer side is implemented under [CONSUMER](CONSUMER). It contains the worker that consumes Kafka log messages, processes them in batches, stores deduplication state, and commits offsets only after successful processing.

See: [CONSUMER/README.md](CONSUMER/README.md)

### Simulator

The simulator module is a lightweight Spring Boot app that pushes sample log payloads to the TCP ingestor over localhost. It is meant to generate traffic for testing the ingestion pipeline.

See: [simulator/README.md](simulator/README.md)

### TCP ingestor

The TCP ingestor module listens for socket connections, reads log payloads, routes them by level, and forwards them to Kafka for downstream processing.

See: [TCP-INGESTOR-MW/README.md](TCP-INGESTOR-MW/README.md)

## Repository layout

- [CONSUMER](CONSUMER)
- [simulator](simulator)
- [TCP-INGESTOR-MW](TCP-INGESTOR-MW)

---

This repository is designed as a practical demonstration of a log processing pipeline with clear module boundaries and easy local experimentation.
