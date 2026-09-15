# Simulator

The simulator is a lightweight Spring Boot module used to generate sample log traffic for the ingestion pipeline.

## Purpose

This module is not the actual processing service. Its job is to act like a source system that emits structured log records over TCP so the rest of the pipeline can be tested end-to-end.

## How it works

- The Spring Boot application starts in `SimulatorApplication`.
- It creates a `TcpPusher` instance.
- The pusher opens a TCP connection to `localhost` on port `5001`.
- It sends several JSON log messages, each containing an `id`, `level`, and `message` field.

The messages are intentionally simple and easy to inspect while debugging the flow from socket input to Kafka topic to consumer processing.

## Key file

- `src/main/java/com/tcp/simulator/SimulatorApplication.java`
- `src/main/java/com/tcp/simulator/TcpPusher.java`

## Typical flow

1. Start the TCP ingestor.
2. Run the simulator.
3. The simulator writes log lines to the ingestor.
4. The ingestor maps each line to a Kafka partition and sends it to the processing pipeline.

## Notes

This is a test-oriented component meant for local validation and demonstration. It is intentionally small and focused on sending log events rather than processing them.
