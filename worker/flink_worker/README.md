# Flink Worker

## Overview

The Flink Worker is the main Apache Flink streaming application for processing events from Kafka. It sets up and executes the complete Flink data processing pipeline, including event consumption, transformation, business logic execution, and output generation.

## Features

- **Kafka Integration**: Consume from and produce to Kafka topics
- **Stream Processing**: Real-time event processing with Apache Flink
- **Business Logic Execution**: Execute complex business rules and transformations
- **State Management**: Maintain processing state across events
- **Metrics & Monitoring**: Prometheus metrics integration for observability

## Architecture

### Main Components

- **FlinkWorker**: Main Flink job entry point and pipeline orchestrator
- Integration with shared pipeline components
- Kafka source and sink configurations
- Custom serializers for different record types

## Dependencies

- Apache Flink (Streaming, Core, Clients, Connector Kafka)
- Kafka connector
- Application container implementation
- All execution modules
- Message processing
- Shared worker utilities
- Common utilities
- Flink adapter
- Prometheus metrics
- Commons JXPath for path expressions

## Building

```bash
mvn clean package
```

This produces a JAR suitable for Flink cluster deployment.

## Running

### Local Execution

```bash
mvn exec:java -Dexec.mainClass="com.hcltech.rmg.performance.FlinkWorker"
```

### Flink Cluster Deployment

```bash
flink run target/flink_worker-1.0.0-SNAPSHOT.jar
```

## Configuration

Configure the following for Flink job execution:

- **Kafka Settings**: Bootstrap servers, topic names, consumer groups
- **Flink Parameters**: Parallelism, checkpointing, state backend
- **Business Logic**: Rules and transformation configurations
- **Metrics**: Prometheus pushgateway URL

## Pipeline Components

The worker utilizes components from `shared_worker`:

- Pipeline builders
- Kafka helpers
- Routing logic
- Serialization handlers

## State Management

The application uses Flink's state management capabilities to maintain:

- Processing state across events
- Aggregations and windowing results
- Stateful business logic

## Monitoring

- Flink Web UI for job monitoring
- Prometheus metrics for custom metrics
- Kafka consumer lag monitoring

## Related Modules

- `shared_worker`: Common pipeline building blocks
- `flinkadapter`: Flink integration utilities
- `all_execution`: Complete execution logic
- `messages`: Message type definitions
- `kafka`: Kafka connector implementation

