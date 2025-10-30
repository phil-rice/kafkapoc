# Producer

## Overview

The Producer module is a Kafka producer application that generates and publishes test events to Kafka topics. It's designed for testing and development purposes, allowing developers to simulate event streams for the Flink processing pipeline.

## Features

- **Event Generation**: Create test events with configurable patterns
- **Kafka Publishing**: Send events to specified Kafka topics
- **Configurable Rate**: Control event generation rate
- **Multiple Event Types**: Support for various event schemas

## Architecture

### Main Components

- Kafka producer configuration
- Event generation logic
- Message serialization
- Rate limiting and throttling

## Dependencies

- Apache Kafka Clients
- Common module (utilities)
- Shared worker (common functionality)
- SLF4J logging
- Logback (runtime)

## Building

```bash
mvn clean package
```

This creates a shaded JAR with all dependencies included.

## Running

```bash
java -jar target/producer-1.0.0-SNAPSHOT.jar
```

## Configuration

Configure the producer using properties:

### Kafka Settings

- **bootstrap.servers**: Kafka broker addresses
- **topic**: Target topic name
- **key.serializer**: Serializer for message keys
- **value.serializer**: Serializer for message values

### Event Generation

- **rate**: Events per second
- **duration**: Total test duration
- **pattern**: Event generation pattern (constant, burst, etc.)

## Use Cases

- **Development Testing**: Generate events for local development
- **Integration Testing**: Populate test environments with data
- **Load Testing**: Generate high-volume event streams
- **Scenario Testing**: Create specific event sequences for testing edge cases

## Configuration Files

Check the `src/main/resources` directory for:

- `producer.properties`: Producer configuration
- Sample XML files for event templates

## Related Modules

- `producer_inputs_outputs_worker`: Variant for inputs/outputs testing
- `producer_same_id_worker`: Variant for same-ID event testing
- `common`: Shared utilities
- `shared_worker`: Common worker functionality

