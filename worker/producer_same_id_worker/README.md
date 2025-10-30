# Producer Same ID Worker

## Overview

The Producer Same ID Worker is a specialized Kafka producer designed to generate multiple events with the same correlation ID. This is useful for testing stateful processing, event aggregation, and scenarios where multiple related events need to be processed together.

## Features

- **Same ID Events**: Generate multiple events sharing the same ID
- **Stateful Testing**: Test state management and aggregation logic
- **Sequence Generation**: Create ordered event sequences
- **Configurable Multiplicity**: Control number of events per ID

## Architecture

### Main Components

- **ProducerInputsOutputApp**: Main application class (note: may share implementation with inputs/outputs variant)
- ID generation strategy
- Event sequencing
- Kafka producer configuration

## Dependencies

- Apache Kafka Clients
- Common module
- Shared worker utilities
- SLF4J logging
- Logback (runtime)

## Building

```bash
mvn clean package
```

Creates a shaded JAR with all dependencies included.

## Running

```bash
java -jar target/producer_same_id_worker-1.0.0-SNAPSHOT.jar
```

## Configuration

### Event Generation Parameters

- **events.per.id**: Number of events to generate per ID
- **id.count**: Number of unique IDs to generate
- **delay.ms**: Delay between events with same ID
- **topic**: Target Kafka topic

## Use Cases

### Stateful Processing Tests

- **Aggregation**: Test event aggregation by key
- **Windowing**: Verify windowing logic with related events
- **Session Windows**: Test session window creation and management
- **Pattern Detection**: Validate complex event pattern matching

### Scenarios

1. **Order Processing**: Multiple line items for same order
2. **User Sessions**: Multiple events in same user session
3. **Transaction Chains**: Related transactions with same correlation ID
4. **Multi-step Workflows**: Events representing workflow steps

## Event Patterns

### Sequential Events

```
ID-1: Event 1 → Event 2 → Event 3
ID-2: Event 1 → Event 2 → Event 3
```

### Interleaved Events

```
ID-1: Event 1 → ID-2: Event 1 → ID-1: Event 2 → ID-2: Event 2
```

## Testing Workflow

1. Configure number of events per ID
2. Generate events with same correlation IDs
3. Publish to Kafka topic
4. Verify Flink pipeline correctly groups and processes related events
5. Validate state management and aggregation results

## Related Modules

- `producer`: Basic producer implementation
- `producer_inputs_outputs_worker`: Inputs/outputs producer variant
- `shared_worker`: Common worker utilities
- `flink_worker`: Processes events with stateful operations

