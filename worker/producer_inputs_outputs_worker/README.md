# Producer Inputs Outputs Worker

## Overview

The Producer Inputs Outputs Worker is a specialized Kafka producer that generates correlated input and output events for testing. It simulates complete event lifecycles with both input events and their expected output events, useful for end-to-end testing and validation.

## Features

- **Correlated Events**: Generate matched input and output event pairs
- **Test Data Generation**: Create comprehensive test scenarios
- **Expected Results**: Define expected outputs for validation
- **End-to-End Testing**: Support complete pipeline testing

## Architecture

### Main Components

- **ProducerInputsOutputApp**: Main application class
- Input event generation
- Output event generation
- Correlation ID management
- Kafka producer for both streams

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

Produces a shaded JAR with all dependencies.

## Running

```bash
java -jar target/producer_inputs_outputs_worker-1.0.0-SNAPSHOT.jar
```

## Configuration

### Kafka Topics

- **Input Topic**: Topic for input events
- **Output Topic**: Topic for expected output events

### Event Correlation

- Maintains correlation IDs between inputs and outputs
- Configurable delay between input and output publication
- Support for multiple output events per input

## Use Cases

- **Validation Testing**: Verify pipeline produces expected outputs
- **Regression Testing**: Ensure changes don't break existing functionality
- **Acceptance Testing**: Validate business requirements
- **Performance Testing**: Test with realistic input/output volumes

## Event Flow

```
Input Event → Kafka (input topic) → Flink Pipeline → Kafka (output topic) → Comparison with Expected Output
```

## Testing Workflow

1. Generate input events with known outcomes
2. Publish inputs to input topic
3. Publish expected outputs to validation topic
4. Run Flink pipeline
5. Compare actual outputs with expected outputs

## Related Modules

- `producer`: Basic producer implementation
- `producer_same_id_worker`: Same-ID producer variant
- `ai_worker`: Can use this for validation testing
- `common`: Shared utilities

