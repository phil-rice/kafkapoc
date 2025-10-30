# Shared Worker

## Overview

The Shared Worker module is a library containing common components, utilities, and building blocks used across multiple worker applications. It provides reusable pipeline components, Kafka helpers, serialization handlers, and other shared functionality for Flink streaming applications.

## Features

- **Pipeline Building**: Reusable Flink pipeline construction utilities
- **Kafka Integration**: Common Kafka source and sink configurations
- **Serialization**: Custom serializers for different message types
- **Routing Logic**: Event routing and distribution logic
- **First-hit Handling**: Utilities for first-event processing scenarios
- **Domain Message Generation**: Message creation and transformation utilities

## Architecture

### Core Components

- **BuildPipeline**: Utilities for constructing Flink data pipelines
- **KafkaFlinkHelper**: Helper methods for Kafka-Flink integration
- **EnvelopeRouting**: Event routing logic based on message types
- **EnvelopeKafkaSinks**: Kafka sink configurations for different output types
- **DomainMessageGenerator**: Generate domain-specific messages

### First-Hit Processing

- **FirstHitFlatMap**: FlatMap function for first-occurrence detection
- **FirstHitJobKiller**: Utility for terminating jobs after first hit (testing)

### Serialization Components

Located in `serialisation/` package:

- **ComposedRecordSerializer**: Serializer for composed records
- **ErrorRecordSerializer**: Error message serialization
- **ValueRecordSerializer**: Value record serialization
- **RetryRecordSerializer**: Retry record serialization
- **AiFailureRecordSerializer**: AI failure record serialization
- **HeaderPopulator**: Kafka header population utilities
- **HeaderPopulators**: Factory for header populators

## Dependencies

- Apache Flink (Streaming, Core, Clients, Connector Kafka, RocksDB)
- All execution modules
- Common utilities
- Application container implementation
- Messages module
- Flink adapter
- Kafka connector
- Prometheus metrics
- Commons JXPath
- Jackson (databind, JSR310)

## Building

```bash
mvn clean package
```

This produces a JAR that can be used as a dependency in other worker modules.

## Usage

### In Other Modules

Add as a dependency in your `pom.xml`:

```xml
<dependency>
    <groupId>com.hcltech.rmg</groupId>
    <artifactId>shared_worker</artifactId>
    <version>${project.version}</version>
</dependency>
```

### Pipeline Building

```java
// Use BuildPipeline utilities to construct your Flink pipeline
BuildPipeline.create()
    .withSource(...)
    .withTransformations(...)
    .withSink(...);
```

### Kafka Integration

```java
// Use KafkaFlinkHelper for common Kafka operations
KafkaFlinkHelper.createSource(properties);
KafkaFlinkHelper.createSink(properties);
```

### Serialization

```java
// Use custom serializers for different record types
new ComposedRecordSerializer();
new ErrorRecordSerializer();
new ValueRecordSerializer();
```

## Components in Detail

### BuildPipeline

Provides builder pattern for Flink pipeline construction:

- Source configuration
- Transformation chaining
- Sink configuration
- State backend setup

### KafkaFlinkHelper

Helper methods for:

- Kafka source creation with proper deserialization
- Kafka sink creation with proper serialization
- Consumer group management
- Offset management strategies

### EnvelopeRouting

Routing logic for different message types:

- Route by message type
- Route by business rules
- Dynamic routing based on content

### EnvelopeKafkaSinks

Predefined sink configurations for:

- Success records
- Error records
- Retry records
- AI failure records

### Serializers

Custom Kafka serializers implementing `KafkaRecordSerializationSchema`:

- Handle different record types
- Populate Kafka headers
- Support versioning and schema evolution

## State Management

Includes utilities for:

- RocksDB state backend configuration
- State TTL management
- Checkpointing configuration
- Savepoint handling

## Best Practices

1. **Reusability**: Extract common patterns to this module
2. **Type Safety**: Use strongly-typed builders and utilities
3. **Configuration**: Externalize configuration, avoid hardcoding
4. **Testing**: Include unit tests for shared components
5. **Documentation**: Document public APIs and usage patterns

## Modules Using Shared Worker

- `flink_worker`: Main Flink streaming application
- `performance`: Performance testing harness
- `ai_worker`: AI worker orchestration service
- `producer`: Event producers (all variants)

## Related Modules

- `flinkadapter`: Flink integration layer
- `all_execution`: Complete execution logic
- `messages`: Message type definitions
- `kafka`: Kafka connector implementation
- `common`: Core utilities

