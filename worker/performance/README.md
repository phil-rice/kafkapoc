# Performance Worker

## Overview

The Performance Worker is a specialized Apache Flink application designed for performance testing and benchmarking of the stream processing pipeline. It provides a harness for measuring throughput, latency, and resource utilization under various load conditions.

## Features

- **Performance Testing**: Benchmark processing throughput and latency
- **Load Generation**: Support for high-volume event processing
- **RocksDB State Backend**: Enhanced state management for performance testing
- **Metrics Collection**: Comprehensive metrics via Prometheus
- **Resource Monitoring**: Track CPU, memory, and I/O utilization

## Architecture

### Main Components

- **PerfHarnessMain**: Main entry point for performance testing harness
- Integration with complete execution pipeline
- RocksDB-backed state management
- Kafka source/sink with performance optimizations

## Dependencies

- Apache Flink (Streaming, Core, Clients, Connector Kafka, RocksDB State Backend)
- All execution modules
- Shared worker utilities
- Common utilities
- Application container implementation
- Messages module
- Flink adapter
- Kafka connector
- Prometheus metrics
- Commons JXPath

## Building

```bash
mvn clean package
```

## Running Performance Tests

### Local Execution

```bash
mvn exec:java -Dexec.mainClass="com.hcltech.rmg.performance.PerfHarnessMain"
```

### Flink Cluster

```bash
flink run -p <parallelism> target/performance-1.0.0-SNAPSHOT.jar
```

## Configuration

### Performance Tuning Parameters

- **Parallelism**: Number of parallel task instances
- **Buffer Timeout**: Network buffer flush timeout
- **Checkpoint Interval**: Frequency of state snapshots
- **RocksDB Options**: State backend tuning parameters

### Test Scenarios

Configure different test scenarios:

- High throughput (events/sec)
- Low latency requirements
- State size variations
- Different parallelism levels

## Metrics

Monitor the following metrics:

- **Throughput**: Events processed per second
- **Latency**: End-to-end processing time (P50, P95, P99)
- **Backpressure**: Pipeline bottlenecks
- **State Size**: Growth and compaction
- **Resource Usage**: CPU, memory, disk I/O

## RocksDB State Backend

This module includes RocksDB for scalable state management:

- Larger-than-memory state
- Better performance for large state
- Tunable for different workloads

## Best Practices

1. **Warm-up Period**: Run warmup before collecting metrics
2. **Steady State**: Ensure system reaches steady state
3. **Multiple Runs**: Perform multiple test runs for statistical significance
4. **Realistic Data**: Use production-like data volumes and patterns
5. **Resource Isolation**: Run tests in isolated environments

## Related Modules

- `shared_worker`: Shared pipeline components
- `flink_worker`: Production Flink worker
- `all_execution`: Complete execution logic

