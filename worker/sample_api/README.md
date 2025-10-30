# Sample API

## Overview

The Sample API is a high-performance Spring Boot WebFlux service that provides reactive REST endpoints for testing and enrichment operations. It's designed as a mock external service for testing the RMG pipeline's integration with external APIs.

## Features

- **Reactive Architecture**: Built on Spring WebFlux for high concurrency
- **Non-blocking I/O**: Netty-based reactive HTTP stack
- **Distributed Tracing**: OpenTelemetry integration for observability
- **Health Monitoring**: Spring Boot Actuator endpoints
- **Lookup Operations**: Simulated data lookup endpoints

## Architecture

### Main Components

- **SampleApi**: Main Spring Boot application entry point
- **LookupController**: REST controller for lookup operations
- **SampleApiConfig**: Application configuration
- **TraceparentResponseHeader**: Distributed tracing header handling
- **ReactorContextPropagationConfig**: Context propagation for reactive flows

## Dependencies

- Spring Boot Starter WebFlux
- Spring Boot Starter Actuator
- Spring Boot Starter Validation
- Spring Boot Starter AOP
- OpenTelemetry exporter (OTLP)
- Micrometer tracing with OpenTelemetry bridge

## Building and Running

### Build

```bash
mvn clean package
```

### Run

```bash
java -jar target/sample_api-1.0.0-SNAPSHOT.jar
```

Or using Maven:

```bash
mvn spring-boot:run
```

## API Endpoints

### Lookup Endpoints

Provides simulated lookup operations for:

- Address validation
- Postal code lookups
- Reference data queries
- Enrichment operations

### Actuator Endpoints

- `/actuator/health`: Health check
- `/actuator/metrics`: Application metrics
- `/actuator/info`: Application information

## Configuration

### Server Settings

- **Port**: Configurable server port (default: 8080)
- **Netty Options**: Connection pool, timeouts

### OpenTelemetry

- **OTLP Endpoint**: Traces export endpoint
- **Service Name**: Identifier for distributed tracing
- **Sampling Rate**: Trace sampling configuration

### Performance Tuning

- **Thread Pool**: Reactor thread configuration
- **Connection Pool**: HTTP client connection limits
- **Timeout Settings**: Request/response timeouts

## Reactive Programming

The service uses Project Reactor for reactive programming:

- **Mono**: Single-value async operations
- **Flux**: Multi-value async streams
- **WebClient**: Non-blocking HTTP client

## Observability

### Distributed Tracing

- OpenTelemetry integration for end-to-end tracing
- Automatic context propagation across async boundaries
- W3C Trace Context headers (traceparent)

### Metrics

- Request/response metrics
- Latency histograms
- Error rates
- JVM metrics

## SSL/TLS Support

The module includes certificate files for secure communication:

- `.crt`: Certificate file
- `.p12`: PKCS12 keystore

Configure SSL in application properties as needed.

## Use Cases

- **API Testing**: Mock external services for testing
- **Load Testing**: High-performance reactive endpoint testing
- **Integration Testing**: Simulate external system responses
- **Enrichment Simulation**: Test data enrichment flows
- **Tracing Validation**: Verify distributed tracing setup

## Performance Characteristics

- **Non-blocking**: Handles thousands of concurrent requests
- **Low Latency**: Minimal overhead for request processing
- **Scalable**: Efficient resource utilization with reactive streams
- **Resilient**: Backpressure handling and error recovery

## Main Class

```
com.hcltech.rmg.testapi.TestApiApplication
```

## Related Modules

- `flink_worker`: May call this API for enrichment
- `ai_worker`: May use for testing scenarios

