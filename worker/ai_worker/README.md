# AI Worker

## Overview

The AI Worker is a Spring Boot application that provides a RESTful API for managing Apache Flink jobs. It serves as a job orchestration service that allows users to start, monitor, and control Flink stream processing jobs through HTTP endpoints.

## Features

- **Job Management**: Start and manage Flink jobs via REST API
- **Job Status Monitoring**: Track job execution status and progress
- **Input/Output Comparison**: Compare expected vs actual job outputs
- **Error Handling**: Global exception handling with detailed error responses
- **Health Monitoring**: Spring Boot Actuator endpoints for health checks

## Architecture

### Main Components

- **AiWorker**: Main Spring Boot application entry point
- **JobController**: REST API controller for job management endpoints
- **JobCoordinator**: Service layer for job lifecycle management
- **JobBuilder**: Factory for creating and configuring Flink jobs
- **LocalFlinkJobAdapter**: Adapter for running Flink jobs locally
- **InputOutputComparator**: Utility for validating job outputs

### API Endpoints

- `POST /api/jobs/start`: Start a new Flink job
- `GET /api/jobs/{id}/status`: Get job status

## Dependencies

- Spring Boot (Web, Validation, Actuator)
- Apache Flink (Streaming, Core)
- Kafka integration
- Application container implementation
- Shared worker utilities
- Message processing modules

## Building and Running

### Build

```bash
mvn clean package
```

### Run

```bash
java -jar target/ai_worker-1.0.0-SNAPSHOT.jar
```

Or using Maven:

```bash
mvn spring-boot:run
```

## Configuration

The application uses Spring Boot configuration. Configure the following properties:

- Server port
- Flink configuration parameters
- Kafka connection settings
- Job-specific configurations

## Main Class

```
com.hcltech.rmg.worker.AiWorker
```

## Related Modules

- `shared_worker`: Common worker utilities and pipeline builders
- `flinkadapter`: Flink integration layer
- `appcontainer_implementation`: Application container framework
- `messages`: Message processing definitions
- `kafka`: Kafka connector implementations

