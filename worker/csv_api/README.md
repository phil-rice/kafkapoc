# CSV API

## Overview

The CSV API is a Spring Boot REST service that provides HTTP endpoints for CSV data operations. It exposes the CSV parsing and processing capabilities from the common module as a web service.

## Features

- **CSV Processing**: Parse and process CSV files via REST API
- **RESTful Endpoints**: Clean REST API for CSV operations
- **Error Handling**: Global exception handler for robust error responses
- **Spring Boot Integration**: Leverages Spring Boot Web for rapid development

## Architecture

### Main Components

- **CsvApiApp**: Main Spring Boot application entry point
- **CsvController**: REST controller for CSV-related endpoints
- **CsvService**: Core service layer for CSV processing logic
- **GlobalExceptionHandler**: Centralized exception handling

## Dependencies

- Spring Boot Starter Web
- Spring Boot Starter Test (test scope)
- Common module (for CSV utilities)

## Building and Running

### Build

```bash
mvn clean package
```

### Run

```bash
java -jar target/csv_api-1.0.0-SNAPSHOT.jar
```

Or using Maven:

```bash
mvn spring-boot:run
```

## API Endpoints

The service provides REST endpoints for CSV operations. Check the `CsvController` class for available endpoints.

## Configuration

Standard Spring Boot configuration applies. Configure:

- Server port (default: 8080)
- CSV processing parameters
- File upload limits (if applicable)

## Use Cases

- Exposing CSV processing capabilities as a microservice
- Integration with other services requiring CSV operations
- Testing and validation of CSV data through HTTP interface

## Related Modules

- `common`: Provides core CSV parsing and processing utilities

