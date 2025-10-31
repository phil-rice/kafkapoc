# RMG Kafka POC

A proof-of-concept Apache Flink streaming application for processing Royal Mail Group parcel tracking events with Kafka/EventHub integration, CEL-based business logic execution, and comprehensive observability.

---

## Table of Contents

- [Getting Started](#getting-started)
- [Building the Project](#building-the-project)
- [Testing](#testing)
- [Running Locally](#running-locally)
- [Monitoring & Observability](#monitoring--observability)
- [Project Architecture](#project-architecture)
- [Module Overview](#module-overview)
- [Quick Start Summary](#quick-start-summary)

---

## Getting Started

### Prerequisites

Before you begin, ensure you have the following installed:

- **Java 21** (JDK 21 or higher)
  ```bash
  java -version  # Should show version 21+
  ```

- **Apache Maven 3.8+**
  ```bash
  mvn -version
  ```

- **Docker & Docker Compose**
  ```bash
  docker --version
  docker compose version
  ```

- **Git**
  ```bash
  git --version
  ```

### Clone the Repository

```bash
git clone <repository-url>
cd kafkapoc
```

---

## Building the Project

### Compile All Modules

Build the entire multi-module Maven project:

```bash
mvn clean install
```

This will:
- Compile all Java source files
- Run unit tests
- Package JARs for all modules
- Install artifacts to local Maven repository

### Build Without Tests

To speed up the build during development:

```bash
mvn clean install -DskipTests
```

### Build Specific Module

To build only a specific module (e.g., `flink_worker`):

```bash
cd worker/flink_worker
mvn clean package
```

### Build Output

After building, JAR files will be located in each module's `target/` directory:
- `worker/flink_worker/target/flink_worker-1.0.0-SNAPSHOT.jar` - The Main Flink Worker, intended to be used in the production environment
- `worker/flink_worker/target/performance-1.0.0-SNAPSHOT.jar` - A standalone 'for development' application that allows everything to be tested in dev
- `worker/ai_worker/target/ai_worker-1.0.0-SNAPSHOT.jar` - Used to support the migration demonstration
- `worker/sample_api/target/sample_api-1.0.0-SNAPSHOT.jar` - A sample API showing the use of observability


---

## Testing

### Run All Tests

Execute all unit tests across all modules:

```bash
mvn test
```

### Run Tests for Specific Module

```bash
cd worker/flink_worker
mvn test
```

### Run Specific Test Class

```bash
mvn test -Dtest=EnvelopeTest
```

### Generate Test Coverage Report

The project uses JaCoCo for code coverage:

```bash
mvn clean verify
```

View the aggregated coverage report:
```bash
open coverage-report/target/site/jacoco-aggregate/index.html
```

Or navigate to: `coverage-report/target/site/jacoco-aggregate/index.html`

### Testing Framework

- **JUnit 5**: Unit testing framework
- **Mockito**: Mocking framework for unit tests
- **Flink Test Utils**: For testing Flink pipelines

---

## Running Locally

### Step 1: Start Infrastructure Services

Start Kafka, Prometheus, and RedPanda Console using Docker Compose:

```bash
docker compose up -d
```

This starts:
- **Kafka**: Apache Kafka broker (localhost:9092)
- **RedPanda Console**: Kafka UI at http://localhost:8081
- **Prometheus**: Metrics collection at http://localhost:9090

Verify services are running:
```bash
docker compose ps
```

### Step 2: Run the Flink Worker

The Flink Worker is the main streaming application that processes events from Kafka.

#### Option A: Run from IDE

1. Open `worker/flink_worker/src/main/java/com/hcltech/rmg/performance/FlinkWorker.java`
2. Run the `main()` method

#### Option B: Run from Maven

```bash
cd worker/flink_worker
mvn exec:java -Dexec.mainClass="com.hcltech.rmg.performance.FlinkWorker"
```

#### Option C: Run Packaged JAR

```bash
cd worker/flink_worker
mvn clean package
java -jar target/flink_worker-1.0.0-SNAPSHOT.jar
```

**What the Flink Worker Does:**
- Consumes events from Kafka input topics
- Executes business logic using CEL expressions
- Performs data enrichment and transformations
- Routes processed events to output Kafka topics
- Exposes metrics on port 9400

**Flink Web UI:**
When running locally, access the Flink dashboard at:
- http://localhost:8081 (Note: May use a different port if 8081 is taken by RedPanda Console)

### Step 3: Run the AI Worker (Optional)

The AI Worker provides a REST API for managing Flink jobs programmatically.

```bash
cd worker/ai_worker
mvn spring-boot:run
```

Or run the JAR:
```bash
java -jar target/ai_worker-1.0.0-SNAPSHOT.jar
```

**API Endpoints:**
- `POST /api/jobs/start`: Start a new Flink job
- `GET /api/jobs/{id}/status`: Get job status
- Health check: http://localhost:8080/actuator/health

### Step 4: Run Sample API (Optional)

The Sample API is a reactive mock service for testing enrichment operations.

```bash
cd worker/sample_api
mvn spring-boot:run
```

**Configuration:**
- Runs on port 1235 (HTTPS enabled with self-signed cert)
- Provides lookup endpoints for address validation
- OpenTelemetry tracing enabled

### Step 5: Run CSV API (Optional)

The CSV API provides REST endpoints for CSV processing operations.

```bash
cd worker/csv_api
mvn spring-boot:run
```

Runs on port 8080 by default.

---

## Monitoring & Observability

### Local Monitoring Stack

#### 1. Kafka Monitoring - RedPanda Console

**URL:** http://localhost:8081

**Features:**
- View Kafka topics and messages
- Browse message content
- Monitor consumer groups and lag
- View topic configurations
- Real-time message inspection

**How to Use:**
1. Navigate to http://localhost:8081
2. Click "Topics" to see all Kafka topics
3. Click a topic to browse messages
4. Use "Consumer Groups" to monitor lag

#### 2. Metrics - Prometheus

**URL:** http://localhost:9090

**Features:**
- Time-series metrics collection
- Query metrics using PromQL
- View graphs and time-series data

**Configuration:**
The Prometheus scrape configuration is in `prometheus.yml`:
```yaml
scrape_configs:
  - job_name: "myapp-host"
    static_configs:
      - targets: ["host.docker.internal:9400"]
```

**How to View Metrics:**
1. Navigate to http://localhost:9090
2. Go to "Graph" tab
3. Enter a PromQL query (e.g., `rate(messages_processed_total[1m])`)
4. Click "Execute"

**Available Metrics:**
- Flink job metrics (throughput, latency, backpressure)
- Custom business metrics (envelopes, errors, retries)
- JVM metrics (heap, GC, threads)

#### 3. Application Logs

**Log Location:** Console output (stdout)

**Log Configuration:**
Each module has its own `logback.xml` configuration in `src/main/resources/`:
- `worker/flink_worker/src/main/resources/logback.xml`
- `worker/ai_worker/src/main/resources/logback.xml`
- etc.

**Log Levels:**
- Most modules use `WARN` level by default for cleaner output
- To enable `DEBUG` logging, modify the `logback.xml` file:
  ```xml
  <root level="DEBUG">
    <appender-ref ref="CONSOLE"/>
  </root>
  ```

**Viewing Logs:**
Logs are written to console where you ran the application. For Docker services:
```bash
# View Kafka logs
docker compose logs kafka -f

# View all service logs
docker compose logs -f
```

#### 4. Flink Web UI

When running the Flink Worker locally, a web UI is available for monitoring:

**URL:** http://localhost:8081 (or check console output for actual port)

**Features:**
- Job execution details
- Task metrics and parallelism
- Checkpoint and savepoint information
- Backpressure monitoring
- Task manager resources
- Exception and failure tracking

**How to Use:**
1. Start the Flink Worker
2. Navigate to the Flink Web UI URL shown in console
3. View running jobs under "Jobs"
4. Click a job to see detailed metrics

#### 5. Spring Boot Actuator (AI Worker / Sample API / CSV API)

Spring Boot applications expose actuator endpoints:

**AI Worker:**
- Health: http://localhost:8080/actuator/health
- Metrics: http://localhost:8080/actuator/metrics
- Info: http://localhost:8080/actuator/info

**Sample API:**
- Health: http://localhost:1235/actuator/health

**Available Endpoints:**
- `/actuator/health`: Application health status
- `/actuator/metrics`: Available metric names
- `/actuator/metrics/{name}`: Specific metric details
- `/actuator/info`: Application information

#### 6. OpenTelemetry Tracing

The Sample API includes OpenTelemetry integration for distributed tracing.

**Configuration:**
- OTLP exporter configured in `worker/sample_api/src/main/resources/application.yaml`
- Automatic context propagation across async boundaries
- W3C Trace Context headers (traceparent)

**Note:** This is configured but requires an external tracing backend (e.g., Jaeger, Zipkin) which is not included in the Docker Compose setup.

### Monitoring Checklist

✅ **Is Kafka working?**
- Check RedPanda Console: http://localhost:8081
- Verify topics are created
- Check for messages in topics

✅ **Is Flink processing events?**
- Check Flink Web UI
- View metrics in Prometheus: `rate(envelopes_total[1m])`
- Check console logs for processing stats

✅ **Are there errors?**
- Check error topics in RedPanda Console
- View error metrics in Prometheus
- Search logs for ERROR or WARN

✅ **Is the system healthy?**
- Check actuator health endpoints
- Monitor consumer lag in RedPanda Console
- Check Flink checkpoint success rate

---

## Project Architecture

### High-Level Architecture

```
┌─────────────┐      ┌──────────────────┐      ┌─────────────┐
│   Kafka     │─────▶│  Flink Worker    │─────▶│   Kafka     │
│  (Input)    │      │  (Processing)    │      │  (Output)   │
└─────────────┘      └──────────────────┘      └─────────────┘
                              │
                              ├─▶ Business Logic (CEL)
                              ├─▶ Enrichment (External API)
                              ├─▶ Transformation
                              └─▶ Validation
                              
                     ┌──────────────────┐
                     │   Prometheus     │
                     │   (Metrics)      │
                     └──────────────────┘
```

### Key Components

1. **Flink Worker** (`worker/flink_worker`)
   - Main streaming application
   - Consumes from Kafka
   - Executes business logic pipeline
   - Produces to Kafka
   - Exposes Prometheus metrics

2. **AI Worker** (`worker/ai_worker`)
   - REST API for job orchestration
   - Spring Boot application
   - Manages Flink job lifecycle

3. **Business Logic** (`Business_logic/`)
   - CEL (Common Expression Language) expressions
   - YAML-based rule definitions
   - Event-driven processing rules

4. **Pipeline Components**
   - `execution/`: Business logic, enrichment, transformation, validation modules
   - `messages/`: Envelope and message handling
   - `dag/`: Directed acyclic graph processing
   - `cel/`: CEL expression evaluation

5. **Infrastructure**
   - `kafka/`: Kafka integration and configuration
   - `flink/`: Flink adapters and utilities
   - `metrics/`: Prometheus and OpenTelemetry integration

### Data Flow

1. **Input**: Events arrive on Kafka input topics as XML messages
2. **Parse**: XML is parsed and converted to internal message format
3. **Route**: Events are routed based on event type and domain
4. **Execute**: Business logic rules are evaluated using CEL
5. **Enrich**: External data is fetched via API calls (optional)
6. **Transform**: Data is transformed according to rules
7. **Validate**: Output is validated against schemas
8. **Output**: Processed events are sent to Kafka output topics
9. **Error Handling**: Errors are routed to error topics for retry/analysis

---

## Module Overview

### Core Modules

#### `appcontainer/` - Application Container Framework
- **interfaces/**: Application container interfaces
- **implementation/**: Concrete implementations
- Dependency injection and lifecycle management

#### `cel/` - Common Expression Language
- **celcore/**: Core CEL evaluation engine
- **celimpl/**: Custom CEL function implementations
- Business rule evaluation

#### `common/` - Shared Utilities
- Utility classes used across all modules
- Data structures, helpers, validators
- CSV processing utilities

#### `config/` - Configuration Management
- Application configuration loading
- Property management
- Environment-specific configs

#### `dag/` - Directed Acyclic Graph
- DAG construction and traversal
- Dependency resolution
- Execution ordering

### Execution Modules

#### `execution/all_execution/` - Complete Execution Pipeline
Aggregates all execution stages into a single pipeline

#### `execution/bizlogic/` - Business Logic Execution
Executes CEL-based business rules on events

#### `execution/enrichment/` - Data Enrichment
Enriches events with external data via API calls

#### `execution/execution/` - Core Execution Framework
Base execution interfaces and abstractions

#### `execution/messages/` - Message Handling
Envelope management, correlation, message types

#### `execution/parameters/` - Parameter Processing
Parameter extraction and processing

#### `execution/transformation/` - Data Transformation
Transforms data structures according to rules

#### `execution/validation/` - Data Validation
Validates data against schemas and rules

### Integration Modules

#### `flink/` - Apache Flink Integration
- **interfaces/**: Flink integration interfaces
- **flinkadapter/**: Adapters for Flink streaming

#### `kafka/` - Apache Kafka Integration
- **kafka/**: Kafka producer/consumer implementations
- **kafkaconfig/**: Kafka configuration

#### `metrics/` - Observability
- **envelope/**: Envelope-level metrics
- **flink/**: Flink-specific metrics
- **opentelemetry/**: OpenTelemetry integration

### Worker Applications

#### `worker/flink_worker/` - Main Flink Job
The primary streaming application for production use

**Main Class:** `com.hcltech.rmg.performance.FlinkWorker`

#### `worker/ai_worker/` - Job Orchestration API
REST API for managing Flink jobs

**Main Class:** `com.hcltech.rmg.worker.AiWorker`

#### `worker/sample_api/` - Mock External Service
Reactive API for testing enrichment flows

**Main Class:** `com.hcltech.rmg.testapi.TestApiApplication`

#### `worker/csv_api/` - CSV Processing API
REST API for CSV operations

**Main Class:** `com.hcltech.rmg.csv.CsvApiApp`

#### `worker/producer/` - Kafka Producer
Standalone producer for sending test events

#### `worker/shared_worker/` - Shared Worker Utilities
Common code shared across all workers

#### `worker/performance/` - Performance Testing
Performance harness for benchmarking

### Supporting Modules

#### `cepstate/` - Complex Event Processing State
State management for CEP scenarios

#### `optics/` - Optics Library
Functional lenses and optics for data access

#### `xml/` - XML Processing
- **xml/**: XML parsing and processing
- **woodstox/**: Woodstox-based XML handling

#### `coverage-report/` - Test Coverage
Aggregated JaCoCo test coverage reports

---

## Development Notes

### Technology Stack

- **Language:** Java 21
- **Build Tool:** Apache Maven 3.8+
- **Streaming:** Apache Flink 2.0.0
- **Messaging:** Apache Kafka 3.7.1
- **Web Framework:** Spring Boot 3.3.4
- **Business Rules:** CEL (Common Expression Language) 0.11.0
- **Metrics:** Prometheus, OpenTelemetry
- **Testing:** JUnit 5, Mockito
- **Code Coverage:** JaCoCo

### Project Structure

```
kafkapoc/
├── pom.xml                      # Parent POM
├── compose.yaml                 # Docker Compose for local services
├── prometheus.yml               # Prometheus configuration
├── Business_logic/              # CEL business rules (YAML)
├── appcontainer/                # Application container framework
├── cel/                         # CEL evaluation engine
├── cepstate/                    # CEP state management
├── common/                      # Shared utilities
├── config/                      # Configuration management
├── dag/                         # DAG processing
├── execution/                   # Execution pipeline modules
├── flink/                       # Flink integration
├── kafka/                       # Kafka integration
├── metrics/                     # Metrics and observability
├── optics/                      # Optics library
├── worker/                      # Worker applications
├── xml/                         # XML processing
└── coverage-report/             # Aggregated test coverage
```

### Key Configuration Files

- `pom.xml`: Maven project configuration and dependencies
- `compose.yaml`: Local Docker services (Kafka, Prometheus, RedPanda)
- `prometheus.yml`: Metrics scraping configuration
- `Business_logic/*.yaml`: CEL business logic rules
- `*/src/main/resources/logback.xml`: Logging configuration per module
- `*/src/main/resources/application.properties`: Spring Boot configs

### Common Development Tasks

**Clean Everything:**
```bash
mvn clean
```

**Rebuild from Scratch:**
```bash
mvn clean install
```

**Run Linting on Python Scripts:**
```bash
./fix_pylint.sh -f testdata-utils/ -a
```

**Stop All Docker Services:**
```bash
docker compose down
```

**Remove All Docker Data:**
```bash
docker compose down -v
```

**View Real-time Docker Logs:**
```bash
docker compose logs -f
```

---

## Troubleshooting

### Kafka Not Starting

**Symptom:** Kafka container fails to start or exits immediately

**Solutions:**
1. Check logs: `docker compose logs kafka`
2. Remove volumes: `docker compose down -v`
3. Restart: `docker compose up -d`

### Port Already in Use

**Symptom:** Error: "bind: address already in use"

**Solutions:**
1. Check what's using the port:
   ```bash
   # macOS/Linux
   lsof -i :9092
   lsof -i :8081
   lsof -i :9090
   ```
2. Stop the conflicting service or change ports in `compose.yaml`

### Flink Worker Won't Start

**Symptom:** Flink Worker crashes on startup

**Solutions:**
1. Ensure Kafka is running: `docker compose ps`
2. Check Java version: `java -version` (must be 21+)
3. Verify build: `cd worker/flink_worker && mvn clean package`
4. Check logs for exceptions

### Maven Build Fails

**Symptom:** `mvn install` fails with compilation errors

**Solutions:**
1. Check Java version: `java -version`
2. Clean build: `mvn clean`
3. Update Maven: `mvn -version` (need 3.8+)
4. Check for missing dependencies: `mvn dependency:tree`

### Tests Fail

**Symptom:** `mvn test` reports failures

**Solutions:**
1. Run specific test: `mvn test -Dtest=ClassName`
2. Skip tests temporarily: `mvn install -DskipTests`
3. Check test logs in `target/surefire-reports/`

### No Metrics in Prometheus

**Symptom:** Prometheus shows no data for Flink Worker

**Solutions:**
1. Verify Flink Worker is running
2. Check metrics endpoint: `curl http://localhost:9400/metrics`
3. Verify Prometheus config in `prometheus.yml`
4. Check Prometheus targets: http://localhost:9090/targets

---

## Getting Help

### Documentation

Each module has its own README with specific details:
- `worker/flink_worker/README.md`
- `worker/ai_worker/README.md`
- `cel/celcore/README.md`
- `execution/*/README.md`
- etc.

### Module Dependencies

To see the full dependency tree:
```bash
mvn dependency:tree
```

### Code Coverage

To view which code is covered by tests:
```bash
mvn clean verify
open coverage-report/target/site/jacoco-aggregate/index.html
```

---

## Quick Start Summary

**1. Install Prerequisites:**
- Java 21
- Maven 3.8+
- Docker & Docker Compose

**2. Build Project:**
```bash
mvn clean install
```

**3. Start Services:**
```bash
docker compose up -d
```

**4. Run Flink Worker:**
```bash
cd worker/flink_worker
mvn exec:java -Dexec.mainClass="com.hcltech.rmg.performance.FlinkWorker"
```

**5. Monitor:**
- Kafka: http://localhost:8081
- Prometheus: http://localhost:9090
- Logs: Console output

That's it! You now have a running Kafka + Flink streaming pipeline.
