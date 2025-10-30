# Optics

## Overview

The Optics module provides an event interpretation and querying framework for processing structured data events. It enables dynamic extraction of values from complex event structures using path-based expressions.

## Purpose

This module implements:
- **Event Interpretation**: Query and extract data from event structures
- **Path-Based Queries**: Navigate complex nested data using XPath-like expressions
- **Event Interface**: Define contract for optics-compatible events
- **JSON Integration**: Work seamlessly with JSON-based event data

## Key Components

### Core Interfaces and Classes

- **`IOpticsEvent`**: Interface defining the contract for events that can be queried
- **`Interpreter`**: Core component for evaluating path expressions against events

### Query Language

The module uses **JXPath** (Apache Commons JXPath) for powerful path-based queries:
- Navigate nested JSON structures
- Extract values using XPath-like syntax
- Support for complex data types
- Null-safe navigation

## Usage

### Implementing an Optics Event

```java
public class MyEvent implements IOpticsEvent {
    private JsonNode data;
    
    @Override
    public Object getValue(String path) {
        // Use Interpreter to extract value at path
        return Interpreter.evaluate(data, path);
    }
}
```

### Querying Event Data

```java
IOpticsEvent event = new MyEvent(jsonData);

// Extract simple value
String userId = (String) event.getValue("/user/id");

// Extract nested value
Integer age = (Integer) event.getValue("/user/profile/age");

// Extract array element
String firstTag = (String) event.getValue("/tags[0]");
```

### Using the Interpreter

```java
JsonNode data = objectMapper.readTree(jsonString);
Object result = Interpreter.evaluate(data, "$.user.email");
```

## Dependencies

- **common**: Shared utilities and Jackson configuration
- **commons-jxpath**: Apache Commons JXPath for path-based queries
- **jackson-databind**: JSON processing
- **jackson-datatype-jsr310**: Java 8 date/time support
- **JUnit Jupiter**: Unit testing framework

## Path Expression Syntax

The module supports JXPath expressions:

| Expression | Description |
|------------|-------------|
| `/user/name` | Simple nested path |
| `/users[0]` | Array element access |
| `/users[name='John']` | Conditional selection |
| `//name` | Recursive search |
| `/user/@id` | Attribute access |

## Testing

The module includes comprehensive tests:
- `IOpticsEventCodecTest`: Event serialization/deserialization tests
- `JXPathInterpreterTest`: Interpreter functionality tests
- `OpticsEventTest`: Event interface contract tests

Run tests with:

```bash
mvn test
```

## Build

Build the module:

```bash
mvn package
```

Output: `optics-1.0.0-SNAPSHOT.jar`

## Integration

The Optics module is used by:
- **Transformation modules**: Extract and transform event data
- **Validation modules**: Validate event structure and content
- **Enrichment modules**: Add derived fields based on existing data
- **Business logic modules**: Implement complex rules based on event data

## Example Use Cases

1. **Field Extraction**: Extract specific fields from incoming Kafka messages
2. **Data Validation**: Check if required fields exist and meet criteria
3. **Dynamic Routing**: Route events based on field values
4. **Data Transformation**: Transform nested structures based on paths
5. **Conditional Logic**: Implement business rules that inspect event structure

