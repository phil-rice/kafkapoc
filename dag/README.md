# DAG (Directed Acyclic Graph)

## Overview

The DAG module provides a framework for building, validating, and executing directed acyclic graphs of requirements. It enables topological sorting, dependency resolution, and producer-consumer relationship management.

## Purpose

This module implements graph-based data structures and algorithms for:
- **Dependency Management**: Track relationships between requirements
- **Topological Sorting**: Determine optimal execution order
- **Cycle Detection**: Validate graph acyclicity
- **Path Finding**: Discover paths between nodes
- **Producer Validation**: Ensure data dependencies are satisfied

## Key Components

### Core Classes

- **`RequirementGraph`**: Main graph data structure for managing nodes and edges
- **`RequirementGraphBuilder`**: Builder pattern for constructing requirement graphs
- **`Edge`**: Represents directed connections between nodes
- **`Topo`**: Topological sorting implementation
- **`TopologicalSearchResult`**: Result container for topological operations

### Node Type Constraints

- **`NodeTC`**: Interface for node type constraints
- **`PathTC`**: Path-based type constraints
- **`ListPathTC`**: List of path constraints
- **`StringPathTC`**: String-based path constraints

### Validation

- **`ProducerValidation`**: Validates that all required data producers exist

## Usage

### Building a Requirement Graph

```java
RequirementGraphBuilder builder = new RequirementGraphBuilder();
RequirementGraph graph = builder
    .addNode("nodeA")
    .addNode("nodeB")
    .addEdge("nodeA", "nodeB")
    .build();
```

### Topological Sorting

```java
TopologicalSearchResult result = Topo.topologicalSort(graph);
if (result.hasCycle()) {
    // Handle cycle detection
} else {
    List<String> executionOrder = result.getOrder();
}
```

### Producer Validation

```java
ProducerValidation validation = new ProducerValidation(graph);
boolean isValid = validation.validate();
```

## Dependencies

- **common**: Shared utilities and models
- **JUnit Jupiter**: Unit testing framework
- **Mockito**: Mocking framework for tests

## Testing

The module includes comprehensive tests:
- `ProducerValidationTest`: Tests for producer validation logic
- `RequirementGraphBuilderTest`: Builder pattern tests
- `RequirementGraphTopoSortTest`: Topological sorting tests
- `PathTCContractTest`: Type constraint contract tests
- `AbstractNodeTCContractTest`: Abstract node constraint tests

Run tests with:

```bash
mvn test
```

## Build

This module produces both a main JAR and a test JAR:

```bash
mvn package
```

Outputs:
- `dag-1.0.0-SNAPSHOT.jar`: Main artifact
- `dag-1.0.0-SNAPSHOT-tests.jar`: Test artifact (for reuse in other modules)

## Integration

The DAG module is used by execution modules to:
1. Model data flow dependencies
2. Determine execution order for transformations
3. Validate that all required data sources are available
4. Detect circular dependencies that would cause deadlocks

