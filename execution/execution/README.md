# Execution Module

## Overview
This module provides the core execution framework based on aspects, enabling dynamic execution of various processing steps.

## Components

### AspectExecutor
- **Purpose**: Base executor for aspect-based execution
- **Location**: `src/main/java/com/hcltech/rmg/execution/aspects/AspectExecutor.java`
- **Usage**: Executes specific aspects of processing logic

### AspectExecutorRepository
- **Purpose**: Repository pattern for managing and retrieving aspect executors
- **Location**: `src/main/java/com/hcltech/rmg/execution/aspects/AspectExecutorRepository.java`
- **Usage**: Central registry for all available aspect executors
- **Tested in**: `AspectExecutorRepositoryTest.java`

## Architecture
The aspect-based execution model allows for:
- Modular processing steps
- Dynamic executor selection
- Extensible execution patterns
- Clear separation of concerns

## Testing
Unit tests are available in `src/test/java/com/hcltech/rmg/execution/aspects/`

## Dependencies
Refer to `pom.xml` for Maven dependencies required by this module.

## Integration
This module serves as the foundation for other execution modules (validation, enrichment, bizlogic) by providing the aspect-based execution framework.

