# CEL Core Module

## Overview
This module provides the core framework for CEL (Common Expression Language) execution, including rule compilation, caching, and execution infrastructure.

## Components

### Core Execution

#### CelExecutor
- **Purpose**: Main executor for compiled CEL rules
- **Location**: `src/main/java/com/hcltech/rmg/celcore/CelExecutor.java`
- **Usage**: Executes compiled CEL expressions against input and context data

#### CompiledCelRule
- **Purpose**: Represents a compiled CEL rule ready for execution
- **Location**: `src/main/java/com/hcltech/rmg/celcore/CompiledCelRule.java`
- **Usage**: Encapsulates compiled CEL programs for efficient execution

#### CompiledCelRuleWithDetails
- **Purpose**: Extended compiled rule with additional metadata
- **Location**: `src/main/java/com/hcltech/rmg/celcore/CompiledCelRuleWithDetails.java`
- **Usage**: Provides compiled rule with execution details and diagnostics

### Rule Building

#### CelRuleBuilder
- **Purpose**: Interface for building CEL rules
- **Location**: `src/main/java/com/hcltech/rmg/celcore/CelRuleBuilder.java`
- **Usage**: Provides fluent API for constructing CEL rules

#### CelRuleBuilderFactory
- **Purpose**: Factory for creating rule builder instances
- **Location**: `src/main/java/com/hcltech/rmg/celcore/CelRuleBuilderFactory.java`
- **Usage**: Centralizes rule builder creation

#### CelVarType
- **Purpose**: Defines variable types for CEL expressions
- **Location**: `src/main/java/com/hcltech/rmg/celcore/CelVarType.java`
- **Usage**: Type system for CEL variables

### Caching Framework

#### CelRuleCache
- **Purpose**: Interface for CEL rule caching
- **Location**: `src/main/java/com/hcltech/rmg/celcore/cache/CelRuleCache.java`
- **Usage**: Defines caching contract for compiled rules

#### InMemoryCelRuleCache
- **Purpose**: In-memory implementation of rule cache
- **Location**: `src/main/java/com/hcltech/rmg/celcore/cache/InMemoryCelRuleCache.java`
- **Usage**: Provides fast in-memory caching of compiled rules

#### PopulateCelRuleCache
- **Purpose**: Utility for populating rule caches
- **Location**: `src/main/java/com/hcltech/rmg/celcore/cache/PopulateCelRuleCache.java`
- **Usage**: Loads and prepopulates caches with CEL rules

#### CelRuleNotFoundException
- **Purpose**: Exception thrown when a cached rule is not found
- **Location**: `src/main/java/com/hcltech/rmg/celcore/cache/CelRuleNotFoundException.java`
- **Usage**: Indicates missing rules in cache lookups

## Key Features
- Compile-once, execute-many pattern for CEL rules
- Efficient in-memory caching of compiled rules
- Type-safe variable handling
- Factory pattern for rule builder creation
- Extensible caching infrastructure

## Performance
The caching framework significantly improves performance by:
- Avoiding repeated compilation of the same rules
- Storing compiled rules in memory for fast access
- Supporting preloading of frequently-used rules

## Testing
Contract tests for caching behavior are available in `src/test/java/com/hcltech/rmg/celcore/cache/`

## Dependencies
Refer to `pom.xml` for Maven dependencies required by this module.

## Integration
This module provides the foundation for CEL-based features used in:
- **validation** module (for rule-based validation)
- **bizlogic** module (for business logic execution)
- Other modules requiring expression evaluation

## Architecture
This module defines the **core abstractions** for CEL processing, while concrete implementations are provided in the `cel/celimpl` module, following the **Hexagonal Architecture** pattern.

