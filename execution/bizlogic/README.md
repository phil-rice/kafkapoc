# Business Logic Module

## Overview
This module provides business logic execution capabilities using CEL (Common Expression Language) for implementing custom business rules and logic.

## Components

### BizLogicExecutor
- **Purpose**: Base interface/class for business logic execution
- **Location**: `src/main/java/com/hcltech/rmg/execution/bizlogic/BizLogicExecutor.java`
- **Usage**: Executes business logic steps in the processing pipeline

### CelInlineLogicExecutor
- **Purpose**: Executes inline CEL expressions for business logic
- **Location**: `src/main/java/com/hcltech/rmg/execution/bizlogic/CelInlineLogicExecutor.java`
- **Usage**: Evaluates CEL-based business rules inline within the pipeline
- **Tested in**: `AbstractCelInlineLogicExecutorContractTest.java`

### Exception Handling

#### InvalidCelException
- Custom exception thrown when CEL expressions are invalid or cannot be evaluated

## Key Features
- CEL-based business logic execution
- Inline expression evaluation
- Integration with execution pipeline
- Custom exception handling for CEL errors

## Testing
Unit tests are available in `src/test/java/com/hcltech/rmg/execution/bizlogic/`

## Dependencies
Refer to `pom.xml` for Maven dependencies required by this module.

## Integration
This module works within the RMG execution framework to provide flexible business logic execution using CEL expressions.

