# Validation Module

## Overview
This module provides validation execution capabilities using CEL (Common Expression Language) for validating data and business rules.

## Components

### CelValidationExecutor
- **Purpose**: Executes CEL-based validation expressions to validate data
- **Location**: `src/main/java/com/hcltech/rmg/execution/validation/cel/CelValidationExecutor.java`
- **Usage**: Used to evaluate validation rules written in CEL against input data

## Testing
Unit tests are available in `src/test/java/com/hcltech/rmg/execution/validation/cel/`

## Dependencies
Refer to `pom.xml` for Maven dependencies required by this module.

## Integration
This module is part of the RMG execution framework and is designed to be used in validation pipelines for ensuring data quality and business rule compliance.

