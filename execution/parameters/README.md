# Parameters Module

## Overview
This module manages parameter extraction, configuration, and handling for the RMG execution framework.

## Components

### Core Classes

#### ParameterExtractor
- Base interface for extracting parameters from various sources

#### MapStringObjectParameterExtractor
- Extracts parameters from Map<String, Object> structures
- Tested in `MapStringObjectParameterExtractorTest.java`

#### ParameterConfig
- Configuration for parameter definitions and mappings

#### OneParameterConfig
- Configuration for individual parameters

#### Parameters
- Container for parameter values and metadata
- Tested in `ParametersTest.java`

#### ParamPermutations
- Handles parameter permutations for multi-value scenarios
- Tested in `ParamPermutationsTest.java`

### Exception Handling

#### CannotFindParameterException
- Custom exception thrown when a required parameter cannot be found

## Testing
Comprehensive unit tests are available in `src/test/java/com/hcltech/rmg/parameters/`

## Dependencies
Refer to `pom.xml` for Maven dependencies required by this module.

## Integration
This module is fundamental to the RMG framework, providing parameter extraction and management capabilities used across all execution modules.

