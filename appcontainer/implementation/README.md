# App Container Implementation Module

## Overview
This module provides concrete implementations of the AppContainer interfaces, including factory classes and Azure storage integration.

## Components

### AppContainerFactoryForMapStringObject
- **Purpose**: Factory implementation for creating app containers that work with Map<String, Object> data structures
- **Location**: `src/main/java/com/hcltech/rmg/appcontainer/impl/AppContainerFactoryForMapStringObject.java`
- **Usage**: Primary factory for instantiating app containers in the RMG framework
- **Tested in**: `AppContainerFactoryForMapStringObjectTest.java`

### AzureStorageTokenGenerator
- **Purpose**: Generates authentication tokens for Azure Storage access
- **Location**: `src/main/java/com/hcltech/rmg/appcontainer/impl/AzureStorageTokenGenerator.java`
- **Usage**: Handles Azure storage authentication for configuration and data access
- **Tested in**: `AzureStorageTokenGeneratorTest.java`

## Configuration

### Configuration Files
The module includes hierarchical configuration support with environment-specific settings:

#### Root Configuration
- `root-prod.json` - Production root configuration
- `root-test.json` - Test root configuration

#### Environment-Specific Configuration
Located in `src/test/resources/config/`:
- `prod/dev/uk.json` - Production development (UK)
- `prod/prod/uk.json` - Production (UK)
- `test/dev/uk.json` - Test development (UK)
- `test/prod/uk.json` - Test production (UK)

### Schema Validation
- `schemas/config.xsd` - XSD schema for configuration validation

## Key Features
- Factory pattern implementation for app container creation
- Azure cloud storage integration
- Hierarchical configuration management
- Environment-specific configuration support
- Configuration schema validation

## Testing
Comprehensive unit tests are available in `src/test/java/com/hcltech/rmg/appcontainer/impl/`

## Dependencies
Refer to `pom.xml` for Maven dependencies required by this module.

## Integration
This module implements the interfaces defined in the `appcontainer/interfaces` module and is used throughout the RMG framework for application container management and Azure storage access.

