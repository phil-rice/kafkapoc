# Enrichment Module

## Overview
This module provides various data enrichment capabilities, allowing messages to be enhanced with additional data from multiple sources.

## Components

### Enrichment Executors

#### ApiEnrichmentExecutor
- **Purpose**: Enriches data by calling external APIs
- **Tested in**: `ApiEnrichmentExecutorTest.java`

#### CsvEnrichmentExecutor
- **Purpose**: Enriches data using CSV lookup files
- **Tested in**: `CsvEnrichmentExecutorTest.java`

#### CsvFromAzureEnrichmentExecutor
- **Purpose**: Enriches data using CSV files stored in Azure
- **Tested in**: `CsvFromAzureEnrichmentExecutorTest.java`

#### MapLookupEnrichmentExecutor
- **Purpose**: Enriches data using in-memory map lookups

#### FixedEnrichmentExecutor
- **Purpose**: Adds fixed/static values to messages

### Interfaces and Base Classes

#### EnrichmentExecutor
- Base interface for all enrichment executors

#### IEnrichmentAspectExecutor
- Interface for enrichment aspect execution

#### EnrichmentAspectExecutor
- Implementation of enrichment aspect execution

### Helper Classes

#### EnricherHelper
- Utility class providing common enrichment operations

## Key Features
- Multiple enrichment sources (API, CSV, Azure, Map, Fixed)
- Aspect-based execution model
- Extensible enrichment framework
- Cloud storage integration (Azure)

## Testing
Unit tests are available in `src/test/java/com/hcltech/rmg/enrichment/`

## Dependencies
Refer to `pom.xml` for Maven dependencies required by this module.

## Integration
This module integrates with the execution framework to provide enrichment capabilities within message processing pipelines.

