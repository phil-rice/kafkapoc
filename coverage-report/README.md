# Coverage Report

## Overview

The Coverage Report module is a Maven aggregator project that generates consolidated JaCoCo code coverage reports across all modules in the RMG POC project.

## Purpose

This module aggregates test coverage data from all project modules into a single, unified coverage report. It provides insights into:
- Overall code coverage percentage
- Coverage by module
- Uncovered code locations
- Test quality metrics

## Usage

### Generating Coverage Reports

Run the following Maven command from the project root:

```bash
mvn clean verify
```

This will:
1. Execute all tests across all modules
2. Collect coverage data from each module
3. Generate an aggregate report in `coverage-report/target/site/jacoco-aggregate/`

### Viewing Reports

After running the verification phase, open the coverage report:

```bash
open coverage-report/target/site/jacoco-aggregate/index.html
```

The report includes:
- **HTML Report**: Interactive web-based coverage visualization
- **XML Report**: Machine-readable coverage data (for CI/CD integration)
- **CSV Report**: Tabular coverage data for analysis

## Configuration

The module uses JaCoCo Maven Plugin with the following configuration:
- **Execution Phase**: `verify`
- **Goal**: `report-aggregate`
- **Inherited**: `false` (only runs at the aggregator level)

## Dependencies

This module declares dependencies on all project modules to ensure complete coverage aggregation:
- common
- dag
- appcontainer (interfaces & implementation)
- celcore & celimpl
- cepstate
- config
- execution modules (bizlogic, enrichment, parameters, transformation, validation, etc.)
- flinkadapter
- optics & performance
- kafka modules
- xml & woodstox

## Notes

- This is a `pom` packaging module (no source code)
- Coverage data is only available after running tests
- Ensure all module tests pass before generating the report
- The report aggregates coverage from all declared dependencies

