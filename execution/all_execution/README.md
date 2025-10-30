# All Execution Module

## Overview
This module orchestrates the complete execution pipeline by integrating parsing, enrichment, and business logic steps.

## Components

### Pipeline Steps

#### ParseMessagePipelineStep
- **Purpose**: Handles message parsing in the pipeline
- **Location**: `src/main/java/com/hcltech/rmg/all_execution/ParseMessagePipelineStep.java`
- **Usage**: First step in the pipeline that parses incoming messages

#### EnrichmentPipelineStep
- **Purpose**: Handles enrichment execution in the pipeline
- **Location**: `src/main/java/com/hcltech/rmg/all_execution/EnrichmentPipelineStep.java`
- **Usage**: Enriches parsed messages with additional data

#### BizLogicPipelineStep
- **Purpose**: Handles business logic execution in the pipeline
- **Location**: `src/main/java/com/hcltech/rmg/all_execution/BizLogicPipelineStep.java`
- **Usage**: Applies business rules and logic to enriched messages

## Pipeline Flow
```
1. ParseMessagePipelineStep
   └─> Parses incoming raw messages
   
2. EnrichmentPipelineStep
   └─> Enriches messages with additional data
   
3. BizLogicPipelineStep
   └─> Applies business logic and rules
```

## Testing
Contract tests are available in `src/test/java/com/hcltech/rmg/all_execution/`:
- `CelInlineLogicExecutorContractTest.java`
- `CelValidationExecutorTest.java`

## Dependencies
This module depends on:
- **messages** module (for message handling)
- **parameters** module (for parameter extraction)
- **enrichment** module (for enrichment execution)
- **bizlogic** module (for business logic execution)
- **validation** module (for validation execution)

Refer to `pom.xml` for Maven dependencies required by this module.

## Integration
This module serves as the top-level orchestrator, bringing together all execution modules into a cohesive processing pipeline.

