# Messages Module

## Overview
This module handles message enveloping, correlation, and type management for the RMG messaging framework.

## Components

### Envelope Classes

#### Envelope
- Base envelope class for wrapping messages
- Tested in `EnvelopeTest.java`

#### ValueEnvelope
- Envelope containing value data

#### ErrorEnvelope
- Envelope for error messages

#### RetryEnvelope
- Envelope for retry-able messages

#### AiFailureEnvelope
- Envelope for AI processing failures
- Created via `AiFailureEnvelopeFactory`

### Message Handling

#### RawMessage
- Represents raw incoming messages

#### MsgTypeClass
- Defines message type classifications

#### MapStringObjectAndListStringMsgTypeClass
- Message type class for Map and List data structures

#### MsgForAiFailure
- Message structure for AI failure scenarios

### Header and Correlation

#### EnvelopeHeader
- Header information for envelopes

#### EnvelopeCorrelator
- Correlates messages across system boundaries

#### HasSeqsForEnvelope
- Manages sequence information for envelopes
- Tested in `HasSeqsForEnvelopeTest.java`

### Adapters and Extractors

#### EnvelopeFailureAdapter
- Adapts failure scenarios to envelope format
- Tested in `EnvelopeFailureAdapterTest.java`

#### IDomainTypeExtractor
- Extracts domain type from messages
- Tested in `IDomainTypeExtractorTest.java`

#### IEventTypeExtractor
- Extracts event type from messages
- Tested in `IEventTypeExtractorTest.java`

## Testing
Comprehensive unit tests are available in `src/test/java/com/hcltech/rmg/messages/`

## Dependencies
Refer to `pom.xml` for Maven dependencies required by this module.

## Integration
This module is core to the RMG messaging infrastructure, providing standardized message handling and envelope management across all pipeline stages.

