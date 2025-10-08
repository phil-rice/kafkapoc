// src/test/java/com/hcltech/rmg/appcontainer/interfaces/IEventTypeExtractorTest.java
package com.hcltech.rmg.messages;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class IEventTypeExtractorTest {

    @Test
    void extractsEventTypeFromNestedPath() {
        IEventTypeExtractor ext = IEventTypeExtractor.fromPathForMapStringObject(List.of("meta", "event", "type"));

        Map<String, Object> msg = Map.of(
                "meta", Map.of(
                        "event", Map.of(
                                "type", "OrderCreated"
                        )
                )
        );

        assertEquals("OrderCreated", ext.extractEventType(msg));
    }

    @Test
    void returnsNullWhenKeyMissing() {
        IEventTypeExtractor ext = IEventTypeExtractor.fromPathForMapStringObject(List.of("meta", "event", "type"));

        Map<String, Object> msg = Map.of(
                "meta", Map.of(
                        "event", Map.of(
                                // missing "type"
                                "id", "123"
                        )
                )
        );

        assertNull(ext.extractEventType(msg));
    }

    @Test
    void returnsNullWhenIntermediateIsNotAMap() {
        IEventTypeExtractor ext = IEventTypeExtractor.fromPathForMapStringObject(List.of("meta", "event", "type"));

        Map<String, Object> msg = Map.of(
                "meta", Map.of(
                        "event", 7 // not a map
                )
        );

        assertNull(ext.extractEventType(msg));
    }

    @Test
    void returnsNullWhenLeafIsNotAString() {
        IEventTypeExtractor ext = IEventTypeExtractor.fromPathForMapStringObject(List.of("meta", "event", "type"));

        Map<String, Object> msg = Map.of(
                "meta", Map.of(
                        "event", Map.of(
                                "type", 42 // not a String
                        )
                )
        );

        assertNull(ext.extractEventType(msg));
    }

    @Test
    void emptyPathReturnsNull() {
        IEventTypeExtractor ext = IEventTypeExtractor.fromPathForMapStringObject(List.of());

        Map<String, Object> msg = Map.of("anything", "goes");
        assertNull(ext.extractEventType(msg));
    }

    @Test
    void pathIsCopied_mutatingOriginalListDoesNotAffectExtractor() {
        List<String> original = new ArrayList<>(List.of("meta", "event", "type"));
        IEventTypeExtractor ext = IEventTypeExtractor.fromPathForMapStringObject(original);

        // mutate the original after creating the extractor
        original.set(0, "WRONG");
        original.add("extra");

        Map<String, Object> msg = Map.of(
                "meta", Map.of(
                        "event", Map.of(
                                "type", "OrderCreated"
                        )
                )
        );

        assertEquals("OrderCreated", ext.extractEventType(msg));
    }
}
