package com.hcltech.rmg.messages;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class IDomainTypeExtractorTest {

    @Test
    void extractsDomainTypeFromNestedPath() {
        IDomainTypeExtractor ext = IDomainTypeExtractor.fromPath(List.of("meta", "domain", "type"));

        Map<String, Object> msg = Map.of(
                "meta", Map.of(
                        "domain", Map.of(
                                "type", "orders"
                        )
                )
        );

        assertEquals("orders", ext.extractDomainType(msg));
    }

    @Test
    void returnsNullWhenKeyMissing() {
        IDomainTypeExtractor ext = IDomainTypeExtractor.fromPath(List.of("meta", "domain", "type"));

        Map<String, Object> msg = Map.of(
                "meta", Map.of(
                        "domain", Map.of(
                                // missing "type"
                                "id", "x"
                        )
                )
        );

        assertNull(ext.extractDomainType(msg));
    }

    @Test
    void returnsNullWhenIntermediateIsNotAMap() {
        IDomainTypeExtractor ext = IDomainTypeExtractor.fromPath(List.of("meta", "domain", "type"));

        Map<String, Object> msg = Map.of(
                "meta", Map.of(
                        "domain", 123 // not a map
                )
        );

        assertNull(ext.extractDomainType(msg));
    }

    @Test
    void returnsNullWhenLeafIsNotAString() {
        IDomainTypeExtractor ext = IDomainTypeExtractor.fromPath(List.of("meta", "domain", "type"));

        Map<String, Object> msg = Map.of(
                "meta", Map.of(
                        "domain", Map.of(
                                "type", 42 // not a String
                        )
                )
        );

        assertNull(ext.extractDomainType(msg));
    }

    @Test
    void emptyPathReturnsNull() {
        IDomainTypeExtractor ext = IDomainTypeExtractor.fromPath(List.of());

        Map<String, Object> msg = Map.of("anything", "goes");
        assertNull(ext.extractDomainType(msg));
    }

    @Test
    void pathIsCopied_mutatingOriginalListDoesNotAffectExtractor() {
        List<String> original = new ArrayList<>(List.of("meta", "domain", "type"));
        IDomainTypeExtractor ext = IDomainTypeExtractor.fromPath(original);

        // mutate the original after creating the extractor
        original.set(0, "WRONG");
        original.add("extra");

        Map<String, Object> msg = Map.of(
                "meta", Map.of(
                        "domain", Map.of(
                                "type", "orders"
                        )
                )
        );

        assertEquals("orders", ext.extractDomainType(msg));
    }

    @Test
    void fixedAlwaysReturnsConstant() {
        IDomainTypeExtractor ext = IDomainTypeExtractor.fixed("static-domain");
        assertEquals("static-domain", ext.extractDomainType(Map.of()));
        assertEquals("static-domain", ext.extractDomainType(Map.of("anything", "else")));
        assertEquals("static-domain", ext.extractDomainType(null));
    }
}
