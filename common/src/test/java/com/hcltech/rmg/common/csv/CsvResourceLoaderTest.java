package com.hcltech.rmg.common.csv;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CsvResourceLoaderTest {

    private static final String BASE = "CsvResourceLoaderTest/"; // Option A: simple resource path

    @Test
    void loadsLookupAndRespectsOutputOrder() {
        CsvLookup lookup = CsvResourceLoader.load(
                BASE + "sample.csv",
                List.of("a", "b"),
                List.of("Z", "X"),
                ',', "."
        );

        Map<String, List<String>> raw = lookup.map();
        assertEquals(List.of("p", "1"), raw.get("a1.b1"));
        assertEquals(List.of("q", "2"), raw.get("a1.b2"));

        assertEquals(Map.of("Z", "p", "X", "1"), lookup.lookupToMap(List.of("a1", "b1")));
        assertEquals(Map.of("Z", "q", "X", "2"), lookup.lookupToMap(List.of("a1", "b2")));
        assertNull(lookup.lookupToMap(List.of("nope", "nope")));
    }

    @Test
    void handlesNullsAndShortRows() {
        CsvLookup lookup = CsvResourceLoader.load(
                BASE + "sample.csv",
                List.of("a", "b"),
                List.of("X", "Y"),
                ',', "."
        );

        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("X", "1");
        expected1.put("Y", "u");
        assertEquals(expected1, lookup.lookupToMap(List.of("a1", "b1")));

        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("X", "3");
        expected2.put("Y", null);
        assertEquals(expected2, lookup.lookupToMap(List.of("a2", "b3")));
    }

    @Test
    void validatesParamsWithObjectsChecks() {
        assertThrows(NullPointerException.class, () ->
                CsvResourceLoader.load(null, List.of("a"), List.of("X")));
        assertThrows(IllegalArgumentException.class, () ->
                CsvResourceLoader.load("  ", List.of("a"), List.of("X")));

        assertThrows(NullPointerException.class, () ->
                CsvResourceLoader.load(BASE + "sample.csv", null, List.of("X")));
        assertThrows(IllegalArgumentException.class, () ->
                CsvResourceLoader.load(BASE + "sample.csv", List.of(), List.of("X")));
        assertThrows(NullPointerException.class, () ->
                CsvResourceLoader.load(BASE + "sample.csv", List.of("a"), null));
        assertThrows(IllegalArgumentException.class, () ->
                CsvResourceLoader.load(BASE + "sample.csv", List.of("a"), List.of()));
        assertThrows(NullPointerException.class, () ->
                CsvResourceLoader.load(BASE + "sample.csv", List.of("a"), List.of("X"), ',', null));
    }

    @Test
    void missingResourceThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                CsvResourceLoader.load(BASE + "does_not_exist.csv", List.of("a"), List.of("X")));
    }

    @Test
    void missingColumnsThrow() {
        assertThrows(IllegalArgumentException.class, () ->
                CsvResourceLoader.load(BASE + "sample.csv", List.of("nope"), List.of("X")));
        assertThrows(IllegalArgumentException.class, () ->
                CsvResourceLoader.load(BASE + "sample.csv", List.of("a"), List.of("nope")));
    }

    @Test
    void handlesUtf8BomAndBlankLines() {
        CsvLookup lookup = CsvResourceLoader.load(
                BASE + "sample_bom.csv",
                List.of("a", "b"),
                List.of("X"),
                ',', "."
        );
        assertEquals(Map.of("X", "1"), lookup.lookupToMap(List.of("a1", "b1")));
        assertEquals(Map.of("X", "2"), lookup.lookupToMap(List.of("a1", "b2")));
    }
}
