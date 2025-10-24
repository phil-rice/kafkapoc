package com.hcltech.rmg.common.csv;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class CsvLookupTest {

    @Test
    void lookupToMapReturnsExpectedValuesOrNull() {
        Map<String, List<String>> rawMap = Map.of(
                "a1.b1", List.of("1","u"),
                "a1.b2", List.of("2","v")
        );
        CsvLookup lookup = new CsvLookup(rawMap, List.of("a","b"), List.of("X","Y"), ".");

        assertEquals(Map.of("X","1","Y","u"), lookup.lookupToMap(List.of("a1","b1")));
        assertEquals(Map.of("X","2","Y","v"), lookup.lookupToMap(List.of("a1","b2")));
        assertNull(lookup.lookupToMap(List.of("missing","key")));
    }

    @Test
    void lookupToMapHandlesNullsAndShortLists() {
        Map<String, List<String>> rawMap = new HashMap<>();
        rawMap.put("a1.b1", Arrays.asList("v1", null)); // List with null element allowed
        rawMap.put("a2.b2", Arrays.asList("v2"));       // shorter than outputCols

        CsvLookup lookup = new CsvLookup(rawMap, List.of("a","b"), List.of("X","Y"), ".");

        Map<String,Object> expected1 = new HashMap<>();
        expected1.put("X", "v1");
        expected1.put("Y", null);
        assertEquals(expected1, lookup.lookupToMap(List.of("a1","b1")));

        Map<String,Object> expected2 = new HashMap<>();
        expected2.put("X", "v2");
        expected2.put("Y", null);
        assertEquals(expected2, lookup.lookupToMap(List.of("a2","b2")));
    }

    @Test
    void keyBuildingHandlesNulls() {
        Map<String, List<String>> rawMap = Map.of(
                ".b1", List.of("1"),
                "a2.", List.of("2")
        );
        CsvLookup lookup = new CsvLookup(rawMap, List.of("a","b"), List.of("X"), ".");

        // keyValues may contain nulls -> use Arrays.asList
        assertEquals(Map.of("X","1"), lookup.lookupToMap(Arrays.asList(null,"b1")));
        assertEquals(Map.of("X","2"), lookup.lookupToMap(Arrays.asList("a2",null)));
    }
}
