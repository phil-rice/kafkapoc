package com.hcltech.rmg.common.csv;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CsvLookupCollectorTest {

    @Test
    void buildsLookupMapAndPreservesOutputOrder() {
        List<String> header = List.of("a","b","X","Y","Z");
        List<String> input = List.of("a","b");
        List<String> output = List.of("Z","X"); // output order should be preserved

        CsvLookupCollector collector = new CsvLookupCollector(header, input, output, ".");
        collector.accept(List.of("a1","b1","1","u","p"));
        collector.accept(List.of("a1","b2","2","v","q"));

        CsvLookup lookup = collector.build();

        // internal map preserves output order (Z, X)
        Map<String, List<String>> map = lookup.map();
        assertEquals(List.of("p","1"), map.get("a1.b1"));
        assertEquals(List.of("q","2"), map.get("a1.b2"));

        // lookupToMap() builds correct field/value mapping
        assertEquals(Map.of("Z","p","X","1"), lookup.lookupToMap(List.of("a1","b1")));
        assertEquals(Map.of("Z","q","X","2"), lookup.lookupToMap(List.of("a1","b2")));
    }

    @Test
    void handlesNullsAndUnknownKeys() {
        List<String> header = List.of("a","b","X");
        List<String> input = List.of("a","b");
        List<String> output = List.of("X");

        CsvLookupCollector collector = new CsvLookupCollector(header, input, output, ".");
        // rows with possible nulls -> Arrays.asList
        collector.accept(Arrays.asList(null, "b1", "1"));
        collector.accept(Arrays.asList("a2", null, null));
        CsvLookup lookup = collector.build();

        // expected maps with possible nulls -> HashMap
        Map<String,Object> expected1 = new HashMap<>();
        expected1.put("X", "1");
        assertEquals(expected1, lookup.lookupToMap(Arrays.asList(null,"b1")));

        Map<String,Object> expected2 = new HashMap<>();
        expected2.put("X", null);
        assertEquals(expected2, lookup.lookupToMap(Arrays.asList("a2", null)));

        assertNull(lookup.lookupToMap(List.of("missing","key")));
    }

    @Test
    void validatesColumnsAndRejectsDuplicates() {
        List<String> header = List.of("a","b","X");

        assertThrows(IllegalArgumentException.class,
                () -> new CsvLookupCollector(header, List.of("nope"), List.of("X"), "."));
        assertThrows(IllegalArgumentException.class,
                () -> new CsvLookupCollector(header, List.of("a"), List.of("nope"), "."));
        assertThrows(IllegalArgumentException.class,
                () -> new CsvLookupCollector(List.of("a","a"), List.of("a"), List.of("a"), "."));
    }
}
