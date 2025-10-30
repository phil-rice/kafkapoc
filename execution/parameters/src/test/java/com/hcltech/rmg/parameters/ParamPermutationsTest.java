package com.hcltech.rmg.parameters;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class ParamPermutationsTest {

    private static OneParameterConfig dim(List<String> values) {
        return new OneParameterConfig(values, values.isEmpty() ? null : values.get(0), "test");
    }

    @Test
    void permutations_noParams_returnsSingleEmptyList() {
        ParameterConfig pc = new ParameterConfig(List.of());
        var perms = ParamPermutations.permutations(pc).collect(Collectors.toList());
        assertEquals(1, perms.size(), "Expected a single empty permutation");
        assertEquals(List.of(), perms.get(0), "Expected the empty permutation []");
    }

    @Test
    void permutations_nullParams_returnsSingleEmptyList() {
        ParameterConfig pc = new ParameterConfig(null);
        var perms = ParamPermutations.permutations(pc).collect(Collectors.toList());
        assertEquals(1, perms.size());
        assertEquals(List.of(), perms.get(0));
    }

    @Test
    void permutations_singleDimension_producesAllValues() {
        ParameterConfig pc = new ParameterConfig(List.of(
                dim(List.of("dev", "qa", "prod"))
        ));

        var perms = ParamPermutations.permutations(pc).collect(Collectors.toList());

        assertEquals(3, perms.size());
        assertTrue(perms.contains(List.of("dev")));
        assertTrue(perms.contains(List.of("qa")));
        assertTrue(perms.contains(List.of("prod")));
    }

    @Test
    void permutations_twoDimensions_cartesianProduct() {
        ParameterConfig pc = new ParameterConfig(List.of(
                dim(List.of("dev", "prod")),
                dim(List.of("uk", "de"))
        ));

        var perms = ParamPermutations.permutations(pc).collect(Collectors.toList());

        // 2 x 2 = 4
        assertEquals(4, perms.size());
        assertTrue(perms.contains(List.of("dev", "uk")));
        assertTrue(perms.contains(List.of("dev", "de")));
        assertTrue(perms.contains(List.of("prod", "uk")));
        assertTrue(perms.contains(List.of("prod", "de")));
    }

    @Test
    void permutations_threeDimensions_cartesianProductAndOrder() {
        ParameterConfig pc = new ParameterConfig(List.of(
                dim(List.of("dev", "prod")),        // p0
                dim(List.of("uk")),                 // p1
                dim(List.of("blue", "green", "red"))// p2
        ));

        var perms = ParamPermutations.permutations(pc).collect(Collectors.toList());

        // 2 x 1 x 3 = 6
        assertEquals(6, perms.size());
        assertTrue(perms.contains(List.of("dev", "uk", "blue")));
        assertTrue(perms.contains(List.of("dev", "uk", "green")));
        assertTrue(perms.contains(List.of("dev", "uk", "red")));
        assertTrue(perms.contains(List.of("prod", "uk", "blue")));
        assertTrue(perms.contains(List.of("prod", "uk", "green")));
        assertTrue(perms.contains(List.of("prod", "uk", "red")));

        // verify positional order (p0, p1, p2)
        perms.forEach(tuple -> {
            assertEquals(3, tuple.size());
            // tuple.get(0) comes from first dimension, etc.
            assertTrue(List.of("dev", "prod").contains(tuple.get(0)));
            assertEquals("uk", tuple.get(1));
            assertTrue(List.of("blue", "green", "red").contains(tuple.get(2)));
        });
    }

    @Test
    void permutations_dimensionWithNoValues_resultsInEmptyStream() {
        ParameterConfig pc = new ParameterConfig(List.of(
                dim(List.of("dev", "prod")),
                dim(List.of()) // empty dimension eliminates all permutations
        ));

        var perms = ParamPermutations.permutations(pc).collect(Collectors.toList());
        assertEquals(0, perms.size(), "Empty dimension should yield no permutations");
    }

    @Test
    void permutations_largeButReasonable_sizesMultiply() {
        ParameterConfig pc = new ParameterConfig(List.of(
                dim(List.of("a", "b", "c")),   // 3
                dim(List.of("x", "y")),        // 2
                dim(List.of("1", "2", "3", "4")) // 4
        ));
        // total = 3 * 2 * 4 = 24
        var perms = ParamPermutations.permutations(pc).collect(Collectors.toList());
        assertEquals(24, perms.size());
    }
}
