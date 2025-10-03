package com.hcltech.rmg.parameters;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public interface ParamPermutations {

    /**
     * Produces a Stream of permutations.
     * Each permutation is a List<String> of chosen values in positional parameter order.
     * Example: parameters = [[dev,qa],[uk,de]] -> stream of [dev,uk], [dev,de], [qa,uk], [qa,de]
     */
    static Stream<List<String>> permutations(ParameterConfig pc) {
        List<OneParameterConfig> dims = pc.parameters();
        if (dims == null || dims.isEmpty()) {
            // single "empty" choice
            return Stream.of(List.of());
        }

        // Iterative cartesian product without recursion to keep stack shallow.
        Stream<List<String>> acc = Stream.of(List.of());
        for (OneParameterConfig dim : dims) {
            List<String> values = dim.legalValue();
            if (values == null || values.isEmpty()) {
                // Treat empty dimension as having a single empty choice (or fail if you prefer)
                return Stream.of(); // no permutations possible if a dimension has 0 legal values
            }
            acc = acc.flatMap(prefix -> values.stream().map(v -> {
                List<String> next = new ArrayList<>(prefix.size() + 1);
                next.addAll(prefix);
                next.add(v);
                return List.copyOf(next);
            }));
        }
        return acc;
    }
}
