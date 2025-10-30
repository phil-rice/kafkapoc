package com.hcltech.rmg.common.resources;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ResourceStoreBuilderTest {

    @Test
    void buildsMapFromLogicalNames() {
        // compiler: ignore InputStream, just echo the resource name with "_out"
        ResourceCompiler<String> stringCompiler = (name) -> name + "_out";

        Map<String, String> result = ResourceStoreBuilder
                .with(stringCompiler)
                .names(List.of("a.txt", "b.txt"))
                .parallelism(1)
                .build();

        // verify keys
        assertTrue(result.containsKey("a.txt"));
        assertTrue(result.containsKey("b.txt"));

        // verify values follow our convention
        assertEquals("a.txt_out", result.get("a.txt"));
        assertEquals("b.txt_out", result.get("b.txt"));
    }

    @Test
    void failsForMissingResource() {
        // compiler that should never be called in this test
        ResourceCompiler<String> stringCompiler = (name) -> {
            if (name.equals("missing.txt")) {
                throw new RuntimeException("Resource not found: " + name);
            }
            return name + "_out";
        };

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                ResourceStoreBuilder
                        .with(stringCompiler)
                        .names(List.of("missing.txt"))
                        .parallelism(1)
                        .build()
        );

        assertTrue(ex.getMessage().contains("missing.txt"),
                "Exception message should include the missing resource path");
    }
}
