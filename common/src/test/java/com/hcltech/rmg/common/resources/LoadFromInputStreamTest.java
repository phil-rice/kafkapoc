package com.hcltech.rmg.common.resources;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class LoadFromInputStreamTest {

    @Test
    void loadFromClasspath_readsFileContent() {
        String result = LoadFromInputStream.loadFromClasspath(
                "iofunction/",                // prefix
                "sample.txt",                 // name
                in -> new String(in.readAllBytes(), StandardCharsets.UTF_8) // converter
        );

        assertEquals("Hello World", result.trim());
    }

    @Test
    void loadFromClasspath_failsForMissingFile() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                LoadFromInputStream.loadFromClasspath("iofunction/", "doesNotExist.txt", in -> "ignored")
        );

        assertTrue(ex.getMessage().contains("iofunction/doesNotExist.txt"));
    }
}
