package com.hcltech.rmg.config.loader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.hcltech.rmg.config.config.RootConfig;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.hcltech.rmg.config.loader.BaseConfigLoader.base;

public interface RootConfigLoader {
    ObjectMapper JSON = base(new ObjectMapper());
    ObjectReader ROOT_READER = JSON.readerFor(RootConfig.class);

    static RootConfig fromJson(InputStream in) throws IOException { return ROOT_READER.readValue(in); }

    static RootConfig fromJson(Path path) throws IOException {
        try (InputStream in = Files.newInputStream(path)) { return fromJson(in); }
    }

    static RootConfig fromJson(String json) throws IOException {
        try (InputStream in = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) { return fromJson(in); }
    }

    static ObjectMapper strictJson() { return JSON; }
}
