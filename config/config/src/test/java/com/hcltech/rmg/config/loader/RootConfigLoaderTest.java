package com.hcltech.rmg.config.loader;

import com.hcltech.rmg.config.config.RootConfig;
import com.hcltech.rmg.parameters.ParameterConfig;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class RootConfigLoaderTest {

    private InputStream resourceStream(String name) throws Exception {
        String path = "RootConfigLoaderTest/" + name;
        InputStream in = getClass().getClassLoader().getResourceAsStream(path);
        assertNotNull(in, "Test resource not found: " + path);
        return in;
    }

    private String resourceString(String name) throws Exception {
        try (InputStream in = resourceStream(name)) {
            return new String(in.readAllBytes());
        }
    }

    private Path resourcePath(String name) throws Exception {
        String path = "RootConfigLoaderTest/" + name;
        URL url = getClass().getClassLoader().getResource(path);
        assertNotNull(url, "Test resource not found: " + path);
        return Path.of(url.toURI());
    }

    // --- Happy paths (all three overloads) ---

    @Test
    void fromJson_InputStream_success() throws Exception {
        try (InputStream in = resourceStream("valid-root.json")) {
            RootConfig rc = RootConfigLoader.fromJson(in);
            assertNotNull(rc);

            ParameterConfig pc = rc.parameterConfig();
            assertNotNull(pc);
            assertEquals(2, pc.parameters().size());
            assertEquals("schemas/config.xsd", rc.xmlSchemaPath());
        }
    }

    @Test
    void fromJson_Path_success() throws Exception {
        Path p = resourcePath("valid-root.json");
        RootConfig rc = RootConfigLoader.fromJson(p);
        assertNotNull(rc);
        assertNotNull(rc.parameterConfig());
        assertEquals("schemas/config.xsd", rc.xmlSchemaPath());
    }

    @Test
    void fromJson_String_success() throws Exception {
        String json = resourceString("valid-root.json");
        RootConfig rc = RootConfigLoader.fromJson(json);
        assertNotNull(rc);
        assertNotNull(rc.parameterConfig());
        assertEquals("schemas/config.xsd", rc.xmlSchemaPath());
    }

    // --- Lenient parsing knobs from BaseConfigLoader ---

    @Test
    void allowsCommentsAndTrailingCommas() throws Exception {
        String json = resourceString("valid-root-with-comments-and-trailing-commas.json");
        RootConfig rc = RootConfigLoader.fromJson(json);
        assertNotNull(rc);
        assertNotNull(rc.parameterConfig());
        assertEquals(1, rc.parameterConfig().parameters().size());
        assertEquals("schemas/with-comments.xsd", rc.xmlSchemaPath());
    }

    @Test
    void ignoresUnknownFields() throws Exception {
        Path p = resourcePath("with-unknown-field.json");
        RootConfig rc = RootConfigLoader.fromJson(p);
        assertNotNull(rc);
        assertNotNull(rc.parameterConfig());
        assertEquals("schemas/ok.xsd", rc.xmlSchemaPath());
    }

    // --- Edge cases we actually support today ---

    @Test
    void allowsNullXmlSchemaPath() throws Exception {
        Path p = resourcePath("valid-root-null-xsd.json");
        RootConfig rc = RootConfigLoader.fromJson(p);
        assertNotNull(rc);
        assertNotNull(rc.parameterConfig());
        assertNull(rc.xmlSchemaPath()); // null is allowed; loader just deserializes
    }

    @Test
    void missingParameterConfig_deserializesAsNull() throws Exception {
        // No validation in the loader; just confirm deserialization behavior.
        String json = resourceString("missing-parameter-config.json");
        RootConfig rc = RootConfigLoader.fromJson(json);
        assertNotNull(rc);
        assertNull(rc.parameterConfig()); // loader doesn't enforce presence
        assertEquals("schemas/config.xsd", rc.xmlSchemaPath());
    }

    @Test
    void malformedJson_throws() {
        assertThrows(Exception.class, () -> {
            String json = "{ not valid JSON ";
            RootConfigLoader.fromJson(json);
        });
    }

    @Test
    void pathVariantAlsoWorksWithTempCopy() throws Exception {
        String json = resourceString("valid-root.json");
        Path tmp = Files.createTempFile("rootcfg-", ".json");
        Files.writeString(tmp, json);
        try {
            RootConfig rc = RootConfigLoader.fromJson(tmp);
            assertNotNull(rc);
            assertNotNull(rc.parameterConfig());
            assertEquals("schemas/config.xsd", rc.xmlSchemaPath());
        } finally {
            Files.deleteIfExists(tmp);
        }
    }
}
