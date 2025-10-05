// src/test/java/com/hcltech/rmg/config/loader/RootConfigLoaderTest.java
package com.hcltech.rmg.config.loader;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.config.config.RootConfig;
import com.hcltech.rmg.parameters.ParameterConfig;
import org.junit.jupiter.api.Test;

import java.io.InputStream;

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

    // --- Happy paths (all supported entry points) ---

    @Test
    void fromJson_InputStream_success() throws Exception {
        try (InputStream in = resourceStream("valid-root.json")) {
            ErrorsOr<RootConfig> eo = RootConfigLoader.fromJson(in);
            assertTrue(eo.isValue(), () -> "Expected value but got: " + eo.getErrors());

            RootConfig rc = eo.valueOrThrow();
            ParameterConfig pc = rc.parameterConfig();
            assertNotNull(pc);
            assertEquals(2, pc.parameters().size());
            assertEquals("schemas/config.xsd", rc.xmlSchemaPath());
        }
    }

    @Test
    void fromClasspath_success() {
        ErrorsOr<RootConfig> eo = RootConfigLoader.fromClasspath("RootConfigLoaderTest/valid-root.json");
        assertTrue(eo.isValue(), () -> "Expected value but got: " + eo.getErrors());

        RootConfig rc = eo.valueOrThrow();
        assertNotNull(rc.parameterConfig());
        assertEquals("schemas/config.xsd", rc.xmlSchemaPath());
    }

    @Test
    void fromJson_String_success() throws Exception {
        String json = resourceString("valid-root.json");
        ErrorsOr<RootConfig> eo = RootConfigLoader.fromJson(json);
        assertTrue(eo.isValue(), () -> "Expected value but got: " + eo.getErrors());

        RootConfig rc = eo.valueOrThrow();
        assertNotNull(rc.parameterConfig());
        assertEquals("schemas/config.xsd", rc.xmlSchemaPath());
    }

    // --- Lenient parsing knobs from BaseConfigLoader ---

    @Test
    void allowsCommentsAndTrailingCommas() throws Exception {
        String json = resourceString("valid-root-with-comments-and-trailing-commas.json");
        ErrorsOr<RootConfig> eo = RootConfigLoader.fromJson(json);
        assertTrue(eo.isValue(), () -> "Expected value but got: " + eo.getErrors());

        RootConfig rc = eo.valueOrThrow();
        assertNotNull(rc.parameterConfig());
        assertEquals(1, rc.parameterConfig().parameters().size());
        assertEquals("schemas/with-comments.xsd", rc.xmlSchemaPath());
    }

    @Test
    void ignoresUnknownFields() {
        ErrorsOr<RootConfig> eo = RootConfigLoader.fromClasspath("RootConfigLoaderTest/with-unknown-field.json");
        assertTrue(eo.isValue(), () -> "Expected value but got: " + eo.getErrors());

        RootConfig rc = eo.valueOrThrow();
        assertNotNull(rc.parameterConfig());
        assertEquals("schemas/ok.xsd", rc.xmlSchemaPath());
    }

    // --- Validation (now enforced by loader) ---

    @Test
    void nullXmlSchemaPath_isError() {
        String path = "RootConfigLoaderTest/valid-root-null-xsd.json";
        ErrorsOr<RootConfig> eo = RootConfigLoader.fromClasspath(path);
        assertTrue(eo.isError(), "Expected error for null xmlSchemaPath");

        var errs = eo.errorsOrThrow();
        // New behavior: classpath loads prefix errors with the resource path
        assertTrue(
                errs.get(0).startsWith("rootconfig '" + path + "': "),
                () -> "First error should be prefixed with resource path: " + errs
        );
        assertTrue(
                errs.stream().anyMatch(m -> m.toLowerCase().contains("xmlschemapath")),
                () -> "Errors should mention xmlSchemaPath: " + errs
        );
    }

    @Test
    void missingParameterConfig_isError() throws Exception {
        String json = resourceString("missing-parameter-config.json");
        ErrorsOr<RootConfig> eo = RootConfigLoader.fromJson(json);
        assertTrue(eo.isError(), "Expected error for missing parameterConfig");
        var errs = eo.errorsOrThrow();
        assertTrue(errs.stream().anyMatch(m -> m.toLowerCase().contains("parameterconfig")),
                () -> "Errors should mention parameterConfig: " + errs);
    }

    @Test
    void emptyParameters_isError() {
        String json = """
                {
                  "xmlSchemaPath":"schemas/x.xsd",
                  "parameterConfig": { "parameters": [] }
                }""";
        var eo = RootConfigLoader.fromJson(json);
        assertTrue(eo.isError());
        assertTrue(eo.errorsOrThrow().stream().anyMatch(m -> m.toLowerCase().contains("parameters must be non-empty")));
    }

    @Test
    void parameterNullEntry_isError() {
        String json = """
                {
                  "xmlSchemaPath":"schemas/x.xsd",
                  "parameterConfig": { "parameters": [ null ] }
                }""";
        var eo = RootConfigLoader.fromJson(json);
        assertTrue(eo.isError());
        assertTrue(eo.errorsOrThrow().stream().anyMatch(m -> m.toLowerCase().contains("parameters[0] is null")));
    }

    @Test
    void legalValueMustBeNonEmpty_andItemsNonBlank() {
        String json = """
                {
                  "xmlSchemaPath":"schemas/x.xsd",
                  "parameterConfig": {
                    "parameters":[
                      { "legalValue": [], "defaultValue": "a", "description":"d1" },
                      { "legalValue": ["", "a"], "defaultValue": "a", "description":"d2" }
                    ]
                  }
                }""";
        var eo = RootConfigLoader.fromJson(json);
        assertTrue(eo.isError());
        var errs = eo.errorsOrThrow();
        assertTrue(errs.stream().anyMatch(m -> m.contains("legalValue must be non-empty")));
        assertTrue(errs.stream().anyMatch(m -> m.contains("legalValue[0] must be non-empty")));
    }

    @Test
    void defaultValueMustExistAndBeInLegal() {
        String json = """
                {
                  "xmlSchemaPath":"schemas/x.xsd",
                  "parameterConfig": {
                    "parameters":[
                      { "legalValue":["a","b"], "defaultValue":"",       "description":"d1" },
                      { "legalValue":["a","b"], "defaultValue":"c",      "description":"d2" },
                      { "legalValue":["a","b"], "defaultValue": null,    "description":"d3" }
                    ]
                  }
                }""";
        var eo = RootConfigLoader.fromJson(json);
        assertTrue(eo.isError());
        var errs = eo.errorsOrThrow();
        assertTrue(errs.stream().anyMatch(m -> m.contains("defaultValue must be set")));
        assertTrue(errs.stream().anyMatch(m -> m.contains("defaultValue ('c')")));
    }

    @Test
    void malformedJson_returnsError() {
        ErrorsOr<RootConfig> eo = RootConfigLoader.fromJson("{ not valid JSON ");
        assertTrue(eo.isError(), "Expected error for malformed JSON");
        var errs = eo.errorsOrThrow();
        assertFalse(errs.isEmpty());
        assertTrue(errs.get(0).toLowerCase().contains("failed to parse"), () -> "Got: " + errs);
    }

    @Test
    void classpathNotFound_returnsError() {
        String path = "RootConfigLoaderTest/does-not-exist.json";
        ErrorsOr<RootConfig> eo = RootConfigLoader.fromClasspath(path);
        assertTrue(eo.isError(), "Expected error for missing classpath resource");
        var errs = eo.errorsOrThrow();
        // New behavior: prefix includes the resourcePath for classpath loads
        assertTrue(
                errs.get(0).toLowerCase().contains("classpath resource not found"),
                () -> "Got: " + errs
        );
    }
}
