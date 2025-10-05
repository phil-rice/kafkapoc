package com.hcltech.rmg.config.loader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.config.config.RootConfig;
import com.hcltech.rmg.parameters.OneParameterConfig;
import com.hcltech.rmg.parameters.ParameterConfig;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.hcltech.rmg.config.loader.BaseConfigLoader.base;

public interface RootConfigLoader {

    ObjectMapper JSON = base(new ObjectMapper());
    ObjectReader ROOT_READER = JSON.readerFor(RootConfig.class);

    // -------- Parse from JSON --------

    static ErrorsOr<RootConfig> fromJson(InputStream in) {
        try {
            RootConfig rc = ROOT_READER.readValue(in);
            List<String> errs = validate(rc);
            return errs.isEmpty() ? ErrorsOr.lift(rc) : ErrorsOr.errors(errs);
        } catch (Exception e) {
            return ErrorsOr.error("Failed to parse RootConfig: {0}: {1}", e);
        }
    }

    static ErrorsOr<RootConfig> fromJson(String json) {
        try (InputStream in = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
            return fromJson(in);
        } catch (Exception e) {
            return ErrorsOr.error("Failed to read RootConfig JSON: {0}: {1}", e);
        }
    }

    // -------- Load from classpath --------

    /** Load from a classpath resource using the thread context ClassLoader (with fallback). */
    static ErrorsOr<RootConfig> fromClasspath(String resourcePath) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        return fromClasspath(resourcePath, cl);
    }

    /** Load from a classpath resource using the provided ClassLoader (with fallback). */
    static ErrorsOr<RootConfig> fromClasspath(String resourcePath, ClassLoader cl) {
        try {
            InputStream in = (cl == null) ? null : cl.getResourceAsStream(resourcePath);
            if (in == null) {
                ClassLoader fallback = RootConfigLoader.class.getClassLoader();
                in = (fallback == null) ? null : fallback.getResourceAsStream(resourcePath);
            }
            if (in == null) {
                return ErrorsOr.error("Classpath resource not found: " + resourcePath);
            }
            try (InputStream autoClose = in) {
                // Prefix any validation/parse errors with the resource path to aid triage.
                return fromJson(autoClose).addPrefixIfError("rootconfig '" + resourcePath + "': ");
            }
        } catch (Exception e) {
            // Include the path explicitly; your ErrorsOr(error, Exception) only formats {0} and {1}.
            return ErrorsOr.errors(List.of(
                    "Failed to load RootConfig from classpath '" + resourcePath + "': "
                            + e.getClass().getSimpleName() + ": " + String.valueOf(e.getMessage())
            ));
        }
    }

    static ObjectMapper strictJson() { return JSON; }

    // -------- Validation: return list of error strings (empty = OK) --------

    private static List<String> validate(RootConfig rc) {
        List<String> errs = new ArrayList<>();
        if (rc == null) {
            errs.add("RootConfig is null");
            return errs;
        }

        // xmlSchemaPath
        if (rc.xmlSchemaPath() == null || rc.xmlSchemaPath().trim().isEmpty()) {
            errs.add("RootConfig.xmlSchemaPath must be set (non-empty)");
        }

        // parameterConfig
        ParameterConfig pc = rc.parameterConfig();
        if (pc == null) {
            errs.add("RootConfig.parameterConfig must be present");
            return errs;
        }

        List<OneParameterConfig> params = pc.parameters();
        if (params == null || params.isEmpty()) {
            errs.add("RootConfig.parameterConfig.parameters must be non-empty");
            return errs;
        }

        for (int i = 0; i < params.size(); i++) {
            OneParameterConfig p = params.get(i);
            if (p == null) {
                errs.add("parameterConfig.parameters[" + i + "] is null");
                continue;
            }

            // legalValue must be non-empty; each element must be non-blank
            List<String> legal = p.legalValue();
            if (legal == null || legal.isEmpty()) {
                errs.add("parameterConfig.parameters[" + i + "].legalValue must be non-empty");
            } else {
                for (int j = 0; j < legal.size(); j++) {
                    String lv = legal.get(j);
                    if (lv == null || lv.isBlank()) {
                        errs.add("parameterConfig.parameters[" + i + "].legalValue[" + j + "] must be non-empty");
                    }
                }
            }

            // defaultValue must be present and one of legalValue (if legalValue is non-empty)
            String def = p.defaultValue();
            if (def == null || def.isEmpty()) {
                errs.add("parameterConfig.parameters[" + i + "].defaultValue must be set");
            } else if (legal != null && !legal.isEmpty() && !legal.contains(def)) {
                errs.add("parameterConfig.parameters[" + i + "].defaultValue ('" + def
                        + "') must be one of legalValue " + legal);
            }
        }

        return errs;
    }
}
