package com.hcltech.rmg.xml;

import com.hcltech.rmg.common.errorsor.ErrorsOr;

import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface XmlTypeClass<Schema> extends KeyExtractor {
    Schema loadSchema(String schemaName, InputStream schemaStream);

    ErrorsOr<Map<String, Object>> parseAndValidate(String xml, Schema schema);

    // ---------- helpers: classpath schema loading (monadic) ----------

    /** Load a single optional schema; returns empty map if name is null/blank. */
    static <S> ErrorsOr<Map<String, S>> loadOptionalSchema(XmlTypeClass<S> tc, String schemaName) {
        if (schemaName == null || schemaName.isBlank()) {
            return ErrorsOr.lift(Map.of());
        }
        InputStream in = resource(schemaName);
        if (in == null) {
            return ErrorsOr.error("Schema resource not found on classpath: " + schemaName);
        }
        try (in) {
            S schema = tc.loadSchema(schemaName, in);
            return ErrorsOr.lift(Map.of(schemaName, schema));
        } catch (Exception e) {
            return ErrorsOr.error("Failed to load schema {0}: {1}", e);
        }
    }

    /** Load multiple schema names from classpath into a map (name -> schema). */
    static <S> ErrorsOr<Map<String, S>> loadSchemas(XmlTypeClass<S> tc, List<String> schemaNames) {
        Map<String, S> out = new LinkedHashMap<>();
        List<String> errs = new java.util.ArrayList<>();
        for (String name : schemaNames) {
            if (name == null || name.isBlank()) {
                errs.add("Schema name must be non-empty");
                continue;
            }
            InputStream in = resource(name);
            if (in == null) {
                errs.add("Schema resource not found on classpath: " + name);
                continue;
            }
            try (in) {
                S schema = tc.loadSchema(name, in);
                if (out.put(name, schema) != null) {
                    errs.add("Duplicate schema name: " + name);
                }
            } catch (Exception e) {
                errs.add("Failed to load schema '" + name + "': " + e.getMessage());
            }
        }
        return errs.isEmpty() ? ErrorsOr.lift(Map.copyOf(out)) : ErrorsOr.errors(errs);
    }

    /** TCCL-first resource resolution with fallback. */
    private static InputStream resource(String path) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream in = (cl != null) ? cl.getResourceAsStream(path) : null;
        if (in == null) in = XmlTypeClass.class.getClassLoader().getResourceAsStream(path);
        return in;
    }
}
