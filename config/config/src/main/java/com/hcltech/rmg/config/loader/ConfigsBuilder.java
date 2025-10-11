package com.hcltech.rmg.config.loader;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.config.config.Configs;
import com.hcltech.rmg.config.config.RootConfig;
import com.hcltech.rmg.parameters.ParamPermutations;

import java.io.InputStream;
import java.util.*;
import java.util.function.Function;

/**
 * Builds {@link Configs} by:
 * - permuting the parameter space in {@code root.parameterConfig()}
 * - mapping each permutation to a key (keyFn)
 * - mapping each permutation to a classpath resource (resourceFn)
 * - loading BehaviorConfig JSON and pairing it with RootConfig's shared fields
 * <p>
 * Assumes {@link RootConfig} is already validated (via RootConfigLoader).
 * This class only reports problems related to key/resource functions and resource loading.
 */
public interface ConfigsBuilder {

    static String defaultKeyFn(List<String> values) {
        return String.join(":", values);
    }

    static Function<List<String>, String> defaultResourceFn(String prefix) {
        return list -> prefix + String.join("/", list) + ".json";
    }

    static ErrorsOr<Configs> buildFromClasspath(
            RootConfig root,
            Function<List<String>, String> keyFn,
            Function<List<String>, String> resourceFn,
            ClassLoader cl
    ) {
        Objects.requireNonNull(root, "root");            // reference must be non-null
        Objects.requireNonNull(keyFn, "keyFn");
        Objects.requireNonNull(resourceFn, "resourceFn");
        Objects.requireNonNull(cl, "classLoader");

        Map<String, Config> out = new LinkedHashMap<>();
        List<String> errors = new ArrayList<>();

        // Assumes: root.parameterConfig() != null and contains parameters (validated upstream).
        ParamPermutations.permutations(root.parameterConfig()).forEach(values -> {
            String key = safeApply(keyFn, values, errors, "keyFn");
            if (key == null) return;

            String resource = safeApply(resourceFn, values, errors, "resourceFn");
            if (resource == null) return;

            try (InputStream in = cl.getResourceAsStream(resource)) {
                if (in == null) {
                    errors.add("Missing behavior resource on classpath: '" + resource + "' (values=" + values + ")");
                    return;
                }

                BehaviorConfig bc = BehaviorConfigLoader.validated(BehaviorConfigLoader.fromJson(in));
                Config cfg = new Config(bc, root.parameterConfig(), root.xmlSchemaPath());

                if (out.put(key, cfg) != null) {
                    errors.add("Duplicate key from keyFn: '" + key + "' (values=" + values + ")");
                }
            } catch (Exception e) {
                errors.add("Failed to load behavior '" + resource + "' (values=" + values + "): " + e.getMessage());
            }
        });

        return errors.isEmpty()
                ? ErrorsOr.lift(new Configs(Map.copyOf(out)))
                : ErrorsOr.errors(errors);
    }

    private static String safeApply(
            Function<List<String>, String> fn,
            List<String> values,
            List<String> errors,
            String name
    ) {
        try {
            String s = fn.apply(values);
            if (s == null || s.isBlank()) {
                errors.add(name + " returned blank for values=" + values);
                return null;
            }
            return s;
        } catch (Exception e) {
            errors.add(name + " threw for values=" + values + ": " + e.getMessage());
            return null;
        }
    }
}
