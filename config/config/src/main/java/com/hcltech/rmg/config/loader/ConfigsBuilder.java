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

public interface ConfigsBuilder {

    /**
     * Build Configs by:
     * - permuting the parameter space in root.parameterConfig()
     * - mapping each permutation to a key (keyFn)
     * - mapping each permutation to a classpath resource name (resourceFn)
     * - loading BehaviorConfig JSON from classpath and pairing it with RootConfig's shared fields
     *
     * @param root       already-deserialized RootConfig (holds ParameterConfig + xmlSchemaPath)
     * @param keyFn      maps the positional values to a unique key, e.g. values -> String.join("-", values)
     * @param resourceFn maps the positional values to a classpath resource, e.g. values -> "behaviors/%s/%s.json"
     * @param cl         classloader to read resources from (use getClass().getClassLoader())
     */
    static ErrorsOr<Configs> buildFromClasspath(RootConfig root,
                                                Function<List<String>, String> keyFn,
                                                Function<List<String>, String> resourceFn,
                                                ClassLoader cl) {

        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(keyFn, "keyFn");
        Objects.requireNonNull(resourceFn, "resourceFn");
        Objects.requireNonNull(cl, "cl");

        Map<String, Config> map = new LinkedHashMap<>();
        List<String> errors = new ArrayList<>();

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
                BehaviorConfig bc = BehaviorConfigLoader.fromJson(in);
                bc = BehaviorConfigLoader.validated(bc); // your existing light check
                Config cfg = new Config(bc, root.parameterConfig(), root.xmlSchemaPath());

                if (map.put(key, cfg) != null) {
                    errors.add("Duplicate key from keyFn: '" + key + "' (values=" + values + ")");
                }
            } catch (Exception e) {
                errors.add("Failed to load behavior '" + resource + "' (values=" + values + "): " + e.getMessage());
            }
        });

        return errors.isEmpty()
                ? ErrorsOr.lift(new Configs(Map.copyOf(map)))
                : ErrorsOr.errors(errors);
    }

    private static String safeApply(Function<List<String>, String> fn,
                                    List<String> values,
                                    List<String> errors,
                                    String name) {
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
