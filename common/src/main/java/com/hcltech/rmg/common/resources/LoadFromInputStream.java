package com.hcltech.rmg.common.resources;

import java.io.InputStream;
import java.util.Objects;

@FunctionalInterface
public interface LoadFromInputStream<I, O> {
    O apply(I in) throws Exception;

    static <T> T loadFromClasspath(String prefix, String name, LoadFromInputStream<InputStream, T> fn) {
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(fn, "fn");

        String full = prefix + name;
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try (InputStream in = loader.getResourceAsStream(full)) {
            if (in == null) {
                throw new IllegalArgumentException("Resource not found on classpath: " + full);
            }
            return fn.apply(in);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to load resource: " + full, e);
        }
    }
}
