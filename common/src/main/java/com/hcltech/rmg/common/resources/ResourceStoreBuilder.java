package com.hcltech.rmg.common.resources;

import java.util.*;
import java.util.concurrent.*;


public final class ResourceStoreBuilder<T> {

    private final ResourceCompiler<T> compiler;
    private final ClassLoader loader;
    private Collection<String> names = List.of();
    private int parallelism = 0; // 0 = auto

    private ResourceStoreBuilder(ClassLoader loader, ResourceCompiler<T> compiler) {
        this.loader = loader != null ? loader : Thread.currentThread().getContextClassLoader();
        this.compiler = Objects.requireNonNull(compiler, "compiler");
    }

    public static <T> ResourceStoreBuilder<T> with(ResourceCompiler<T> compiler) {
        return new ResourceStoreBuilder<>(null, compiler);
    }

    public static <T> ResourceStoreBuilder<T> with(ClassLoader loader, ResourceCompiler<T> compiler) {
        return new ResourceStoreBuilder<>(loader, compiler);
    }

    public ResourceStoreBuilder<T> names(Collection<String> logicalNames) {
        this.names = new ArrayList<>(Objects.requireNonNull(logicalNames));
        return this;
    }

    public ResourceStoreBuilder<T> parallelism(int parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    /**
     * Compile all resources in parallel and return an immutable Map.
     * Keys are (prefix + logicalName).
     */
    public Map<String, T> build() {
        if (names.isEmpty()) return Map.of();

        final int threads = Math.max(1, Math.min(
                names.size(),
                parallelism > 0 ? parallelism : Runtime.getRuntime().availableProcessors()
        ));

        ExecutorService pool = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "resource-compiler");
            t.setDaemon(true);
            return t;
        });

        try {
            Map<String, CompletableFuture<T>> futures = new LinkedHashMap<>();
            for (String full : names) {
                futures.put(full, CompletableFuture.supplyAsync(() -> compileUnchecked(full), pool));
            }

            Map<String, T> results = new LinkedHashMap<>();
            for (Map.Entry<String, CompletableFuture<T>> e : futures.entrySet()) {
                try {
                    results.put(e.getKey(), e.getValue().join());
                } catch (CompletionException ex) {
                    throw new RuntimeException("Failed to compile resource: " + e.getKey(), ex.getCause());
                }
            }
            return Collections.unmodifiableMap(results);
        } finally {
            pool.shutdown();
        }
    }

    private T compileUnchecked(String fullResourceName) {
        try {
            return compiler.compile(fullResourceName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to compile resource: " + fullResourceName, e);
        }
    }
}
