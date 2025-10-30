package com.hcltech.rmg.common.async;

import java.lang.reflect.Method;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pluggable factory for creating ExecutorService instances.
 *
 * Use the static helpers:
 *   - ExecutorServiceFactory.fixed()
 *   - ExecutorServiceFactory.loomOrThrow()
 *   - ExecutorServiceFactory.smart()  // try Loom (JDK 21+) else fixed
 *
 * Example:
 *   ExecutorServiceFactory f = ExecutorServiceFactory.smart();
 *   ExecutorService io = f.create(64, "io");
 */
public interface ExecutorServiceFactory {

    /**
     * Create an executor appropriate for the given level of concurrency.
     * @param threads             desired concurrency hint (ignored for Loom)
     * @param threadNamePrefix    prefix for thread names
     */
    ExecutorService create(int threads, String threadNamePrefix);

    /** Human-friendly name, e.g. "loom" or "fixed". */
    default String name() { return getClass().getSimpleName(); }

    /** True if the executor uses virtual threads. */
    default boolean isVirtual() { return false; }

    // ---------- Static helpers ----------

    /** Always create a fixed thread pool (daemon threads). */
    static ExecutorServiceFactory fixed() {
        return new FixedImpl();
    }

    /** Create a Loom (virtual thread) executor or throw if not available (requires JDK 21+). */
    static ExecutorServiceFactory loomOrThrow() {
        return new LoomImpl();
    }

    /**
     * Prefer Loom on JDK 21+, otherwise fall back to fixed pool.
     * Probes availability once at construction.
     */
    static ExecutorServiceFactory smart() {
        try {
            // Try to construct Loom and do a tiny probe
            LoomImpl loom = new LoomImpl();
            ExecutorService probe = loom.create(1, "probe");
            probe.shutdown();
            return loom;
        } catch (Throwable ignore) {
            return new FixedImpl();
        }
    }

    // ---------- Implementations (single file) ----------

    final class FixedImpl implements ExecutorServiceFactory {
        @Override
        public ExecutorService create(int threads, String prefix) {
            int n = Math.max(1, threads);
            return Executors.newFixedThreadPool(n, namedDaemon(Objects.requireNonNullElse(prefix, "pool")));
        }
        @Override public String name() { return "fixed"; }
        private static ThreadFactory namedDaemon(String prefix) {
            AtomicInteger seq = new AtomicInteger(1);
            return r -> {
                Thread t = new Thread(r, prefix + "-" + seq.getAndIncrement());
                t.setDaemon(true);
                return t;
            };
        }
    }

    final class LoomImpl implements ExecutorServiceFactory {
        @Override
        public ExecutorService create(int threads, String prefix) {
            try {
                var vf = Thread.ofVirtual().name(prefix == null ? "vt" : prefix, 0).factory();
                // per-task executor using our factory; honors your naming, same semantics
                return Executors.newThreadPerTaskExecutor(vf);
            } catch (Throwable t) {
                throw new UnsupportedOperationException("Virtual threads require JDK 21+", t);
            }
        }
        @Override public boolean isVirtual() { return true; }
        @Override public String name() { return "loom"; }
    }
}
