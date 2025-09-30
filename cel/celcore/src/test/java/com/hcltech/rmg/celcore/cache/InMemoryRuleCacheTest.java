package com.hcltech.rmg.celcore.cache;

import com.hcltech.rmg.celcore.CompiledRule;
import com.hcltech.rmg.celcore.RuleExecutor;
import com.hcltech.rmg.celcore.RuleUsage;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryRuleCacheTest {

    private static CompiledRule<String, Boolean> makeCompiled(String src) {
        return new CompiledRule<>() {
            @Override
            public String source() {
                return src;
            }

            @Override
            public RuleUsage usage() {
                return new RuleUsage() {
                    @Override
                    public List<String> inputPaths() {
                        return List.of();
                    }

                    @Override
                    public List<String> contextPaths() {
                        return List.of();
                    }
                };
            }

            @Override
            public RuleExecutor<String, Boolean> executor() {
                return (inp, ctx) -> ErrorsOr.lift(Boolean.TRUE);
            }
        };
    }

    @Test
    void compilesOncePerKeyAndCachesTheValue() {
        AtomicInteger compileCount = new AtomicInteger();

        InMemoryRuleCache<String, Boolean> cache =
                new InMemoryRuleCache<>(k -> {
                    compileCount.incrementAndGet();
                    return ErrorsOr.lift(makeCompiled(k));
                });

        var r1 = cache.get("k1");
        var r2 = cache.get("k1");

        assertTrue(r1.isValue(), "expected value");
        assertSame(r1, r2, "same cached ErrorsOr instance");
        assertEquals(1, compileCount.get(), "compile once per key");

        // Execute compiled rule
        var result = r1.getValue().orElseThrow()
                .executor().execute("input", null)
                .getValue().orElseThrow();
        assertTrue(result);
        assertEquals("k1", r1.getValue().orElseThrow().source());
    }

    @Test
    void distinctKeysCompileIndependently() {
        AtomicInteger compileCount = new AtomicInteger();
        InMemoryRuleCache<String, Boolean> cache =
                new InMemoryRuleCache<>(k -> {
                    compileCount.incrementAndGet();
                    return ErrorsOr.lift(makeCompiled(k));
                });

        var a = cache.get("a");
        var b = cache.get("b");

        assertTrue(a.isValue());
        assertTrue(b.isValue());
        assertEquals(2, compileCount.get());
        assertNotSame(a.getValue().orElseThrow(), b.getValue().orElseThrow());
        assertEquals("a", a.getValue().orElseThrow().source());
        assertEquals("b", b.getValue().orElseThrow().source());
    }

    @Test
    void cachesFailuresAsFailures() {
        AtomicInteger compileCount = new AtomicInteger();

        InMemoryRuleCache<String, Boolean> cache =
                new InMemoryRuleCache<>(k -> {
                    compileCount.incrementAndGet();
                    return ErrorsOr.error("bad rule for " + k);
                });

        var r1 = cache.get("broken");
        var r2 = cache.get("broken");

        assertTrue(r1.isError(), "expected error");
        assertSame(r1, r2, "same cached error instance");
        assertEquals(1, compileCount.get(), "compiled once even on error");
        assertEquals(List.of("bad rule for broken"), r1.getErrors());
    }

    @Test
    void preloadCompilesAllProvidedKeysAndGetUsesCache() {
        AtomicInteger compileCount = new AtomicInteger();

        InMemoryRuleCache<String, Boolean> cache =
                new InMemoryRuleCache<>(k -> {
                    compileCount.incrementAndGet();
                    return ErrorsOr.lift(makeCompiled(k));
                });

        cache.preloadWith(Set.of("a", "b", "c"));
        assertEquals(3, compileCount.get(), "preload should eagerly compile all keys");

        // Subsequent get should not recompile
        var r = cache.get("a");
        assertTrue(r.isValue());
        assertEquals(3, compileCount.get(), "no extra compile after get on preloaded key");
    }

    @Test
    void getThrowsOnNullKey() {
        InMemoryRuleCache<String, Boolean> cache =
                new InMemoryRuleCache<>(k -> ErrorsOr.lift(makeCompiled(k)));

        assertThrows(NullPointerException.class, () -> cache.get(null));
    }

    @Test
    void concurrentGetsStillCompileOnce() throws Exception {
        AtomicInteger compileCount = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);

        InMemoryRuleCache<String, Boolean> cache =
                new InMemoryRuleCache<>(k -> {
                    // simulate a slower compile so threads race properly
                    try {
                        start.await();
                    } catch (InterruptedException ignored) {
                    }
                    compileCount.incrementAndGet();
                    return ErrorsOr.lift(makeCompiled(k));
                });

        var pool = Executors.newFixedThreadPool(8);
        for (int i = 0; i < 8; i++) {
            pool.submit(() -> cache.get("race-key"));
        }

        // let them all run the computeIfAbsent race
        start.countDown();
        pool.shutdown();
        // quick spin-wait: ensure value is present
        var res = cache.get("race-key");
        assertTrue(res.isValue());
        assertEquals(1, compileCount.get(), "only one compile under concurrency");
    }
}
