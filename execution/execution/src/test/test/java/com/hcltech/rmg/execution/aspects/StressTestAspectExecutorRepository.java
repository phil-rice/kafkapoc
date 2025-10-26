package com.hcltech.rmg.execution.aspects;

import com.hcltech.rmg.common.function.Callback;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class StressTestAspectExecutorRepository {

    // ---- Tunables (override with -Dcalls=, -Dthreads=, -DminDelayMs=, -DmaxDelayMs=) ----
    private static final int TOTAL_CALLS    = Integer.getInteger("calls",      50_000);
    private static final int THREADS        = Integer.getInteger("threads",    200);
    private static final int MIN_DELAY_MS   = Integer.getInteger("minDelayMs", 1);
    private static final int MAX_DELAY_MS   = Integer.getInteger("maxDelayMs", 10);
    private static final Duration AWAIT_MAX = Duration.ofMinutes(2);

    // Two exact component classes to validate exact-class dispatch
    static final class CompA { final int id; CompA(int id) { this.id = id; } }
    static final class CompB { final int id; CompB(int id) { this.id = id; } }

    // Result echoes enough to validate integrity
    static final class Result {
        final String key;
        final Class<?> componentClass;
        final int input;
        final long threadId;

        Result(String key, Class<?> componentClass, int input, long threadId) {
            this.key = key;
            this.componentClass = componentClass;
            this.input = input;
            this.threadId = threadId;
        }
    }

    @Test
    void stress_noDrops_noDuplicates_noCrossover_orderedFoldMatches() throws Exception {
        var repo = new AspectExecutorRepository<Object, Integer, Result>();
        var rngTL = ThreadLocal.withInitial(() -> new Random(System.nanoTime()));

        AspectExecutor<Object, Integer, Result> execA = (key, component, input) -> {
            if (!(component instanceof CompA)) throw new IllegalStateException("A got " + component);
            randomSleep(rngTL.get());
            return new Result(key, component.getClass(), input, Thread.currentThread().getId());
        };
        AspectExecutor<Object, Integer, Result> execB = (key, component, input) -> {
            if (!(component instanceof CompB)) throw new IllegalStateException("B got " + component);
            randomSleep(rngTL.get());
            return new Result(key, component.getClass(), input, Thread.currentThread().getId());
        };

        repo.registerSync(CompA.class, execA);
        repo.registerSync(CompB.class, execB);

        var dispatcher = repo.build();

        // Orchestration
        var pool = Executors.newFixedThreadPool(THREADS, r -> {
            var t = new Thread(r, "stress-" + THREAD_COUNTER.incrementAndGet());
            t.setDaemon(true);
            return t;
        });

        var latch = new CountDownLatch(TOTAL_CALLS);
        var successes = new AtomicInteger();
        var failures  = new AtomicInteger();

        // Exactly one completion per seq; detect duplicates
        Map<Integer, Result> resultsBySeq = new ConcurrentHashMap<>(TOTAL_CALLS * 2);

        // Per-class counts as a sanity check (even/odd routing)
        var countA = new AtomicInteger();
        var countB = new AtomicInteger();

        for (int i = 0; i < TOTAL_CALLS; i++) {
            final int seq = i;
            pool.submit(() -> {
                Object component = (seq & 1) == 0 ? new CompA(seq) : new CompB(seq);
                String key = "k-" + seq;
                int input = seq;

                dispatcher.call(key, component, input, new Callback<>() {
                    @Override public void success(Result value) {
                        try {
                            assertNotNull(value, "null result");
                            assertEquals(key, value.key, "key mismatch");
                            assertEquals(input, value.input, "input mismatch");
                            assertEquals(component.getClass(), value.componentClass, "component class mismatch");

                            var prev = resultsBySeq.putIfAbsent(seq, value);
                            if (prev != null) {
                                fail("Duplicate callback for seq=" + seq);
                            }

                            if (component instanceof CompA) countA.incrementAndGet();
                            else countB.incrementAndGet();

                            successes.incrementAndGet();
                        } catch (AssertionError e) {
                            failures.incrementAndGet();
                        } finally {
                            latch.countDown();
                        }
                    }
                    @Override public void failure(Throwable t) {
                        failures.incrementAndGet();
                        latch.countDown();
                    }
                });
            });
        }

        boolean finished = latch.await(AWAIT_MAX.toMillis(), TimeUnit.MILLISECONDS);
        pool.shutdownNow();

        // Core assertions: no timeout, no drops/dupes/crossover
        assertTrue(finished, "Timed out waiting for callbacks");
        assertEquals(TOTAL_CALLS, resultsBySeq.size(), "Missing or duplicate results");
        assertEquals(TOTAL_CALLS, successes.get(), "Not all calls succeeded");
        assertEquals(0, failures.get(), "There were failures");

        // Even/odd routing sanity check
        if ((TOTAL_CALLS & 1) == 0) {
            assertEquals(TOTAL_CALLS / 2, countA.get(), "CompA count mismatch");
            assertEquals(TOTAL_CALLS / 2, countB.get(), "CompB count mismatch");
        } else {
            assertEquals(TOTAL_CALLS / 2 + 1, countA.get(), "CompA count mismatch");
            assertEquals(TOTAL_CALLS / 2,     countB.get(), "CompB count mismatch");
        }

        // ---- ORDER-SENSITIVE FOLD ----
        // Fold results strictly in submission order 0..TOTAL_CALLS-1 and compare against expected
        long actualOrderedHash = 1;
        for (int i = 0; i < TOTAL_CALLS; i++) {
            Result r = resultsBySeq.get(i);
            assertNotNull(r, "Missing result at index " + i); // extra guard
            actualOrderedHash = orderedFold(actualOrderedHash, r.input);
        }

        long expectedOrderedHash = 1;
        for (int i = 0; i < TOTAL_CALLS; i++) {
            expectedOrderedHash = orderedFold(expectedOrderedHash, i);
        }

        assertEquals(expectedOrderedHash, actualOrderedHash,
                "Ordered fold mismatch (suggests loss, crossover, or accumulation out of order)");
    }

    // Order-sensitive fold (not commutative): rolling polynomial hash over inputs
    private static long orderedFold(long acc, int value) {
        // Simple, fast, order-sensitive hash
        return acc * 31 + value;
    }

    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger();

    private static void randomSleep(Random r) {
        int range = Math.max(0, MAX_DELAY_MS - MIN_DELAY_MS);
        int delay = MIN_DELAY_MS + (range == 0 ? 0 : r.nextInt(range + 1));
        if (delay <= 0) return;
        try { Thread.sleep(delay); }
        catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }
}
