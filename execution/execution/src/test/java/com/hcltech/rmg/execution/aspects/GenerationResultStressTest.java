package com.hcltech.rmg.execution.aspects;

import com.hcltech.rmg.common.function.SemigroupTc;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * STRESS tests for GenerationResult<Acc, Out, Comp>.
 * Acc = List<Integer>
 * Out = Integer
 * Comp = String
 *
 * Invariants checked:
 *  I1: Exactly one terminal continuation (success OR failure) fires per run.
 *  I2: On success, fold order is slot index order (not completion order).
 *  I3: Duplicate completions for a slot are ignored (no double count).
 *  I4: Late callbacks from a previous epoch are ignored.
 *  I5: No cross-run leakage of outputs.
 *  I6: Holds under heavy concurrency & random delays.
 */
@DisplayName("GenerationResult STRESS")
class GenerationResultStressTest {

    private static final SemigroupTc<List<Integer>, Integer> LIST_APPEND =
            (acc, v) -> { acc.add(v); return acc; };

    // Tune for CI vs. local runs
    private static final int SLOTS        = 128;
    private static final int THREADS      = 32;
    private static final int ITERS        = 8;
    private static final int MAX_DELAY_MS = 10;

    // -------- Scenario 1: pure success, random delays, many threads --------
    @Test
    @DisplayName("Stress: pure success, random delays, deterministic fold & single terminal")
    void stressPureSuccess() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        try {
            Random rng = new Random(1_234_567);

            for (int iter = 0; iter < ITERS; iter++) {
                GenerationResult<List<Integer>, Integer, String> gr =
                        new GenerationResult<>(LIST_APPEND, SLOTS, /*expectedPerSlot*/1);

                List<Integer> initial = new ArrayList<>();
                AtomicReference<List<Integer>> ok = new AtomicReference<>();
                AtomicReference<Throwable> err = new AtomicReference<>();
                AtomicInteger terminalCount = new AtomicInteger();

                gr.beginRun(iter, SLOTS, initial,
                        acc -> { ok.set(acc); terminalCount.incrementAndGet(); },
                        e   -> { err.set(e); terminalCount.incrementAndGet(); });

                final int epoch = gr.currentEpoch();

                CountDownLatch start = new CountDownLatch(1);
                CountDownLatch done  = new CountDownLatch(SLOTS);

                for (int slot = 0; slot < SLOTS; slot++) {
                    final int s = slot;
                    pool.submit(() -> {
                        try {
                            start.await();
                            rndSleep(rng, MAX_DELAY_MS);
                            gr.recordSuccess(epoch, s, Collections.singletonList(s));
                        } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        } finally {
                            done.countDown();
                        }
                    });
                }

                start.countDown();
                assertTrue(done.await(10, TimeUnit.SECONDS), "timeout in pure success run");

                // I1: exactly one terminal; success not failure
                assertEquals(1, terminalCount.get(), "exactly one terminal callback");
                assertNull(err.get(), "no error expected");
                assertNotNull(ok.get(), "success expected");

                // I2: deterministic fold in slot order
                List<Integer> res = ok.get();
                assertEquals(SLOTS, res.size());
                for (int i = 0; i < SLOTS; i++) assertEquals(i, res.get(i));
            }
        } finally {
            pool.shutdownNow();
        }
    }

    // -------- Scenario 2: duplicates + random delays --------
    @Test
    @DisplayName("Stress: duplicate successes per slot race; exactly one value per slot, fold order by slot")
    void stressDuplicateSuccessesIdempotent() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        try {
            Random rng = new Random(7_654_321);

            for (int iter = 0; iter < ITERS; iter++) {
                GenerationResult<List<Integer>, Integer, String> gr =
                        new GenerationResult<>(LIST_APPEND, SLOTS, 2);

                List<Integer> initial = new ArrayList<>();
                AtomicReference<List<Integer>> ok = new AtomicReference<>();
                AtomicReference<Throwable> err = new AtomicReference<>();
                AtomicInteger terminalCount = new AtomicInteger();

                gr.beginRun(iter, SLOTS, initial,
                        acc -> { ok.set(acc); terminalCount.incrementAndGet(); },
                        e   -> { err.set(e); terminalCount.incrementAndGet(); });

                final int epoch = gr.currentEpoch();

                CountDownLatch start = new CountDownLatch(1);
                CountDownLatch done  = new CountDownLatch(SLOTS * 2); // two completions per slot

                for (int slot = 0; slot < SLOTS; slot++) {
                    final int s = slot;

                    // Primary success
                    pool.submit(() -> {
                        try {
                            start.await();
                            rndSleep(rng, MAX_DELAY_MS);
                            gr.recordSuccess(epoch, s, Collections.singletonList(s));
                        } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        } finally {
                            done.countDown();
                        }
                    });

                    // Duplicate success with a different marker (may win the race)
                    pool.submit(() -> {
                        try {
                            start.await();
                            rndSleep(rng, MAX_DELAY_MS);
                            gr.recordSuccess(epoch, s, Collections.singletonList(100_000 + s));
                        } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        } finally {
                            done.countDown();
                        }
                    });
                }

                start.countDown();
                assertTrue(done.await(10, TimeUnit.SECONDS), "timeout in duplicate successes run");

                // Exactly one terminal; it must be success (no failures were sent)
                assertEquals(1, terminalCount.get(), "exactly one terminal callback");
                assertNull(err.get(), "no error expected");
                List<Integer> res = ok.get();
                assertNotNull(res);

                // Idempotence + deterministic fold: exactly one value per slot, slot order
                assertEquals(SLOTS, res.size());
                for (int i = 0; i < SLOTS; i++) {
                    int v = res.get(i);
                    assertTrue(v == i || v == 100_000 + i,
                            "slot " + i + " must be either primary or duplicate value, got " + v);
                }
            }
        } finally {
            pool.shutdownNow();
        }
    }


    // -------- Scenario 3: mixed success/failure --------
    @Test
    @DisplayName("Stress: mixed success/failure, aggregate errors once")
    void stressMixedSuccessFailure() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        try {
            Random rng = new Random(13579);

            for (int iter = 0; iter < ITERS; iter++) {
                GenerationResult<List<Integer>, Integer, String> gr =
                        new GenerationResult<>(LIST_APPEND, SLOTS, 1);

                List<Integer> initial = new ArrayList<>();
                AtomicReference<List<Integer>> ok = new AtomicReference<>();
                AtomicReference<Throwable> err = new AtomicReference<>();
                AtomicInteger terminalCount = new AtomicInteger();

                gr.beginRun(iter, SLOTS, initial,
                        acc -> { ok.set(acc); terminalCount.incrementAndGet(); },
                        e   -> { err.set(e); terminalCount.incrementAndGet(); });

                final int epoch = gr.currentEpoch();

                CountDownLatch start = new CountDownLatch(1);
                CountDownLatch done  = new CountDownLatch(SLOTS);

                // Randomly mark about 20% slots as failing
                boolean[] willFail = new boolean[SLOTS];
                for (int s = 0; s < SLOTS; s++) {
                    willFail[s] = (rng.nextInt(5) == 0);
                }

                for (int slot = 0; slot < SLOTS; slot++) {
                    final int s = slot;
                    pool.submit(() -> {
                        try {
                            start.await();
                            rndSleep(rng, MAX_DELAY_MS);
                            if (willFail[s]) {
                                gr.recordFailure(epoch, s, "C" + s, new IllegalStateException("fail-" + s));
                            } else {
                                gr.recordSuccess(epoch, s, Collections.singletonList(s));
                            }
                        } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        } finally {
                            done.countDown();
                        }
                    });
                }

                start.countDown();
                assertTrue(done.await(10, TimeUnit.SECONDS), "timeout in mixed run");

                // I1: exactly one terminal callback
                assertEquals(1, terminalCount.get(), "exactly one terminal callback");

                int failCount = 0; for (boolean b : willFail) if (b) failCount++;
                if (failCount > 0) {
                    assertNull(ok.get(), "should not succeed when failures exist");
                    assertNotNull(err.get());
                    assertTrue(err.get() instanceof EnrichmentBatchException);
                    EnrichmentBatchException ex = (EnrichmentBatchException) err.get();
                    assertEquals(iter, ex.generationIndex());
                    assertEquals(failCount, ex.errors().size(), "aggregated failure count");
                } else {
                    assertNull(err.get());
                    assertNotNull(ok.get());
                    List<Integer> res = ok.get();
                    for (int i = 0; i < SLOTS; i++) assertEquals(i, res.get(i));
                }
            }
        } finally {
            pool.shutdownNow();
        }
    }

    // -------- Scenario 4: epoch late callbacks ignored --------
    @Test
    @DisplayName("Stress: epoch guard drops late callbacks from previous run")
    void stressEpochLateCallbacks() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        try {
            Random rng = new Random(24680);

            for (int iter = 0; iter < ITERS; iter++) {
                GenerationResult<List<Integer>, Integer, String> gr =
                        new GenerationResult<>(LIST_APPEND, SLOTS, 1);

                AtomicReference<List<Integer>> ok = new AtomicReference<>();
                AtomicReference<Throwable> err = new AtomicReference<>();
                AtomicInteger terminalCount = new AtomicInteger();

                // Run A
                gr.beginRun(iter, SLOTS, new ArrayList<>(),
                        acc -> { ok.set(acc); terminalCount.incrementAndGet(); },
                        e   -> { err.set(e); terminalCount.incrementAndGet(); });
                int epochA = gr.currentEpoch();

                // Fire half of A's slots now
                CountDownLatch startA = new CountDownLatch(1);
                CountDownLatch halfA  = new CountDownLatch(SLOTS / 2);
                for (int s = 0; s < SLOTS / 2; s++) {
                    final int slot = s;
                    pool.submit(() -> {
                        try {
                            startA.await();
                            rndSleep(rng, MAX_DELAY_MS);
                            gr.recordSuccess(epochA, slot, Collections.singletonList(10_000 + slot)); // A mark
                        } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        } finally {
                            halfA.countDown();
                        }
                    });
                }
                startA.countDown();
                assertTrue(halfA.await(10, TimeUnit.SECONDS), "timeout in early A half");

                // Immediately start Run B
                ok.set(null); err.set(null); terminalCount.set(0);
                gr.beginRun(iter + 10_000, SLOTS, new ArrayList<>(),
                        acc -> { ok.set(acc); terminalCount.incrementAndGet(); },
                        e   -> { err.set(e); terminalCount.incrementAndGet(); });
                int epochB = gr.currentEpoch();

                // Fire all B slots
                CountDownLatch startB = new CountDownLatch(1);
                CountDownLatch doneB  = new CountDownLatch(SLOTS);
                for (int s = 0; s < SLOTS; s++) {
                    final int slot = s;
                    pool.submit(() -> {
                        try {
                            startB.await();
                            rndSleep(rng, MAX_DELAY_MS);
                            gr.recordSuccess(epochB, slot, Collections.singletonList(slot)); // B mark
                        } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        } finally {
                            doneB.countDown();
                        }
                    });
                }
                startB.countDown();

                // Meanwhile, schedule the late second half of A
                CountDownLatch lateA = new CountDownLatch(SLOTS - SLOTS / 2);
                for (int s = SLOTS / 2; s < SLOTS; s++) {
                    final int slot = s;
                    pool.submit(() -> {
                        try {
                            rndSleep(rng, MAX_DELAY_MS);
                            gr.recordSuccess(epochA, slot, Collections.singletonList(10_000 + slot)); // late A
                        } finally {
                            lateA.countDown();
                        }
                    });
                }

                assertTrue(doneB.await(10, TimeUnit.SECONDS), "timeout in B");
                assertTrue(lateA.await(10, TimeUnit.SECONDS), "timeout in late A");

                // I1: exactly one terminal (B's)
                assertEquals(1, terminalCount.get());
                assertNull(err.get());
                List<Integer> res = ok.get();
                assertNotNull(res);

                // I4 + I5: result must contain only B's marks (0..SLOTS-1), not A's 10_000+ marks
                assertEquals(SLOTS, res.size());
                for (int i = 0; i < SLOTS; i++) assertEquals(i, res.get(i));
            }
        } finally {
            pool.shutdownNow();
        }
    }

    // -------- helpers --------
    private static void rndSleep(Random rng, int maxMs) {
        if (maxMs <= 0) return;
        try {
            Thread.sleep(rng.nextInt(maxMs + 1));
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}
