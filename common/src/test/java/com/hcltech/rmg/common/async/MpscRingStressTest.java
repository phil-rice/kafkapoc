package com.hcltech.rmg.common.async;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Stress tests for the lock-free MPSC ring buffer (many producers, single consumer).
 */
public class MpscRingStressTest {

    private ExecutorService pool;

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.shutdownNow();
        }
    }

    /** Simple capture record. */
    static final class Rec {
        final String kind; // "ok" or "err"
        final String fr;
        final String in;
        final long corr;
        final String out;       // when ok
        final String errorType; // when err

        Rec(String kind, String fr, String in, long corr, String out, String errorType) {
            this.kind = kind; this.fr = fr; this.in = in; this.corr = corr; this.out = out; this.errorType = errorType;
        }
    }

    private static final String FR = "FR";

    // --- 1) Single consumer, many producers, random delays (success only) ---

    @Test
    void stressManyProducers_randomDelays_successOnly() throws Exception {
        final int CAPACITY = 512;
        final int PRODUCERS = 12;
        final int PER_PRODUCER = 1000;
        final int TOTAL = PRODUCERS * PER_PRODUCER;
        final long TIMEOUT_SEC = 30;

        IMpscRing<String, String, String> ring = new MpscRing<>(CAPACITY);
        pool = Executors.newScheduledThreadPool(PRODUCERS);

        CountDownLatch started = new CountDownLatch(PRODUCERS);
        CountDownLatch done = new CountDownLatch(PRODUCERS);
        AtomicLong corrGen = new AtomicLong(1);
        Random rnd = new Random(42);

        // Start producers: random delay per offer
        for (int p = 0; p < PRODUCERS; p++) {
            ((ScheduledExecutorService) pool).schedule(() -> {
                started.countDown();
                try { started.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

                for (int i = 0; i < PER_PRODUCER; i++) {
                    long corr = corrGen.getAndIncrement();
                    int delayMs = 1 + rnd.nextInt(4 + (i & 7)); // small jitter
                    try { Thread.sleep(delayMs); } catch (InterruptedException ignored) {}

                    // Offer with spin if the slot isn't ready yet
                    for (;;) {
                        if (ring.offerSuccess(FR, "in-"+corr, corr, "out-"+corr)) break;
                        Thread.onSpinWait();
                    }
                }
                done.countDown();
            }, 0, TimeUnit.MILLISECONDS);
        }

        // Single consumer loop
        List<Rec> got = Collections.synchronizedList(new ArrayList<>(TOTAL));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SEC);
        while ((done.getCount() > 0 || got.size() < TOTAL) && System.nanoTime() < deadline) {
            int drained = ring.drain(new IMpscRing.Handler<>() {
                @Override public void onSuccess(String fr, String in, long corr, String out) {
                    got.add(new Rec("ok", fr, in, corr, out, null));
                }
                @Override public void onFailure(String fr, String in, long corr, Throwable err) {
                    got.add(new Rec("err", fr, in, corr, null, err.getClass().getSimpleName()));
                }
            });
            if (drained == 0) {
                // light backoff to avoid burning CPU when idle
                Thread.sleep(1);
            }
        }

        assertEquals(TOTAL, got.size(), "missing records");

        // Integrity: all success, no duplicates, FR carried through
        BitSet seen = new BitSet(TOTAL + 10);
        for (Rec r : got) {
            assertEquals("ok", r.kind);
            assertEquals(FR, r.fr);
            assertEquals("in-"+r.corr, r.in);
            assertEquals("out-"+r.corr, r.out);
            int idx = Math.toIntExact(r.corr); // safe for this test range
            assertFalse(seen.get(idx), "duplicate corr id " + r.corr);
            seen.set(idx);
        }
    }

    // --- 2) Mix success and failure with random delays ---

    @Test
    void stressMixedSuccessFailure_randomDelays() throws Exception {
        final int CAPACITY = 512;
        final int PRODUCERS = 8;
        final int PER_PRODUCER = 3000;
        final int TOTAL = PRODUCERS * PER_PRODUCER;
        final long TIMEOUT_SEC = 30;

        IMpscRing<String, String, String> ring = new MpscRing<>(CAPACITY);
        pool = Executors.newScheduledThreadPool(PRODUCERS);

        CountDownLatch started = new CountDownLatch(PRODUCERS);
        CountDownLatch done = new CountDownLatch(PRODUCERS);
        AtomicLong corrGen = new AtomicLong(1);
        Random rnd = new Random(7);

        for (int p = 0; p < PRODUCERS; p++) {
            ((ScheduledExecutorService) pool).schedule(() -> {
                started.countDown();
                try { started.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

                for (int i = 0; i < PER_PRODUCER; i++) {
                    long corr = corrGen.getAndIncrement();
                    try { Thread.sleep(1 + rnd.nextInt(5)); } catch (InterruptedException ignored) {}

                    boolean ok = rnd.nextInt(5) != 0; // 80% success, 20% failure
                    for (;;) {
                        if (ok) {
                            if (ring.offerSuccess(FR, "in-"+corr, corr, "out-"+corr)) break;
                        } else {
                            if (ring.offerFailure(FR, "in-"+corr, corr, new TimeoutX("tmo"))) break;
                        }
                        Thread.onSpinWait();
                    }
                }
                done.countDown();
            }, 0, TimeUnit.MILLISECONDS);
        }

        List<Rec> got = Collections.synchronizedList(new ArrayList<>(TOTAL));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SEC);
        while ((done.getCount() > 0 || got.size() < TOTAL) && System.nanoTime() < deadline) {
            int drained = ring.drain(new IMpscRing.Handler<>() {
                @Override public void onSuccess(String fr, String in, long corr, String out) {
                    got.add(new Rec("ok", fr, in, corr, out, null));
                }
                @Override public void onFailure(String fr, String in, long corr, Throwable err) {
                    got.add(new Rec("err", fr, in, corr, null, err.getClass().getSimpleName()));
                }
            });
            if (drained == 0) Thread.sleep(1);
        }

        assertEquals(TOTAL, got.size());

        // Basic integrity
        Set<Long> seen = new HashSet<>(TOTAL);
        for (Rec r : got) {
            assertTrue(seen.add(r.corr), "duplicate corr " + r.corr);
            assertEquals(FR, r.fr);
            if ("ok".equals(r.kind)) {
                assertEquals("in-"+r.corr, r.in);
                assertEquals("out-"+r.corr, r.out);
            } else {
                assertEquals("in-"+r.corr, r.in);
                assertEquals("TimeoutX", r.errorType);
            }
        }
    }

    // --- 3) Capacity wrap-around pressure, interleaving produce/consume ---

    @Test
    void stressWrapAround_interleaved() throws Exception {
        final int CAPACITY = 64; // small to force wrap quickly
        final int PRODUCERS = 6;
        final int PER_PRODUCER = 4000;
        final int TOTAL = PRODUCERS * PER_PRODUCER;

        IMpscRing<String, String, String> ring = new MpscRing<>(CAPACITY);
        pool = Executors.newFixedThreadPool(PRODUCERS);

        AtomicLong corrGen = new AtomicLong(1);
        CountDownLatch started = new CountDownLatch(PRODUCERS);
        CountDownLatch done = new CountDownLatch(PRODUCERS);

        Runnable producer = () -> {
            started.countDown();
            try { started.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            for (int i = 0; i < PER_PRODUCER; i++) {
                long corr = corrGen.getAndIncrement();
                for (;;) {
                    if (ring.offerSuccess(FR, "in-"+corr, corr, "out-"+corr)) break;
                    Thread.onSpinWait();
                }
                if ((corr & 7) == 0) Thread.yield();
            }
            done.countDown();
        };
        for (int i = 0; i < PRODUCERS; i++) pool.submit(producer);

        List<Rec> got = Collections.synchronizedList(new ArrayList<>(TOTAL));
        while (done.getCount() > 0 || got.size() < TOTAL) {
            int drained = ring.drain(new IMpscRing.Handler<>() {
                @Override public void onSuccess(String fr, String in, long corr, String out) {
                    got.add(new Rec("ok", fr, in, corr, out, null));
                }
                @Override public void onFailure(String fr, String in, long corr, Throwable err) {
                    got.add(new Rec("err", fr, in, corr, null, err.getClass().getSimpleName()));
                }
            });
            if (drained == 0) Thread.onSpinWait();
        }

        assertEquals(TOTAL, got.size());
        // spot check ends
        assertTrue(got.get(0).corr >= 1);
        assertTrue(got.get(got.size()-1).corr <= TOTAL);
        // integrity: no dups
        BitSet seen = new BitSet(TOTAL + 10);
        for (Rec r : got) {
            int idx = Math.toIntExact(r.corr);
            assertFalse(seen.get(idx), "dup corr " + r.corr);
            seen.set(idx);
        }
    }

    // --- 4) Empty drain returns 0 (replaces pollOne test) ---

    @Test
    void emptyDrainReturnsZero() {
        IMpscRing<String, String, String> ring = new MpscRing<>(8);
        int drained = ring.drain(new IMpscRing.Handler<>() {
            @Override public void onSuccess(String fr, String in, long corr, String out) { /* no-op */ }
            @Override public void onFailure(String fr, String in, long corr, Throwable err) { /* no-op */ }
        });
        assertEquals(0, drained);
    }

    // local unchecked timeout type for failure path
    static final class TimeoutX extends RuntimeException {
        TimeoutX(String m) { super(m); }
    }
}
