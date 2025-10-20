package com.hcltech.rmg.common.async;// CircularBufferWithCallbackStressTest.java
import com.hcltech.rmg.common.async.CircularBufferWithCallback;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class CircularBufferWithCallbackStressTest {

    // Simple capture helper for drained values & failures
    private static class Captures<T> {
        final List<T> drained = new ArrayList<>();
        final List<Throwable> failures = new ArrayList<>();
    }

    private <T> CircularBufferWithCallback<T> mk(int capacity, Captures<T> cap) {
        return new CircularBufferWithCallback<>(
                capacity,
                cap.drained::add,
                (v, ex) -> cap.failures.add(ex)
        );
    }

    @Test
    public void stressOutOfOrderWithRandomDelays_singleThreadedBufferAccess() throws Exception {
        final int N = 5_000;
        final int CAP = 32;
        Captures<Long> cap = new Captures<>();

        CircularBufferWithCallback<Long> buf = mk(CAP, cap);

        ScheduledExecutorService sch = Executors.newScheduledThreadPool(8);
        BlockingQueue<Long> completions = new LinkedBlockingQueue<>();
        Random rnd = new Random(42);

        // Schedule random-delayed completions that enqueue only (buffer remains single-threaded)
        for (long seq = 0; seq < N; seq++) {
            int delayMs = 1 + rnd.nextInt(10); // 1..10 ms
            long s = seq;
            sch.schedule(() -> completions.add(s), delayMs, TimeUnit.MILLISECONDS);
        }

        // Stash arrivals that are temporarily out-of-window and retry them later
        ConcurrentMap<Long, Long> stash = new ConcurrentHashMap<>();
        boolean hasEverFailed = false; // true if canAdd(seq) returns false at least once

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (cap.drained.size() < N) {
            long remainingNs = deadline - System.nanoTime();
            if (remainingNs <= 0) {
                fail("stress test timed out; drained=" + cap.drained.size()
                        + ", baseSeq=" + buf.baseSeq() + ", stash=" + stash.size());
            }

            Long seq = completions.poll(
                    Math.min(remainingNs, TimeUnit.MILLISECONDS.toNanos(200)),
                    TimeUnit.NANOSECONDS
            );
            if (seq != null) {
                stash.put(seq, seq);
            }

            if (!stash.isEmpty()) {
                List<Long> keys = new ArrayList<>(stash.keySet());
                // Prefer smaller unsigned seq first to close gaps and relieve pressure
                keys.sort(Long::compareUnsigned);
                for (Long s : keys) {
                    if (buf.canAdd(s)) {
                        CircularBufferWithCallback.PutResult r = buf.put(s, s);
                        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, r);
                        stash.remove(s);
                    } else {
                        hasEverFailed = true;
                    }
                }
            }
        }

        sch.shutdownNow();
        assertTrue(cap.failures.isEmpty(), "onFailure should not be called: " + cap.failures);
        assertTrue(hasEverFailed, "Expected at least one canAdd(false) during stress");
        assertEquals(N, cap.drained.size());
        for (int i = 0; i < N; i++) {
            assertEquals((long) i, cap.drained.get(i));
        }
    }

    @Test
    public void stressBoundaryPressure_manyCanAddFalse() throws Exception {
        final int N = 20_000;     // larger volume
        final int CAP = 32;       // small window to force pressure
        final int PRODUCERS = 64; // high scheduling concurrency

        Captures<Long> cap = new Captures<>();
        CircularBufferWithCallback<Long> buf = mk(CAP, cap);

        ScheduledExecutorService sch = Executors.newScheduledThreadPool(PRODUCERS);
        BlockingQueue<Long> completions = new LinkedBlockingQueue<>();
        Random rnd = new Random(123);

        // Bias: early seqs slow, later seqs fast → persistent head-of-line gaps
        for (long seq = 0; seq < N; seq++) {
            int delayMs = (seq % 128 < 8) ? (50 + rnd.nextInt(50)) : (1 + rnd.nextInt(5));
            if (rnd.nextInt(1000) == 0) delayMs += 200; // occasional spike
            long s = seq;
            sch.schedule(() -> completions.add(s), delayMs, TimeUnit.MILLISECONDS);
        }

        ConcurrentMap<Long, Long> stash = new ConcurrentHashMap<>();
        long canAddFalse = 0;
        boolean hasEverFailed = false;

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (cap.drained.size() < N) {
            long remainingNs = deadline - System.nanoTime();
            if (remainingNs <= 0) {
                fail("boundary stress timed out; drained=" + cap.drained.size()
                        + ", baseSeq=" + buf.baseSeq() + ", stash=" + stash.size());
            }

            Long seq = completions.poll(
                    Math.min(remainingNs, TimeUnit.MILLISECONDS.toNanos(200)),
                    TimeUnit.NANOSECONDS
            );
            if (seq != null) {
                stash.put(seq, seq);
            }

            if (!stash.isEmpty()) {
                List<Long> keys = new ArrayList<>(stash.keySet());
                keys.sort(Long::compareUnsigned);
                for (Long s : keys) {
                    if (buf.canAdd(s)) {
                        CircularBufferWithCallback.PutResult r = buf.put(s, s);
                        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, r);
                        stash.remove(s);
                    } else {
                        canAddFalse++;
                        hasEverFailed = true;
                    }
                }
            }
        }

        sch.shutdownNow();
        assertTrue(cap.failures.isEmpty(), "onFailure should not be called: " + cap.failures);
        assertTrue(hasEverFailed, "Expected at least one canAdd(false)");
        assertTrue(canAddFalse > N / 10, "Expected frequent canAdd(false), saw=" + canAddFalse);
        assertEquals(N, cap.drained.size());
        for (int i = 0; i < N; i++) assertEquals((long) i, cap.drained.get(i));
    }

    @Test
    public void stressProbeRejections_withoutCanAddGuard() throws Exception {
        final int N = 10_000;
        final int CAP = 32;
        final int PRODUCERS = 64;

        Captures<Long> cap = new Captures<>();
        CircularBufferWithCallback<Long> buf = mk(CAP, cap);

        ScheduledExecutorService sch = Executors.newScheduledThreadPool(PRODUCERS);
        BlockingQueue<Long> completions = new LinkedBlockingQueue<>();
        Random rnd = new Random(7);

        // Same bias: early items slow, later fast → head-of-line gaps
        for (long seq = 0; seq < N; seq++) {
            int delayMs = (seq % 128 < 8) ? (60 + rnd.nextInt(60)) : (1 + rnd.nextInt(5));
            if (rnd.nextInt(800) == 0) delayMs += 200; // occasional spike
            long s = seq;
            sch.schedule(() -> completions.add(s), delayMs, TimeUnit.MILLISECONDS);
        }

        ConcurrentMap<Long, Long> stash = new ConcurrentHashMap<>();
        long rejectedWindowFull = 0;
        boolean hasEverFailed = false; // either guard false or actual rejection

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (cap.drained.size() < N) {
            long remainingNs = deadline - System.nanoTime();
            if (remainingNs <= 0) {
                fail("rejection probe timed out; drained=" + cap.drained.size()
                        + ", baseSeq=" + buf.baseSeq() + ", stash=" + stash.size());
            }

            Long seq = completions.poll(
                    Math.min(remainingNs, TimeUnit.MILLISECONDS.toNanos(200)),
                    TimeUnit.NANOSECONDS
            );
            if (seq != null) {
                stash.put(seq, seq);
            }

            if (!stash.isEmpty()) {
                // 1) Probe half the time WITHOUT canAdd to force buffer-side rejections
                List<Long> keys = new ArrayList<>(stash.keySet());
                for (Long s : keys) {
                    if (rnd.nextBoolean()) {
                        CircularBufferWithCallback.PutResult r = buf.put(s, s);
                        switch (r) {
                            case ACCEPTED -> stash.remove(s);
                            case REJECTED_WINDOW_FULL -> { rejectedWindowFull++; hasEverFailed = true; }
                            case ALREADY_COMMITTED_DROPPED -> stash.remove(s); // base advanced since stash
                            case DROPPED_AFTER_FINISH -> fail("unexpected finish");
                        }
                    }
                }
                // 2) For the rest, use the safe guard path
                keys = new ArrayList<>(stash.keySet());
                keys.sort(Long::compareUnsigned);
                for (Long s : keys) {
                    if (buf.canAdd(s)) {
                        CircularBufferWithCallback.PutResult r = buf.put(s, s);
                        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, r);
                        stash.remove(s);
                    } else {
                        hasEverFailed = true;
                    }
                }
            }
        }

        sch.shutdownNow();
        assertTrue(cap.failures.isEmpty(), "onFailure should not be called: " + cap.failures);
        assertTrue(hasEverFailed, "Expected to hit boundary (canAdd false and/or REJECTED_WINDOW_FULL)");
        assertTrue(rejectedWindowFull > 0, "Expected some direct REJECTED_WINDOW_FULL without guard");
        assertEquals(N, cap.drained.size());
        for (int i = 0; i < N; i++) assertEquals((long) i, cap.drained.get(i));
    }
}
