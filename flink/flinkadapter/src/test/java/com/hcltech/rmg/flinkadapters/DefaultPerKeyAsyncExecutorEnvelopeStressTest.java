package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.common.async.*;
import com.hcltech.rmg.messages.*;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real single-threaded stress test for DefaultPerKeyAsyncExecutor.
 * - Launch N envelopes for one key.
 * - Each async result completes after 1..10ms (real delay).
 * - Verify: completions happen out of order, but emits are strictly ordered.
 */
public class DefaultPerKeyAsyncExecutorEnvelopeStressTest {

    private static EnvelopeHeader<Object> header(String domainId) {
        return new EnvelopeHeader<>(
                "domainType",
                "eventType",
                new RawMessage("raw", domainId, 0L, 0L, 0, 0L, "", "", ""),
                null,
                null
        );
    }

    private static ValueEnvelope<Object, String> ve(String domainId, String data) {
        return new ValueEnvelope<>(header(domainId), data, null, new ArrayList<>());
    }

    @SuppressWarnings("unchecked")
    private static final HasSeq<Envelope<Object, String>> SEQ =
            (HasSeq<Envelope<Object, String>>) (HasSeq<?>) HasSeqsForEnvelope.INSTANCE;

    static final class CountingPermitManager implements PermitManager {
        private final Semaphore sem; private final int max;
        int acquired = 0, released = 0;
        CountingPermitManager(int max) { this.sem = new Semaphore(max); this.max = max; }
        @Override public boolean tryAcquire() { boolean ok = sem.tryAcquire(); if (ok) acquired++; return ok; }
        @Override public void release() { released++; sem.release(); }
        @Override public int availablePermits() { return sem.availablePermits(); }
        @Override public int maxPermits() { return max; }
    }

    static final class CapturingExec
            extends DefaultPerKeyAsyncExecutor<Envelope<Object, String>, Envelope<Object, String>> {
        final List<Envelope<Object, String>> ordered = new ArrayList<>();
        CapturingExec(int K,
                      Executor exec,
                      Function<Envelope<Object, String>, CompletionStage<Envelope<Object, String>>> fn,
                      FailureAdapter<Envelope<Object, String>, Envelope<Object, String>> failure,
                      PermitManager pm) {
            super(K, exec, fn, failure, pm, SEQ, SEQ);
        }
        @Override protected void handleOrdered(Envelope<Object, String> out) { ordered.add(out); }
    }

    record Due(long dueNanos, int index, CompletableFuture<Envelope<Object, String>> f) {}

    @Test
    void stress_realTime_singleThread_outOfOrderCompletions_but_strictOrderedEmits() {
        final int N = 50_000;   // ~2–3s on a modern CPU
        final String KEY = "K";
        final Random rnd = new Random(123);

        final CountingPermitManager pm = new CountingPermitManager(512);
        final Executor direct = Runnable::run;
        final int K = 128;

        final PriorityQueue<Due> dueQueue = new PriorityQueue<>(Comparator.comparingLong(Due::dueNanos));
        final List<Integer> completionOrder = new ArrayList<>();
        @SuppressWarnings("unchecked")
        final CompletableFuture<Envelope<Object, String>>[] futures = new CompletableFuture[N];

        Function<Envelope<Object, String>, CompletionStage<Envelope<Object, String>>> asyncFn = in -> {
            int idx = Integer.parseInt(in.valueEnvelope().data());
            CompletableFuture<Envelope<Object, String>> f = new CompletableFuture<>();
            futures[idx] = f;
            int delayMs = 1 + rnd.nextInt(10);
            long dueNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delayMs);
            dueQueue.add(new Due(dueNanos, idx, f));
            return f;
        };

        // use the real failure adapter (operator id "async")
        FailureAdapter<Envelope<Object, String>, Envelope<Object, String>> failure =
                EnvelopeFailureAdapter.defaultAdapter("async");

        CapturingExec exec = new CapturingExec(K, direct, asyncFn, failure, pm);

        int launched = 0;
        long testThreadId = Thread.currentThread().getId();

        while (exec.ordered.size() < N) {
            // Launch until capacity full
            while (launched < N) {
                Envelope<Object, String> in = ve(KEY, Integer.toString(launched));
                if (exec.execute(in) == AcceptResult.LAUNCHED) launched++;
                else break;
            }

            // Sleep until next due completion
            if (!dueQueue.isEmpty()) {
                Due next = dueQueue.peek();
                long now = System.nanoTime();
                long sleepNanos = next.dueNanos() - now;
                if (sleepNanos > 0) {
                    try { TimeUnit.NANOSECONDS.sleep(sleepNanos); } catch (InterruptedException ignored) {}
                }

                now = System.nanoTime();
                while (!dueQueue.isEmpty() && dueQueue.peek().dueNanos() <= now) {
                    Due d = dueQueue.poll();
                    completionOrder.add(d.index());
                    Envelope<Object, String> out = ve(KEY, "ok:" + d.index());
                    d.f().complete(out);
                }
            }

            exec.onWake();

            // sanity: ensure all callbacks run on same thread
            assertEquals(testThreadId, Thread.currentThread().getId(), "must stay single-threaded");
        }

        // ---------- Assertions ----------

        // 1. Out-of-order completions must have occurred
        boolean sawOutOfOrder = false;
        for (int i = 1; i < completionOrder.size(); i++) {
            if (completionOrder.get(i) < completionOrder.get(i - 1)) { sawOutOfOrder = true; break; }
        }
        assertTrue(sawOutOfOrder, "test did not exercise out-of-order completions");

        // 2. Emits are strictly ordered by data (ok:0..ok:N-1)
        assertEquals(N, exec.ordered.size(), "must emit all outputs");
        for (int i = 0; i < N; i++) {
            Envelope<Object, String> e = exec.ordered.get(i);
            assertEquals("ok:" + i, e.valueEnvelope().data(), "emit order mismatch at " + i);
            if (i > 0) {
                long prevSeq = SEQ.get(exec.ordered.get(i - 1));
                long curSeq  = SEQ.get(e);
                assertTrue(prevSeq < curSeq, "seq must increase at index " + i);
            }
        }

        // 3. Permits balanced
        assertEquals(pm.acquired, pm.released, "permits not balanced");
        assertEquals(pm.maxPermits(), pm.availablePermits(), "permits not fully released");
    }
}
