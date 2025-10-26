package com.hcltech.rmg.execution.aspects;

import com.hcltech.rmg.common.function.SemigroupTc;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for GenerationResult<Acc, Out, Comp>.
 * Acc = List<Integer>
 * Out = Integer
 * Comp = String (component label for diagnostics)
 */
@DisplayName("GenerationResult")
class GenerationResultTest {

    /** Semigroup that appends integers to a (mutable) List<Integer> and returns it (in-place). */
    private static final SemigroupTc<List<Integer>, Integer> LIST_APPEND =
            (acc, v) -> { acc.add(v); return acc; };

    @Test
    @DisplayName("happy path: folds in slot order regardless of completion order")
    void happyPathDeterministicFold() {
        GenerationResult<List<Integer>, Integer, String> gr =
                new GenerationResult<>(LIST_APPEND, 4, 2);

        List<Integer> initial = new ArrayList<>();
        AtomicReference<List<Integer>> ok = new AtomicReference<>();
        AtomicReference<Throwable> err = new AtomicReference<>();

        gr.beginRun(0, 2, initial, ok::set, err::set);
        int epoch = gr.currentEpoch();

        // Complete slot 1 first, then slot 0 (out-of-order)
        gr.recordSuccess(epoch, 1, Collections.singletonList(3));
        gr.recordSuccess(epoch, 0, Arrays.asList(1, 2));

        assertNull(err.get(), "No error expected");
        assertNotNull(ok.get(), "Success continuation must fire");
        assertEquals(Arrays.asList(1, 2, 3), ok.get(), "Fold must respect slot order: slot0 then slot1");
    }

    @Test
    @DisplayName("sync fast-path: write into slotBuffer then successFromBuffer")
    void syncFastPath() {
        GenerationResult<List<Integer>, Integer, String> gr =
                new GenerationResult<>(LIST_APPEND, 4, 4);

        List<Integer> initial = new ArrayList<>();
        AtomicReference<List<Integer>> ok = new AtomicReference<>();
        AtomicReference<Throwable> err = new AtomicReference<>();

        gr.beginRun(1, 2, initial, ok::set, err::set);
        int epoch = gr.currentEpoch();

        // Slot 0 uses lane-owned buffer (no external list allocation)
        gr.slotBuffer(0).addAll(Arrays.asList(10, 11));
        gr.successFromBuffer(0);

        // Slot 1 uses external list path
        gr.recordSuccess(epoch, 1, Arrays.asList(20, 21));

        assertNull(err.get());
        assertEquals(Arrays.asList(10, 11, 20, 21), ok.get());
    }

    @Test
    @DisplayName("duplicate completions for a slot are ignored (exactly-once semantics)")
    void duplicateCallbacksIgnored() {
        GenerationResult<List<Integer>, Integer, String> gr =
                new GenerationResult<>(LIST_APPEND, 2, 2);

        List<Integer> initial = new ArrayList<>();
        AtomicReference<List<Integer>> ok = new AtomicReference<>();
        AtomicReference<Throwable> err = new AtomicReference<>();

        gr.beginRun(2, 2, initial, ok::set, err::set);
        int epoch = gr.currentEpoch();

        gr.recordSuccess(epoch, 0, Collections.singletonList(1));
        // Duplicate completion for same slot must be ignored
        gr.recordSuccess(epoch, 0, Collections.singletonList(999));

        gr.recordSuccess(epoch, 1, Collections.singletonList(2));

        assertNull(err.get());
        assertEquals(Arrays.asList(1, 2), ok.get());
    }

    @Test
    @DisplayName("aggregates failures and forwards EnrichmentBatchException once")
    void aggregatesFailures() {
        GenerationResult<List<Integer>, Integer, String> gr =
                new GenerationResult<>(LIST_APPEND, 3, 1);

        List<Integer> initial = new ArrayList<>();
        AtomicReference<List<Integer>> ok = new AtomicReference<>();
        AtomicReference<Throwable> err = new AtomicReference<>();

        gr.beginRun(3, 3, initial, ok::set, err::set);
        int epoch = gr.currentEpoch();

        gr.recordFailure(epoch, 0, "E0", new RuntimeException("boom0"));
        gr.recordSuccess(epoch, 1, Collections.singletonList(1));
        gr.recordFailure(epoch, 2, "E2", new IllegalStateException("boom2"));

        assertNull(ok.get(), "Should not succeed when any slot fails");
        Throwable t = err.get();
        assertNotNull(t);
        assertTrue(t instanceof EnrichmentBatchException, "Forwarded error must be EnrichmentBatchException");
        EnrichmentBatchException ex = (EnrichmentBatchException) t;
        assertEquals(3, ex.generationIndex());
        assertEquals(2, ex.errors().size());

        // Basic content checks
        EnrichmentBatchException.ErrorInfo i0 = ex.errors().get(0);
        assertNotNull(i0.component());
        assertNotNull(i0.cause());
    }

    @Test
    @DisplayName("epoch guard: late callbacks from older run are ignored")
    void epochGuardDropsLateCallbacks() {
        GenerationResult<List<Integer>, Integer, String> gr =
                new GenerationResult<>(LIST_APPEND, 2, 2);

        AtomicReference<List<Integer>> ok = new AtomicReference<>();
        AtomicReference<Throwable> err = new AtomicReference<>();

        // Run A
        gr.beginRun(10, 2, new ArrayList<>(), ok::set, err::set);
        int epochA = gr.currentEpoch();
        // Immediately start Run B
        gr.beginRun(11, 2, new ArrayList<>(), ok::set, err::set);
        int epochB = gr.currentEpoch();

        // Late completion from Run A (must be ignored)
        gr.recordSuccess(epochA, 0, Collections.singletonList(999));

        // Valid completions for Run B
        gr.recordSuccess(epochB, 0, Collections.singletonList(1));
        gr.recordSuccess(epochB, 1, Collections.singletonList(2));

        assertNull(err.get());
        assertEquals(Arrays.asList(1, 2), ok.get(), "Late epoch completions must not affect outcome");
    }

    @Test
    @DisplayName("concurrent completions across many slots complete exactly once")
    void concurrentCallbacks() throws Exception {
        final int slots = 128;
        GenerationResult<List<Integer>, Integer, String> gr =
                new GenerationResult<>(LIST_APPEND, slots, 1);

        List<Integer> initial = new ArrayList<>();
        AtomicReference<List<Integer>> ok = new AtomicReference<>();
        AtomicReference<Throwable> err = new AtomicReference<>();

        gr.beginRun(42, slots, initial, ok::set, err::set);
        final int epoch = gr.currentEpoch();

        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(slots);

            for (int i = 0; i < slots; i++) {
                final int slot = i;
                pool.submit(() -> {
                    try {
                        start.await();
                        gr.recordSuccess(epoch, slot, Collections.singletonList(slot));
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }

            start.countDown();
            assertTrue(done.await(5, TimeUnit.SECONDS), "Parallel slot completions timed out");

            // Completion should have triggered exactly once
            assertNull(err.get());
            List<Integer> res = ok.get();
            assertNotNull(res);
            assertEquals(slots, res.size());
            // Fold order must be slot order
            for (int i = 0; i < slots; i++) {
                assertEquals(i, res.get(i));
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @DisplayName("beginRun rejects size > maxSlots")
    void beginRunRejectsOversize() {
        GenerationResult<List<Integer>, Integer, String> gr =
                new GenerationResult<>(LIST_APPEND, 2, 1);

        assertThrows(IllegalArgumentException.class,
                () -> gr.beginRun(0, 3, new ArrayList<>(), acc -> {}, e -> {}));
    }

    @Test
    @DisplayName("defensive guards: out-of-range slot indices are ignored")
    void outOfRangeSlotsIgnored() {
        GenerationResult<List<Integer>, Integer, String> gr =
                new GenerationResult<>(LIST_APPEND, 2, 1);

        AtomicReference<List<Integer>> ok = new AtomicReference<>();
        AtomicReference<Throwable> err = new AtomicReference<>();

        gr.beginRun(0, 1, new ArrayList<>(), ok::set, err::set);
        int epoch = gr.currentEpoch();

        // slot 1 is out-of-range for size=1; ignored
        gr.recordSuccess(epoch, 1, Collections.singletonList(9));
        // valid slot 0
        gr.recordSuccess(epoch, 0, Collections.singletonList(7));

        assertNull(err.get());
        assertEquals(Collections.singletonList(7), ok.get());
    }
    @Test
    @DisplayName("onSuccess fires exactly once despite duplicate completions")
    void onSuccessFiresOnce() {
        GenerationResult<List<Integer>, Integer, String> gr =
                new GenerationResult<>(LIST_APPEND, 2, 1);
        List<Integer> initial = new ArrayList<>();
        var count = new java.util.concurrent.atomic.AtomicInteger();
        AtomicReference<Throwable> err = new AtomicReference<>();

        gr.beginRun(0, 1, initial, acc -> { count.incrementAndGet(); }, err::set);
        int epoch = gr.currentEpoch();

        gr.recordSuccess(epoch, 0, List.of(1));
        gr.recordSuccess(epoch, 0, List.of(2)); // duplicate
        assertNull(err.get());
        assertEquals(1, count.get(), "onSuccess must be called exactly once");
    }

    @Test
    @DisplayName("multiple outputs per slot are folded in slot order")
    void multipleOutputsPerSlot() {
        GenerationResult<List<Integer>, Integer, String> gr =
                new GenerationResult<>(LIST_APPEND, 2, 4);
        List<Integer> acc0 = new ArrayList<>();
        AtomicReference<List<Integer>> ok = new AtomicReference<>();
        AtomicReference<Throwable> err = new AtomicReference<>();

        gr.beginRun(0, 2, acc0, ok::set, err::set);
        int epoch = gr.currentEpoch();

        gr.recordSuccess(epoch, 0, List.of(1,2,3));
        gr.recordSuccess(epoch, 1, List.of(4,5));
        assertNull(err.get());
        assertEquals(List.of(1,2,3,4,5), ok.get());
    }

    @Test
    @DisplayName("successFromBuffer is idempotent for a slot")
    void successFromBufferIdempotent() {
        GenerationResult<List<Integer>, Integer, String> gr =
                new GenerationResult<>(LIST_APPEND, 1, 2);
        List<Integer> acc0 = new ArrayList<>();
        AtomicReference<List<Integer>> ok = new AtomicReference<>();
        AtomicReference<Throwable> err = new AtomicReference<>();

        gr.beginRun(0, 1, acc0, ok::set, err::set);
        gr.slotBuffer(0).addAll(List.of(7,8));
        gr.successFromBuffer(0);
        gr.successFromBuffer(0); // duplicate
        assertNull(err.get());
        assertEquals(List.of(7,8), ok.get());
    }

    @Test
    @DisplayName("null/empty success payload completes slot without adding")
    void nullOrEmptySuccessIsNoopButCompletes() {
        GenerationResult<List<Integer>, Integer, String> gr =
                new GenerationResult<>(LIST_APPEND, 2, 1);
        List<Integer> acc0 = new ArrayList<>();
        AtomicReference<List<Integer>> ok = new AtomicReference<>();
        AtomicReference<Throwable> err = new AtomicReference<>();

        gr.beginRun(0, 2, acc0, ok::set, err::set);
        int epoch = gr.currentEpoch();

        gr.recordSuccess(epoch, 0, null);
        gr.recordSuccess(epoch, 1, Collections.emptyList());
        assertNull(err.get());
        assertEquals(Collections.emptyList(), ok.get());
    }

    @Test
    @DisplayName("first completion wins: success then failure is ignored; failure then success is ignored")
    void firstCompletionWinsPerSlot() {
        GenerationResult<List<Integer>, Integer, String> gr =
                new GenerationResult<>(LIST_APPEND, 2, 1);
        List<Integer> acc0 = new ArrayList<>();
        AtomicReference<List<Integer>> ok = new AtomicReference<>();
        AtomicReference<Throwable> err = new AtomicReference<>();

        // success then failure
        gr.beginRun(0, 1, acc0, ok::set, err::set);
        int e1 = gr.currentEpoch();
        gr.recordSuccess(e1, 0, List.of(1));
        gr.recordFailure(e1, 0, "X", new RuntimeException("should-ignore"));
        assertNull(err.get());
        assertEquals(List.of(1), ok.get());

        // failure then success
        ok.set(null); err.set(null);
        gr.beginRun(1, 1, new ArrayList<>(), ok::set, err::set);
        int e2 = gr.currentEpoch();
        gr.recordFailure(e2, 0, "X", new RuntimeException("boom"));
        gr.recordSuccess(e2, 0, List.of(2));
        assertNull(ok.get());
        assertTrue(err.get() instanceof EnrichmentBatchException);
    }

    @Test
    @DisplayName("epoch guard drops late callback for the SAME slot")
    void epochGuardSameSlot() {
        GenerationResult<List<Integer>, Integer, String> gr =
                new GenerationResult<>(LIST_APPEND, 1, 1);
        AtomicReference<List<Integer>> ok = new AtomicReference<>();
        AtomicReference<Throwable> err = new AtomicReference<>();

        gr.beginRun(5, 1, new ArrayList<>(), ok::set, err::set);
        int oldEpoch = gr.currentEpoch();
        // start next run
        gr.beginRun(6, 1, new ArrayList<>(), ok::set, err::set);
        int newEpoch = gr.currentEpoch();

        // late success from old run for slot 0 -> ignored
        gr.recordSuccess(oldEpoch, 0, List.of(999));
        // valid new run success
        gr.recordSuccess(newEpoch, 0, List.of(1));

        assertNull(err.get());
        assertEquals(List.of(1), ok.get());
    }

    @Test
    @DisplayName("slotBuffer bounds throw IndexOutOfBoundsException")
    void slotBufferBounds() {
        GenerationResult<List<Integer>, Integer, String> gr =
                new GenerationResult<>(LIST_APPEND, 2, 1);
        assertThrows(IndexOutOfBoundsException.class, () -> gr.slotBuffer(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> gr.slotBuffer(2));
    }

    @Test
    @DisplayName("state resets across runs (buffers cleared)")
    void buffersClearedBetweenRuns() {
        GenerationResult<List<Integer>, Integer, String> gr =
                new GenerationResult<>(LIST_APPEND, 2, 4);
        AtomicReference<List<Integer>> ok = new AtomicReference<>();
        AtomicReference<Throwable> err = new AtomicReference<>();

        gr.beginRun(0, 2, new ArrayList<>(), ok::set, err::set);
        int e0 = gr.currentEpoch();
        gr.recordSuccess(e0, 0, List.of(1,2));
        gr.recordSuccess(e0, 1, List.of(3));
        assertEquals(List.of(1,2,3), ok.get());

        ok.set(null); err.set(null);
        gr.beginRun(1, 2, new ArrayList<>(), ok::set, err::set);
        int e1 = gr.currentEpoch();
        // if buffers weren’t cleared, we’d see 1,2,3 again
        gr.recordSuccess(e1, 0, List.of(4));
        gr.recordSuccess(e1, 1, List.of(5));
        assertNull(err.get());
        assertEquals(List.of(4,5), ok.get());
    }

}
