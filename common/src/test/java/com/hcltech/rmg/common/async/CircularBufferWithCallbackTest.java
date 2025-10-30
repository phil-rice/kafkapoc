package com.hcltech.rmg.common.async;// CircularBufferWithCallbackTest.java
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class CircularBufferWithCallbackTest {

    // Helper to capture drained values and any failures from onFailure
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
    public void inOrderDrainsImmediately() {
        Captures<Integer> cap = new Captures<>();
        CircularBufferWithCallback<Integer> buf = mk(8, cap);

        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, buf.put(0, 10));
        assertEquals(List.of(10), cap.drained);
        assertEquals(1L, buf.baseSeq());
        assertEquals(0, buf.size());

        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, buf.put(1, 11));
        assertEquals(List.of(10, 11), cap.drained);
        assertEquals(2L, buf.baseSeq());
        assertEquals(0, buf.size());
    }

    @Test
    public void outOfOrderWaitsThenDrains() {
        Captures<Integer> cap = new Captures<>();
        CircularBufferWithCallback<Integer> buf = mk(8, cap);

        // base=0, insert seq=2 first (gap at 0 and 1)
        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, buf.put(2, 102));
        assertTrue(cap.drained.isEmpty());
        assertEquals(1, buf.size());
        assertEquals(0L, buf.baseSeq());

        // fill seq=1 (still waiting for seq=0)
        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, buf.put(1, 101));
        assertTrue(cap.drained.isEmpty());
        assertEquals(2, buf.size());

        // now seq=0 arrives, should drain 0,1,2
        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, buf.put(0, 100));
        assertEquals(List.of(100, 101, 102), cap.drained);
        assertEquals(0, buf.size());
        assertEquals(3L, buf.baseSeq());
    }

    @Test
    public void rejectOutOfWindow() {
        Captures<Integer> cap = new Captures<>();
        CircularBufferWithCallback<Integer> buf = mk(4, cap); // capacity 4 => window [base .. base+3]

        // Place seq=2 and 3 (leave 0..1 missing) so base stays 0 and window is [0..3]
        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, buf.put(2, 12));
        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, buf.put(3, 13));
        assertEquals(2, buf.size());

        // seq=4 is out of window (diff=4)
        assertEquals(CircularBufferWithCallback.PutResult.REJECTED_WINDOW_FULL, buf.put(4, 14));
        assertTrue(cap.drained.isEmpty());
        assertEquals(2, buf.size());
    }

    @Test
    public void finishDropsLate() {
        Captures<Integer> cap = new Captures<>();
        CircularBufferWithCallback<Integer> buf = mk(8, cap);

        buf.finish();
        assertEquals(CircularBufferWithCallback.PutResult.DROPPED_AFTER_FINISH, buf.put(0, 1));
        assertTrue(cap.drained.isEmpty());
    }

    @Test
    public void duplicateInsertThrows() {
        Captures<Integer> cap = new Captures<>();
        CircularBufferWithCallback<Integer> buf = mk(8, cap);

        // Make a gap so seq=1 stays buffered and doesn't drain
        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, buf.put(1, 11));
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> buf.put(1, 11));
        assertTrue(ex.getMessage().contains("Duplicate insert"));
    }

    @Test
    public void canAddWindowLogic() {
        Captures<Integer> cap = new Captures<>();
        CircularBufferWithCallback<Integer> buf = mk(4, cap); // window [0..3]

        assertTrue(buf.canAdd(0));
        assertTrue(buf.canAdd(3));
        assertFalse(buf.canAdd(4));

        buf.put(2, 2);
        buf.put(3, 3);
        assertFalse(buf.canAdd(4));
    }

    @Test
    public void wraparoundUnsignedOrdering() throws Exception {
        Captures<Long> cap = new Captures<>();
        CircularBufferWithCallback<Long> buf = mk(8, cap);

        // Reflectively set baseSeq near unsigned wrap to 2^64-3 (i.e., -3L)
        Field baseSeq = CircularBufferWithCallback.class.getDeclaredField("baseSeq");
        baseSeq.setAccessible(true);
        baseSeq.setLong(buf, -3L);

        Field head = CircularBufferWithCallback.class.getDeclaredField("head");
        head.setAccessible(true);
        head.setInt(buf, 0);

        // Insert ahead-of-base (within window) out of order: -1 and 0
        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, buf.put(-1L, -1L)); // distance 2
        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, buf.put(0L,  0L));  // distance 3

        assertEquals(2, buf.size());
        assertTrue(cap.drained.isEmpty());

        // Close the gaps at base: -3 then -2
        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, buf.put(-3L, -3L));
        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, buf.put(-2L, -2L));

        assertEquals(List.of(-3L, -2L, -1L, 0L), cap.drained);
        assertEquals(0, buf.size());
    }

    @Test
    public void onSuccessThrowsCallsOnFailureAndContinues() {
        // onSuccess will throw for a sentinel value; we verify onFailure is invoked and draining continues
        Captures<Integer> cap = new Captures<>();
        CircularBufferWithCallback<Integer> buf = new CircularBufferWithCallback<>(
                8,
                v -> {
                    if (v == -999) throw new RuntimeException("boom-tag");
                    cap.drained.add(v);
                },
                (v, ex) -> cap.failures.add(ex)
        );

        buf.put(0, 1);
        buf.put(1, -999); // will throw inside success
        buf.put(2, 3);

        // drain already invoked in put
        assertEquals(List.of(1, 3), cap.drained); // the -999 is not added to drained
        assertEquals(1, cap.failures.size());
        assertTrue(cap.failures.get(0).getMessage().contains("boom-tag"));
    }
    @Test
    public void isFullReflectsSize() {
        Captures<Integer> cap = new Captures<>();
        CircularBufferWithCallback<Integer> buf = mk(2, cap);

        // Initially not full
        assertFalse(buf.isFull());
        assertEquals(0, buf.size());

        // Insert only the ahead slot; not full (size == 1, capacity == 2)
        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, buf.put(1, 11));
        assertFalse(buf.isFull());
        assertEquals(1, buf.size());

        // Fill the gap; drain runs and frees everything
        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, buf.put(0, 10));
        assertFalse(buf.isFull());
        assertEquals(0, buf.size());

        // Base now 2; insert only seq=3 (leave 2 missing); still not full
        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, buf.put(3, 23));
        assertFalse(buf.isFull());
        assertEquals(1, buf.size());
    }

    @Test
    public void resetClears() {
        Captures<Integer> cap = new Captures<>();
        CircularBufferWithCallback<Integer> buf = mk(8, cap);

        buf.put(1, 11);
        assertEquals(1, buf.size());
        buf.reset();
        assertEquals(0, buf.size());
        assertEquals(0L, buf.baseSeq());
        assertFalse(buf.isFinished());

        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, buf.put(0, 1));
        assertEquals(List.of(1), cap.drained);
    }
    @Test
    public void put_farAheadBeyond2Pow31_isRejectedWindowFull() {
        // This should FAIL with the current (int) cast bug and PASS after the fix.
        Captures<Integer> cap = new Captures<>();
        CircularBufferWithCallback<Integer> buf = mk(4, cap); // window [0..3]

        // Prime with base=0; accept 0 so base moves to 1 and state is non-trivial
        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, buf.put(0, 10));
        assertEquals(1L, buf.baseSeq());
        assertEquals(List.of(10), cap.drained);

        // Choose a seq that's >= 2^33 ahead of current base to force 64-bit distance
        long far = buf.baseSeq() + (1L << 33);
        assertEquals(CircularBufferWithCallback.PutResult.REJECTED_WINDOW_FULL, buf.put(far, 99));

        // Nothing else should have drained
        assertEquals(List.of(10), cap.drained);
        assertEquals(1L, buf.baseSeq());
    }

    @Test
    public void canAdd_farAheadBeyond2Pow31_isFalse() {
        // This mirrors the window check for canAdd
        Captures<Integer> cap = new Captures<>();
        CircularBufferWithCallback<Integer> buf = mk(8, cap);

        long base = buf.baseSeq();           // 0
        long far  = base + (1L << 33);       // way outside window
        assertFalse(buf.canAdd(far));
        assertTrue(buf.canAdd(base));        // sanity
        assertTrue(buf.canAdd(base + 7));    // last in window for cap=8
    }

    @Test
    public void alreadyCommittedDropped_whenSeqLessThanBase() {
        Captures<Integer> cap = new Captures<>();
        CircularBufferWithCallback<Integer> buf = mk(8, cap);

        // Drain a few in order so base advances to 3
        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, buf.put(0, 10));
        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, buf.put(1, 11));
        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, buf.put(2, 12));
        assertEquals(3L, buf.baseSeq());

        // Putting an already-committed sequence is dropped (unsigned less-than base)
        assertEquals(CircularBufferWithCallback.PutResult.ALREADY_COMMITTED_DROPPED, buf.put(1, 111));
        assertEquals(List.of(10, 11, 12), cap.drained);
        assertEquals(3L, buf.baseSeq());
        assertEquals(0, buf.size());
    }

    @Test
    public void onFailureThrows_isSwallowedAndDrainContinues() {
        // Ensure onFailure exceptions are swallowed and do not block draining subsequent items
        List<Integer> drained = new ArrayList<>();
        CircularBufferWithCallback<Integer> buf = new CircularBufferWithCallback<>(
                8,
                drained::add,
                (v, ex) -> { throw new RuntimeException("onFailure blew up"); }
        );

        // Make the middle item trigger onSuccess exception
        buf = new CircularBufferWithCallback<>(
                8,
                v -> {
                    if (v == 2) throw new RuntimeException("boom");
                    drained.add(v);
                },
                (v, ex) -> { throw new RuntimeException("onFailure blew up"); }
        );

        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, buf.put(0, 0));
        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, buf.put(1, 2)); // will throw in success
        assertEquals(CircularBufferWithCallback.PutResult.ACCEPTED, buf.put(2, 4));

        // We still expect 0 and 4 to drain; onFailure's own throw is swallowed
        assertEquals(List.of(0, 4), drained);
        assertEquals(3L, buf.baseSeq());
        assertEquals(0, buf.size());
    }

}
