package com.hcltech.rmg.common.async;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;

final class LaneTest {

    @Test
    void constructor_validatesPowerOfTwoAndPositive() {
        assertThrows(IllegalArgumentException.class, () -> new Lane<String>(0));
        assertThrows(IllegalArgumentException.class, () -> new Lane<String>(3));   // not power of two
        assertDoesNotThrow(() -> new Lane<String>(2));
        assertDoesNotThrow(() -> new Lane<String>(8));
    }

    @Test
    void enqueue_pop_preservesOrder_andMetadata() {
        ILane<String> lane = new Lane<>(4);

        assertTrue(lane.isEmpty());
        lane.enqueue("A", 101L, 1_000L);
        lane.enqueue("B", 202L, 2_000L);

        assertFalse(lane.isEmpty());
        assertFalse(lane.isFull());

        assertEquals("A", lane.headT());
        assertEquals(101L, lane.headCorrId());
        assertEquals(1_000L, lane.headStartedAtNanos());
        assertTrue(lane.popHead((String t) -> assertEquals("A", t)));

        assertEquals("B", lane.headT());
        assertEquals(202L, lane.headCorrId());
        assertEquals(2_000L, lane.headStartedAtNanos());
        assertTrue(lane.popHead((String t) -> assertEquals("B", t)));

        assertTrue(lane.isEmpty());
        assertFalse(lane.popHead((String t) -> {})); // safe no-op
    }

    @Test
    void biConsumer_popHead_passesContextCorrectly() {
        ILane<String> lane = new Lane<>(2);
        lane.enqueue("A", 11L, 100L);
        lane.enqueue("B", 22L, 200L);

        String context = "CTX-1";
        BiConsumer<String,String> consumer = (t, ctx) -> {
            assertEquals("A", t);
            assertEquals("CTX-1", ctx);
        };

        assertTrue(lane.popHead(context, consumer));

        // next item
        BiConsumer<String,String> consumer2 = (t, ctx) -> {
            assertEquals("B", t);
            assertEquals("CTX-2", ctx);
        };
        assertTrue(lane.popHead("CTX-2", consumer2));

        assertTrue(lane.isEmpty());
    }

    @Test
    void full_and_empty_invariants_and_enqueue_when_full_throws() {
        ILane<Integer> lane = new Lane<>(2);

        assertTrue(lane.isEmpty());
        lane.enqueue(1, 11L, 111L);
        lane.enqueue(2, 22L, 222L);
        assertTrue(lane.isFull());

        // Enqueuing when full should throw (logic bug)
        assertThrows(IllegalStateException.class, () -> lane.enqueue(3, 33L, 333L));

        assertTrue(lane.popHead((Integer t) -> {}));
        assertTrue(lane.popHead((Integer t) -> {}));
        assertTrue(lane.isEmpty());
        assertFalse(lane.isFull());
    }

    @Test
    void wraparound_with_mask_preserves_order() {
        ILane<Integer> lane = new Lane<>(4); // mask = 3
        ILaneTestHooks<Integer> hooks = (ILaneTestHooks<Integer>) lane;
        assertEquals(3, hooks._maskForTest());

        // Fill ring
        lane.enqueue(10, 10L, 100L);
        lane.enqueue(11, 11L, 110L);
        lane.enqueue(12, 12L, 120L);
        lane.enqueue(13, 13L, 130L);
        assertTrue(lane.isFull());

        // Pop two (headIdx moves forward)
        assertTrue(lane.popHead((Integer t) -> {}));
        assertTrue(lane.popHead((Integer t) -> {}));

        // Enqueue two more -> should wrap to slots 0 and 1
        lane.enqueue(14, 14L, 140L);
        lane.enqueue(15, 15L, 150L);
        assertTrue(lane.isFull());

        // Verify order still correct
        assertEquals(12, lane.headT()); assertTrue(lane.popHead((Integer t) -> {}));
        assertEquals(13, lane.headT()); assertTrue(lane.popHead((Integer t) -> {}));
        assertEquals(14, lane.headT()); assertTrue(lane.popHead((Integer t) -> {}));
        assertEquals(15, lane.headT()); assertTrue(lane.popHead((Integer t) -> {}));
        assertTrue(lane.isEmpty());
    }

    @Test
    void stress_small_ring_order_and_no_leaks() {
        final int depth = 8;
        ILane<Long> lane = new Lane<>(depth);
        ILaneTestHooks<Long> hooks = (ILaneTestHooks<Long>) lane;
        final int rounds = 10_000;
        AtomicLong nextId = new AtomicLong(0);

        for (int i = 0; i < rounds; i++) {
            if (lane.isFull()) assertTrue(lane.popHead((Long t) -> {}));
            long id = nextId.getAndIncrement();
            lane.enqueue(id, id * 10, id * 100);
            if ((i & 3) == 0) {
                while (!lane.isEmpty() && (hooks._countForTest() > depth / 2)) {
                    assertEquals(lane.headCorrId() / 10, lane.headT());
                    assertTrue(lane.popHead((Long t) -> {}));
                }
            }
        }
        while (!lane.isEmpty()) {
            assertEquals(lane.headCorrId() / 10, lane.headT());
            assertTrue(lane.popHead((Long t) -> {}));
        }
        assertEquals(0, hooks._countForTest());
        assertTrue(lane.isEmpty());
    }

    @Test
    void depthOne_ring_full_empty_cycle() {
        ILane<Integer> lane = new Lane<>(1);
        assertTrue(lane.isEmpty());
        lane.enqueue(42, 420L, 4200L);
        assertTrue(lane.isFull());
        assertEquals(42, lane.headT());
        assertTrue(lane.popHead((Integer t) -> {}));
        assertTrue(lane.isEmpty());
        assertFalse(lane.popHead((Integer t) -> {}));
    }

    @Test
    void enqueue_null_throws() {
        ILane<Object> lane = new Lane<>(2);
        assertThrows(NullPointerException.class, () -> lane.enqueue(null, 1L, 1L));
    }

    @Test
    void popHead_with_context_handlesNullContextGracefully() {
        ILane<String> lane = new Lane<>(2);
        lane.enqueue("A", 1L, 1L);
        assertTrue(lane.popHead(null, (String t, String ctx) -> {
            assertNull(ctx);
            assertEquals("A", t);
        }));
        assertTrue(lane.isEmpty());
    }
}
