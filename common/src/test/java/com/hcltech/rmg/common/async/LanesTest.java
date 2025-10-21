package com.hcltech.rmg.common.async;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

final class LanesTest {

    // ---- Test scaffolding ----------------------------------------------------

    static final class Env {
        final String id;
        final int hash; // stable lane hash we choose for tests
        Env(String id, int hash) { this.id = id; this.hash = hash; }
        @Override public String toString() { return "Env(" + id + ",hash=" + hash + ")"; }
    }

    static final class StubCorrelator implements Correlator<Env> {
        @Override public String correlationId(Env env) { return env.id.hashCode() ^ (env.hash * 31L); }
        @Override public int  laneHash(Env env)      { return env.hash; }
    }

    private static Lanes<Env> mk(int laneCount, int laneDepth) {
        return new Lanes<>(laneCount, laneDepth, new StubCorrelator());
    }

    // ---- Constructor / validation -------------------------------------------

    @Test
    void constructor_validates_powerOfTwo_and_positive() {
        assertThrows(IllegalArgumentException.class, () -> mk(0, 8));
        assertThrows(IllegalArgumentException.class, () -> mk(8, 0));
        assertThrows(IllegalArgumentException.class, () -> mk(3, 8)); // not power of two
        assertThrows(IllegalArgumentException.class, () -> mk(8, 6)); // not power of two
        assertDoesNotThrow(() -> mk(8, 4));
    }

    @Test
    void constructor_rejects_null_correlator() {
        assertThrows(NullPointerException.class, () -> new Lanes<Env>(8, 4, null));
    }

    // ---- Routing basics ------------------------------------------------------

    @Test
    void routing_uses_hash_and_mask_correctly() {
        Lanes<Env> lanes = mk(8, 4);
        ILanes<Env> api = lanes;
        ILanesTestHooks<Env> hooks = lanes;

        assertEquals(8, hooks._laneCount());
        assertEquals(7, hooks._laneMask());

        Env a = new Env("a", 0b0010_1101); // 45
        int idxA = a.hash & hooks._laneMask();
        assertSame(hooks._laneAt(idxA), api.lane(a));

        Env b = new Env("b", 0b1110_0001); // 225
        int idxB = b.hash & hooks._laneMask();
        assertSame(hooks._laneAt(idxB), api.lane(b));
        // Likely different lanes for our chosen hashes
        assertNotSame(api.lane(a), api.lane(b));
    }

    @Test
    void stability_same_env_routes_to_same_lane() {
        Lanes<Env> lanes = mk(8, 4);
        ILanes<Env> api = lanes;
        ILanesTestHooks<Env> hooks = lanes;

        Env e = new Env("steady", 0xCAFEBABE);
        ILane<Env> first = api.lane(e);
        for (int i = 0; i < 10; i++) {
            assertSame(first, api.lane(e));
        }
        int idx = e.hash & hooks._laneMask();
        assertSame(first, hooks._laneAt(idx));
    }

    // ---- Enqueue / lane behavior --------------------------------------------

    @Test
    void routed_enqueue_goes_into_exactly_one_lane() {
        Lanes<Env> lanes = mk(8, 8);
        ILanes<Env> api = lanes;
        ILanesTestHooks<Env> hooks = lanes;

        Env e1 = new Env("e1", 0x1234);
        api.lane(e1).enqueue(e1, 111L, 1_000L);

        int seen = 0;
        for (int i = 0; i < hooks._laneCount(); i++) {
            ILaneTestHooks<Env> th = (ILaneTestHooks<Env>) hooks._laneAt(i);
            if (th._containsForTest(e1)) seen++;
        }
        assertEquals(1, seen, "enqueued item should be present in exactly one lane");
    }

    @Test
    void filled_then_wrap_enqueue_and_pop_per_lane() {
        int laneCount = 8, laneDepth = 4;
        Lanes<Env> lanes = mk(laneCount, laneDepth);
        ILanes<Env> api = lanes;
        ILanesTestHooks<Env> hooks = lanes;
        int mask = hooks._laneMask();

        int targetIdx = 3;
        Env e1 = new Env("e1", targetIdx);                // idx = 3
        Env e2 = new Env("e2", targetIdx | (1 << 8));     // also idx = 3
        assertEquals(targetIdx, e1.hash & mask);
        assertEquals(targetIdx, e2.hash & mask);

        ILane<Env> lane = api.lane(e1);
        assertSame(lane, api.lane(e2));

        // Fill lane
        lane.enqueue(e1, 11L, 110L);
        lane.enqueue(e2, 22L, 220L);
        lane.enqueue(e1, 33L, 330L);
        lane.enqueue(e2, 44L, 440L);
        assertTrue(lane.isFull());

        // Pop two, then enqueue two to force wrap
        assertTrue(lane.popHead((Env t) -> {}));
        assertTrue(lane.popHead((Env t) -> {}));
        assertFalse(lane.isFull());
        lane.enqueue(e1, 55L, 550L);
        lane.enqueue(e2, 66L, 660L);
        assertTrue(lane.isFull());

        // Drain in order
        assertSame(e1, lane.headT()); assertTrue(lane.popHead((Env t) -> {}));
        assertSame(e2, lane.headT()); assertTrue(lane.popHead((Env t) -> {}));
        assertSame(e1, lane.headT()); assertTrue(lane.popHead((Env t) -> {}));
        assertSame(e2, lane.headT()); assertTrue(lane.popHead((Env t) -> {}));
        assertTrue(lane.isEmpty());
    }

    // ---- Edge cases: tiny sizes, zero/negative hashes, nulls -----------------

    @Test
    void laneCount1_depth1_mask0_and_fullCycle() {
        Lanes<Env> lanes = mk(1, 1); // mask = 0
        ILanes<Env> api = lanes;
        ILanesTestHooks<Env> hooks = lanes;
        assertEquals(1, hooks._laneCount());
        assertEquals(0, hooks._laneMask());
        assertEquals(1, hooks._laneDepth());

        Env e0 = new Env("e0", 0);
        ILane<Env> l0 = api.lane(e0);
        assertSame(hooks._laneAt(0), l0);

        // Fill, pop, repeat
        l0.enqueue(e0, 1, 1);
        assertTrue(l0.isFull());
        assertSame(e0, l0.headT());
        assertTrue(l0.popHead((Env t) -> {}));
        assertTrue(l0.isEmpty());
        assertFalse(l0.popHead((Env t) -> {}));

        // Enqueue when full throws (depth=1)
        l0.enqueue(e0, 2, 2);
        assertTrue(l0.isFull());
        assertThrows(IllegalStateException.class, () -> l0.enqueue(new Env("e1", 0), 3, 3));
    }

    @Test
    void hashZero_routesToLane0() {
        Lanes<Env> lanes = mk(8, 2); // mask 7
        ILanes<Env> api = lanes;
        ILanesTestHooks<Env> hooks = lanes;

        Env e = new Env("zero", 0);
        assertSame(hooks._laneAt(0), api.lane(e));
    }

    @Test
    void integerMinValue_routesWithMask() {
        Lanes<Env> lanes = mk(8, 2); // mask 7
        ILanes<Env> api = lanes;
        ILanesTestHooks<Env> hooks = lanes;

        Env e = new Env("min", Integer.MIN_VALUE); // 0x80000000 & 7 == 0
        int idx = Integer.MIN_VALUE & hooks._laneMask();
        assertEquals(0, idx);
        assertSame(hooks._laneAt(idx), api.lane(e));
    }

    @Test
    void negativeHashes_mapCorrectly() {
        Lanes<Env> lanes = mk(4, 2); // mask 3
        ILanes<Env> api = lanes;
        ILanesTestHooks<Env> hooks = lanes;

        int[] hashes = { -1, -2, -3, -4, -123456789 };
        for (int h : hashes) {
            Env e = new Env("n"+h, h);
            int idx = h & hooks._laneMask();
            assertSame(hooks._laneAt(idx), api.lane(e), "hash "+h+" -> idx "+idx);
        }
    }

    @Test
    void lane_null_env_throwsNpe() {
        // Ensure Lanes.lane() does: Objects.requireNonNull(t, "env");
        Lanes<Env> lanes = mk(8, 4);
        assertThrows(NullPointerException.class, () -> ((ILanes<Env>) lanes).lane(null));
    }

    // ---- Distribution sanity -------------------------------------------------

    @Test
    void distribution_smoke_over_random_hashes() {
        // Not a strict statistical test; just sanity that we hit many lanes
        int laneCount = 32, laneDepth = 2;
        Lanes<Env> lanes = mk(laneCount, laneDepth);
        ILanes<Env> api = lanes;
        ILanesTestHooks<Env> hooks = lanes;

        int[] hits = new int[laneCount];
        Random rnd = new Random(12345);
        int n = 5_000;
        for (int i = 0; i < n; i++) {
            int h = rnd.nextInt();
            Env e = new Env("x" + i, h);
            ILane<Env> l = api.lane(e);
            // Map back to index for counting
            for (int idx = 0; idx < laneCount; idx++) {
                if (hooks._laneAt(idx) == l) { hits[idx]++; break; }
            }
        }

        int nonZero = 0;
        for (int c : hits) if (c > 0) nonZero++;
        assertTrue(nonZero > laneCount / 2, "should spread across many lanes");
    }
}
