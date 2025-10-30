package com.hcltech.rmg.cepstate.worklease;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

final class MemoryWorkLeaseTest {

    @Test
    void acquireFirstToken_thenQueueSubsequent() {
        var wl = new MemoryWorkLease<String>(ITokenGenerator.incrementingGenerator());

        String t1 = wl.tryAcquire("d1", "m1");
        assertNotNull(t1, "first acquire should return token");
        assertEquals("d1-1", t1);

        String t2 = wl.tryAcquire("d1", "m2");
        assertNull(t2, "second acquire while leased should queue and return null");
    }
    @Test
    void succeed_handsOffWithNewToken_then_endWhenEmpty() {
        var wl = new MemoryWorkLease<String>(ITokenGenerator.incrementingGenerator());

        String tok = wl.tryAcquire("d1", "m1");
        assertEquals("d1-1", tok);

        // enqueue a second message
        assertNull(wl.tryAcquire("d1", "m2"));

        // succeed m1 → should hand off m2 with a NEW token
        HandBackTokenResult<String> r1 = wl.succeed("d1", tok);
        assertEquals(CompletionStatus.HANDED_OFF, r1.status());
        assertEquals("m2", r1.message());
        assertEquals("d1-2", r1.token());

        // succeed m2 → queue now empty → ENDED
        HandBackTokenResult<String> r2 = wl.succeed("d1", r1.token());
        assertEquals(CompletionStatus.ENDED, r2.status());

        // further completes -> NOOP_WRONG_TOKEN (domain still present but idle)
        HandBackTokenResult<String> r3 = wl.succeed("d1", r1.token());
        assertEquals(CompletionStatus.NOOP_WRONG_TOKEN, r3.status());
    }


    @Test
    void fail_behavesLikeSucceed_forHandoffAndEnd() {
        var wl = new MemoryWorkLease<String>(ITokenGenerator.incrementingGenerator());

        String tok = wl.tryAcquire("dX", "a1");
        assertEquals("dX-1", tok);
        assertNull(wl.tryAcquire("dX", "a2"));

        // fail a1 → hand off a2 with new token
        HandBackTokenResult<String> f1 = wl.fail("dX", tok);
        assertEquals(CompletionStatus.HANDED_OFF, f1.status());
        assertEquals("a2", f1.message());
        assertEquals("dX-2", f1.token());

        // fail a2 → end
        HandBackTokenResult<String> f2 = wl.fail("dX", f1.token());
        assertEquals(CompletionStatus.ENDED, f2.status());
    }

    @Test
    void wrongToken_isNoop_and_doesNotDequeue() {
        var wl = new MemoryWorkLease<String>(ITokenGenerator.incrementingGenerator());

        String tok = wl.tryAcquire("d2", "x1");
        assertEquals("d2-1", tok);
        assertNull(wl.tryAcquire("d2", "x2"));

        // wrong token
        HandBackTokenResult<String> bad = wl.succeed("d2", "d2-999");
        assertEquals(CompletionStatus.NOOP_WRONG_TOKEN, bad.status(), "wrong token should be a no-op");

        // correct token still works; should hand off x2 with new token
        HandBackTokenResult<String> good = wl.succeed("d2", tok);
        assertEquals(CompletionStatus.HANDED_OFF, good.status());
        assertEquals("x2", good.message());
        assertEquals("d2-2", good.token());
    }

    @Test
    void isolationAcrossDomains_preservesSeparateTokensAndQueues() {
        var wl = new MemoryWorkLease<String>(ITokenGenerator.incrementingGenerator());

        String tA1 = wl.tryAcquire("A", "A1");
        String tB1 = wl.tryAcquire("B", "B1");
        assertEquals("A-1", tA1);
        assertEquals("B-1", tB1);

        assertNull(wl.tryAcquire("A", "A2"));
        assertNull(wl.tryAcquire("B", "B2"));

        // Finish A1 → should hand off A2 with A-2; B is unaffected
        HandBackTokenResult<String> aH = wl.succeed("A", tA1);
        assertEquals(CompletionStatus.HANDED_OFF, aH.status());
        assertEquals("A-2", aH.token());
        assertEquals("A2", aH.message());

        // Finish B1 → should hand off B2 with B-2; A is unaffected
        HandBackTokenResult<String> bH = wl.succeed("B", tB1);
        assertEquals(CompletionStatus.HANDED_OFF, bH.status());
        assertEquals("B-2", bH.token());
        assertEquals("B2", bH.message());

        // End both
        assertEquals(CompletionStatus.ENDED, wl.succeed("A", aH.token()).status());
        assertEquals(CompletionStatus.ENDED, wl.succeed("B", bH.token()).status());
    }
    @Test
    void duplicateCompletionAfterHandoff_isNoopWrongToken_and_doesNotDequeueAgain() {
        var wl = new MemoryWorkLease<String>(ITokenGenerator.incrementingGenerator());

        String t1 = wl.tryAcquire("d", "m1");
        assertNull(wl.tryAcquire("d", "m2"));
        assertNull(wl.tryAcquire("d", "m3"));

        // First completion hands off m2 with new token t2
        var r1 = wl.succeed("d", t1);
        assertEquals(CompletionStatus.HANDED_OFF, r1.status());
        assertEquals("m2", r1.message());
        String t2 = r1.token();

        // Duplicate completion with old token t1 must be NOOP_WRONG_TOKEN and not dequeue m3
        var dup = wl.succeed("d", t1);
        assertEquals(CompletionStatus.NOOP_WRONG_TOKEN, dup.status());

        // Proper completion with t2 now hands off m3
        var r2 = wl.succeed("d", t2);
        assertEquals(CompletionStatus.HANDED_OFF, r2.status());
        assertEquals("m3", r2.message());
    }

    @Test
    void wrongTokenOnFail_isNoop_and_keepsQueueIntact() {
        var wl = new MemoryWorkLease<String>(ITokenGenerator.incrementingGenerator());

        String tok = wl.tryAcquire("x", "a1");
        assertNull(wl.tryAcquire("x", "a2"));

        var bad = wl.fail("x", "x-999");
        assertEquals(CompletionStatus.NOOP_WRONG_TOKEN, bad.status());

        // Correct token still hands off a2
        var ok = wl.fail("x", tok);
        assertEquals(CompletionStatus.HANDED_OFF, ok.status());
        assertEquals("a2", ok.message());
    }

    @Test
    void multipleHandoffs_preserveStrictFifo() {
        var wl = new MemoryWorkLease<String>(ITokenGenerator.incrementingGenerator());

        String t = wl.tryAcquire("d", "m1");
        for (int i = 2; i <= 5; i++) assertNull(wl.tryAcquire("d", "m" + i));

        String tok = t;
        for (int i = 2; i <= 5; i++) {
            var r = wl.succeed("d", tok);
            assertEquals(CompletionStatus.HANDED_OFF, r.status());
            assertEquals("m" + i, r.message());
            tok = r.token(); // advance token each time
        }
        // Finally ends
        var end = wl.succeed("d", tok);
        assertEquals(CompletionStatus.ENDED, end.status());
    }

    @Test
    void endedThenReacquire_startsNewEpoch_and_oldTokenIsWrong() {
        var wl = new MemoryWorkLease<String>(ITokenGenerator.incrementingGenerator());

        String t1 = wl.tryAcquire("d", "m1");
        var end = wl.succeed("d", t1); // no backlog → ENDED
        assertEquals(CompletionStatus.ENDED, end.status());

        // Old token now sees NOOP_WRONG_TOKEN (domain entry still exists with token == null)
        var noop = wl.succeed("d", t1);
        assertEquals(CompletionStatus.NOOP_WRONG_TOKEN, noop.status());

        // Reacquire starts new epoch
        String t2 = wl.tryAcquire("d", "m2");
        assertNotNull(t2);
        assertNotEquals(t1, t2);
         assertEquals("d-2", t2);
    }


    @Test
    void wrongTokenNeverShrinksQueueWhileLeased() {
        var wl = new MemoryWorkLease<String>(ITokenGenerator.incrementingGenerator());

        String t = wl.tryAcquire("d", "m1");
        assertNull(wl.tryAcquire("d", "m2"));
        assertNull(wl.tryAcquire("d", "m3"));

        // Wrong token does nothing
        var bad = wl.succeed("d", "d-999");
        assertEquals(CompletionStatus.NOOP_WRONG_TOKEN, bad.status());

        // Valid completion should still hand off m2 (not m3)
        var r = wl.succeed("d", t);
        assertEquals("m2", r.message());
    }

    @Test
    void nullArguments_throwNpe() {
        var wl = new MemoryWorkLease<String>(ITokenGenerator.incrementingGenerator());

        assertThrows(NullPointerException.class, () -> wl.tryAcquire(null, "m"));
        assertThrows(NullPointerException.class, () -> wl.tryAcquire("d", null));
        assertThrows(NullPointerException.class, () -> wl.succeed(null, "d-1"));
        assertThrows(NullPointerException.class, () -> wl.succeed("d", null));
        assertThrows(NullPointerException.class, () -> wl.fail(null, "d-1"));
        assertThrows(NullPointerException.class, () -> wl.fail("d", null));
    }

}
