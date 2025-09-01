// src/test/java/com/example/cepstate/worklease/MemoryWorkLeaseInvariantTest.java
package com.hcltech.rmg.cepstate.worklease;

import com.hcltech.rmg.common.ITimeService;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

final class MemoryWorkLeaseInvariantTest {

    // --- simple fakes ---
    static final class FakeTime implements ITimeService {
        private long now;
        FakeTime(long start) { this.now = start; }
        @Override public long currentTimeMillis() { return now; }
        void advance(long ms) { now += ms; }
    }

    static final class DeterministicTokenGenerator implements ITokenGenerator {
        private long seq = 0;
        @Override public String next(String domainId, long offset) {
            return domainId + ":" + offset + "#" + (seq++);
        }
    }

    private MemoryWorkLease newWL(int maxRetries) {
        return new MemoryWorkLease(new DeterministicTokenGenerator(), new FakeTime(1_000_000L), maxRetries);
    }

    // 1) succeed with empty backlog → NO_BACKLOG
    @Test
    void succeed_with_no_backlog_returns_noBacklog() {
        var wl = newWL(3);
        var a1 = wl.tryAcquire("A", 100L);
        var s1 = wl.succeed("A", a1.token());

        assertEquals("lease.succeed.noBacklog", s1.reason());
        assertFalse(s1.hasNextOffset());
    }

    // 2) final give-up with empty backlog → GIVE_UP_NO_BACKLOG
    @Test
    void giveUp_with_no_backlog_clears_lease_and_returns_noBacklog() {
        var wl = newWL(0); // immediate give-up
        var a1 = wl.tryAcquire("A", 200L);

        var f = wl.fail("A", a1.token());
        assertEquals("lease.fail.giveUp", f.reason());
        assertFalse(f.willRetry());
        assertFalse(f.hasNextOffset());

        // After clearing the lease, calling succeed/fail should throw "no lease"
        assertThrows(IllegalStateException.class, () -> wl.succeed("A", "anything"));
        assertThrows(IllegalStateException.class, () -> wl.fail("A", "anything"));
    }

    // 3) backlog dedupe: enqueue same offset twice → processed once
    @Test
    void duplicate_enqueue_is_ignored_only_one_processing_of_that_offset() {
        var wl = newWL(3);

        // Acquire 300; enqueue 301 twice
        var a1 = wl.tryAcquire("A", 300L);
        wl.tryAcquire("A", 301L);
        wl.tryAcquire("A", 301L); // duplicate enqueue should be ignored

        // Finish 300 -> hint 301
        var s1 = wl.succeed("A", a1.token());
        assertEquals("lease.succeed.nextBacklog", s1.reason());
        assertEquals(301L, s1.nextOffset());

        // Acquire 301 and finish it
        var a2 = wl.tryAcquire("A", 301L);
        assertTrue(a2.isAcquired());
        var s2 = wl.succeed("A", a2.token());

        // If dedupe failed we'd see nextBacklog(301) again; instead we expect no backlog
        assertEquals("lease.succeed.noBacklog", s2.reason());
        assertFalse(s2.hasNextOffset());
    }

    // 4) succeed/fail without an active lease → throws
    @Test
    void succeed_without_active_lease_throws_and_fail_without_active_lease_throws() {
        var wl = newWL(3);
        assertThrows(IllegalStateException.class, () -> wl.succeed("A", "tok"));
        assertThrows(IllegalStateException.class, () -> wl.fail("A", "tok"));
    }

    // 5) retry count increments monotonically until limit, then give up
    @Test
    void retry_count_increments_until_limit_then_gives_up() {
        var wl = newWL(2); // allow two retries; third fail gives up

        var a1 = wl.tryAcquire("A", 400L);
        var t = a1.token();

        var f1 = wl.fail("A", t);
        assertEquals("lease.fail.retryScheduled", f1.reason());
        assertTrue(f1.willRetry());
        assertEquals(1, f1.retryCount());

        var f2 = wl.fail("A", t);
        assertEquals("lease.fail.retryScheduled", f2.reason());
        assertTrue(f2.willRetry());
        assertEquals(2, f2.retryCount());

        var f3 = wl.fail("A", t);
        assertTrue(f3.reason().startsWith("lease.fail.giveUp"));
        assertFalse(f3.willRetry());
    }

    // 6) terminal items (processed or failed) are not re-enqueued or re-acquired
    @Test
    void processed_or_failed_offsets_are_terminal() {
        var wl = newWL(0); // final-fail immediately for the second offset

        // Success path → processed
        var a1 = wl.tryAcquire("A", 500L);
        wl.succeed("A", a1.token());
        var againProcessed = wl.tryAcquire("A", 500L);
        assertEquals("lease.itemAlreadyProcessed", againProcessed.reason());

        // Final fail path → failed (terminal)
        var a2 = wl.tryAcquire("A", 501L);
        wl.fail("A", a2.token());
        var againFailed = wl.tryAcquire("A", 501L);
        assertEquals("lease.itemAlreadyFailed", againFailed.reason());
    }

    // 7) ordering error includes context (domain/backlog) and throws
    @Test
    void ordering_error_message_includes_domain_and_backlog() {
        var wl = newWL(3);

        var a1 = wl.tryAcquire("A", 600L);
        wl.tryAcquire("A", 601L); // backlog head = 601
        wl.succeed("A", a1.token()); // lease cleared, backlog intact

        var ex = assertThrows(IllegalStateException.class, () -> wl.tryAcquire("A", 602L));
        assertTrue(ex.getMessage().contains("Backlog ordering exception"), "message should mention ordering");
        assertTrue(ex.getMessage().contains("A"), "message should include domain id");
        assertTrue(ex.getMessage().contains("601"), "message should include backlog head");
    }
}
