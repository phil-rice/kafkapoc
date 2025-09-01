package com.hcltech.rmg.cepstate.worklease;

import com.hcltech.rmg.common.ITimeService;
import com.hcltech.rmg.cepstate.ProcessMessageContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MemoryWorkLeaseTest {

    // ---- fakes ----

    static class FakeTime implements ITimeService {
        private long now;
        FakeTime(long start) { this.now = start; }
        @Override public long currentTimeMillis() { return now; }
        void advance(long ms) { now += ms; }
    }

    static class DeterministicTokenGenerator implements ITokenGenerator {
        private long seq = 0;
        @Override public String next(String domainId, long offset) {
            return domainId + ":" + offset + "#" + (seq++);
        }
    }

    // ---- shared fixtures ----

    private FakeTime time;
    private DeterministicTokenGenerator tokens;

    @BeforeEach
    void setup() {
        time = new FakeTime(1_000_000L);
        tokens = new DeterministicTokenGenerator();
    }

    private ProcessMessageContext<Object, String, String> newCtx(int maxRetries) {
        var lease = new MemoryWorkLease(tokens, time, maxRetries);
        // We don’t use CepState in these tests, so pass null. The test helper sets buckets=null too.
        return ProcessMessageContext.test(
                "orders-topic",
                "INIT",
                /* cepState */ null,
                lease,
                time,
                m -> "domain-1"
        );
    }

    private MemoryWorkLease wl(ProcessMessageContext<?, ?, ?> ctx) {
        return (MemoryWorkLease) ctx.workLease();
    }

    // ---- tests ----

    @Test
    void acquire_firstTime_grantsLease_and_token_stable_for_same_offset() {
        var ctx = newCtx(3);
        var lease = wl(ctx);

        var r1 = lease.tryAcquire("A", 10L);
        assertTrue(r1.isAcquired());
        assertEquals("lease.acquired.firstTime", r1.reason());
        assertNotNull(r1.token());
        var token = r1.token();

        var r2 = lease.tryAcquire("A", 10L);
        assertEquals("lease.alreadyAcquired", r2.reason());
        assertEquals(token, r2.token());
    }

    @Test
    void acquire_while_other_offset_in_flight_enqueues() {
        var ctx = newCtx(3);
        var lease = wl(ctx);

        var r1 = lease.tryAcquire("A", 10L);
        assertTrue(r1.isAcquired());

        var r2 = lease.tryAcquire("A", 11L);
        assertFalse(r2.isAcquired());
        assertEquals("lease.addToBacklog", r2.reason());
        assertNull(r2.token());
    }

    @Test
    void succeed_clears_lease_and_returns_next_hint_then_tryAcquire_pops_head_and_acquires() {
        var ctx = newCtx(3);
        var lease = wl(ctx);

        var a1 = lease.tryAcquire("A", 10L);
        var token1 = a1.token();

        // enqueue next
        var enq = lease.tryAcquire("A", 11L);
        assertEquals("lease.addToBacklog", enq.reason());

        // succeed current; get hint to next
        var s1 = lease.succeed("A", token1);
        assertEquals("lease.succeed.nextBacklog", s1.reason());
        assertTrue(s1.hasNextOffset());
        assertEquals(11L, s1.nextOffset());

        // orchestrator acquires hinted head; backlog head must match and be popped
        var a2 = lease.tryAcquire("A", 11L);
        assertTrue(a2.isAcquired());
        assertEquals("lease.acquired.afterFirstTime", a2.reason());
        assertNotNull(a2.token());
    }

    @Test
    void tryAcquire_out_of_order_when_backlog_non_empty_throws() {
        var ctx = newCtx(3);
        var lease = wl(ctx);

        var a1 = lease.tryAcquire("A", 10L);
        var token1 = a1.token();

        // backlog head becomes 12
        var enq = lease.tryAcquire("A", 12L);
        assertEquals("lease.addToBacklog", enq.reason());

        // clear lease and hint 12
        var s1 = lease.succeed("A", token1);
        assertEquals("lease.succeed.nextBacklog", s1.reason());
        assertEquals(12L, s1.nextOffset());

        // out-of-order acquire: not the head -> throws
        assertThrows(IllegalStateException.class, () -> lease.tryAcquire("A", 11L));

        // correct head works
        var a2 = lease.tryAcquire("A", 12L);
        assertTrue(a2.isAcquired());
    }

    @Test
    void fail_under_limit_increments_retry_and_keeps_lease() {
        var ctx = newCtx(3);
        var lease = wl(ctx);

        var a1 = lease.tryAcquire("A", 20L);
        var f1 = lease.fail("A", a1.token());

        assertEquals("lease.fail.retryScheduled", f1.reason());
        assertTrue(f1.willRetry());
        assertEquals(1, f1.retryCount());

        // Same offset is still in-flight; re-entrant acquire returns same token
        var again = lease.tryAcquire("A", 20L);
        assertEquals("lease.alreadyAcquired", again.reason());
        assertEquals(a1.token(), again.token());
    }

    @Test
    void fail_at_limit_marks_failed_clears_lease_and_returns_next_hint() {
        var ctx = newCtx(1); // allow one retry, then give up
        var lease = wl(ctx);

        var a1 = lease.tryAcquire("A", 30L);
        var token = a1.token();

        // enqueue next
        lease.tryAcquire("A", 31L);

        // first failure -> scheduled
        var f1 = lease.fail("A", token);
        assertEquals("lease.fail.retryScheduled", f1.reason());
        assertTrue(f1.willRetry());
        assertEquals(1, f1.retryCount());

        // second failure -> give up & hint next
        var f2 = lease.fail("A", token);
        assertTrue(f2.reason().startsWith("lease.fail.giveUp"));
        assertFalse(f2.willRetry());
        assertTrue(f2.hasNextOffset());
        assertEquals(31L, f2.nextOffset());

        // orchestrator acquires hinted head
        var a2 = lease.tryAcquire("A", 31L);
        assertTrue(a2.isAcquired());
    }

    @Test
    void succeed_with_wrong_token_throws() {
        var ctx = newCtx(3);
        var lease = wl(ctx);

        lease.tryAcquire("A", 40L);
        assertThrows(IllegalStateException.class, () -> lease.succeed("A", "wrong"));
    }

    @Test
    void fail_with_wrong_token_throws() {
        var ctx = newCtx(3);
        var lease = wl(ctx);

        lease.tryAcquire("A", 50L);
        assertThrows(IllegalStateException.class, () -> lease.fail("A", "bad"));
    }

    @Test
    void idempotence_after_success_and_after_final_fail() {
        var ctx = newCtx(0); // immediate give-up
        var lease = wl(ctx);

        var a1 = lease.tryAcquire("A", 60L);
        lease.succeed("A", a1.token());
        var again1 = lease.tryAcquire("A", 60L);
        assertEquals("lease.itemAlreadyProcessed", again1.reason());

        var a2 = lease.tryAcquire("A", 61L);
        lease.fail("A", a2.token()); // maxRetries=0 => give up immediately
        var again2 = lease.tryAcquire("A", 61L);
        assertEquals("lease.itemAlreadyFailed", again2.reason());
    }


}
