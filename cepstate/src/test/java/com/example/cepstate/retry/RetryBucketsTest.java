package com.example.cepstate.retry;

import com.example.cepstate.metrics.IMetrics;
import com.example.kafka.common.ITimeService;
import com.example.cepstate.worklease.WorkLeaseStage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for RetryBuckets.
 */
class RetryBucketsTest {

    static final class FakeTime implements ITimeService {
        private long now;
        FakeTime(long start) { this.now = start; }
        @Override public long currentTimeMillis() { return now; }
        void set(long t) { this.now = t; }
        void advance(long d) { this.now += d; }
    }

    private FakeTime time;
    private RetryBuckets buckets;

    // --- Helpers ---

    /** Build a simple ladder with explicit timeouts (ms) and optional jitter (defaults 0). */
    private List<WorkLeaseStage> ladder(int... timeoutsMs) {
        var list = new ArrayList<WorkLeaseStage>(timeoutsMs.length);
        for (int t : timeoutsMs) list.add(stage(t, 0));
        return list;
    }

    /** Create a WorkLeaseStage with timeout + jitter. Adjust if your ctor differs. */
    private WorkLeaseStage stage(int timeoutMs, int jitterMs) {
        // If your WorkLeaseStage is a record/class with these fields, this matches:
        return new WorkLeaseStage("one", timeoutMs, jitterMs, null);
        // If instead WorkLeaseStage is an interface, replace with an anonymous impl exposing
        // retryTimeOutMs() and jitterms().
    }

    @BeforeEach
    void setUp() {
        time = new FakeTime(0);
        buckets = new RetryBuckets(
                ladder(1_000, 5_000, 60_000), // 3 attempts
                time,
                1_000,                        // 1s bucket granularity
                IMetrics.nullMetrics()
        );
    }

    @Test
    void schedulesIntoCorrectBucket_andReturnsTrue() {
        boolean ok = buckets.addToRetryBucket("topicA", "D1", 0, /*lease*/0L, /*attempt*/0);
        assertTrue(ok);

        // dueAt = 0 + 1000 + 0 = 1000, bucket floor(1000/1000)*1000 = 1000
        var internal = buckets.buckets();
        assertEquals(1, internal.size());
        assertTrue(internal.containsKey(1_000L));

        var list = internal.get(1_000L).keys();
        assertEquals(1, list.size());
        assertEquals("D1", list.getFirst().domainId());
        assertEquals("topicA", list.getFirst().topic());
        assertEquals(1_000L, list.getFirst().dueAtMs());
    }

    @Test
    void outOfLadderReturnsFalse_andDoesNotSchedule() {
        // Our ladder size is 3; retriesSoFar == 3 → give up
        boolean ok = buckets.addToRetryBucket("topicA", "D1", 0, 0L, 3);
        assertFalse(ok);
        assertTrue(buckets.buckets().isEmpty());
    }

    @Test
    void rescheduleMovesEarlier_noDuplicates() {
        // First schedule at attempt #1 (5s timeout), dueAt=5000 → bucket 5000
        assertTrue(buckets.addToRetryBucket("topicA", "D1", 0, 0L, 1));
        assertTrue(buckets.buckets().containsKey(5_000L));
        assertEquals(1, buckets.buckets().get(5_000L).keys().size());

        // Reschedule same (topic,domain) at attempt #0 (1s timeout), earlier than previous
        assertTrue(buckets.addToRetryBucket("topicA", "D1", 0, 0L, 0));

        // Should have moved from 5000 to 1000, with no duplicates
        assertFalse(buckets.buckets().containsKey(5_000L));
        assertTrue(buckets.buckets().containsKey(1_000L));
        assertEquals(1, buckets.buckets().get(1_000L).keys().size());

        var k = buckets.buckets().get(1_000L).keys().getFirst();
        assertEquals("topicA", k.topic());
        assertEquals("D1", k.domainId());
        assertEquals(1_000L, k.dueAtMs()); // exact due time preserved
    }

    @Test
    void rescheduleLater_keepsEarlierSchedule() {
        // First: earlier at 1s
        assertTrue(buckets.addToRetryBucket("topicA", "D1", 0, 0L, 0));
        // Then: later at 5s — should NO-OP (keep earlier)
        assertTrue(buckets.addToRetryBucket("topicA", "D1", 0, 0L, 1));

        assertTrue(buckets.buckets().containsKey(1_000L));
        assertFalse(buckets.buckets().containsKey(5_000L));
        assertEquals(1, buckets.buckets().get(1_000L).keys().size());
    }

    @Test
    void drainReturnsDue_inInsertionOrder_withinBucket() {
        // Place three entries that all land in the same bucket (quantized to 1000)
        // due times: 100, 200, 900  -> all bucket 0
        buckets = new RetryBuckets(
                List.of(stage(100, 0), stage(200, 0), stage(900, 0)),
                time,
                1_000,
                IMetrics.nullMetrics()
        );

        assertTrue(buckets.addToRetryBucket("T", "A", 0, 0, 0)); // dueAt=100 -> bucket 0
        assertTrue(buckets.addToRetryBucket("T", "B", 0, 0, 1)); // dueAt=200 -> bucket 0
        assertTrue(buckets.addToRetryBucket("T", "C", 0, 0, 2)); // dueAt=900 -> bucket 0

        // Advance time to bucket boundary; head bucket is due
        time.set(1_000);

        var due = buckets.retryKeysForNow();
        assertEquals(3, due.size());
        assertEquals("A", due.get(0).domainId()); // FIFO in bucket
        assertEquals("B", due.get(1).domainId());
        assertEquals("C", due.get(2).domainId());

        // Buckets drained
        assertTrue(buckets.buckets().isEmpty());
        // Draining again returns empty
        assertTrue(buckets.retryKeysForNow().isEmpty());
    }

    @Test
    void crossTopicSameDomain_areIndependent() {
        assertTrue(buckets.addToRetryBucket("A", "D1", 0, 0L, 0));
        assertTrue(buckets.addToRetryBucket("B", "D1", 0, 0L, 0)); // different topic, same domain

        var map = buckets.buckets();
        assertEquals(1, map.size()); // both in same time bucket (1s)
        var deque = map.values().iterator().next().keys();
        assertEquals(2, deque.size());
        // Identity is (topic, domainId), so both should be present
        assertTrue(deque.stream().anyMatch(k -> k.topic().equals("A") && k.domainId().equals("D1")));
        assertTrue(deque.stream().anyMatch(k -> k.topic().equals("B") && k.domainId().equals("D1")));
    }

    @Test
    void bucketQuantization_floorsToNearestBoundary() {
        // timeout=1499, granularity=1000 → bucket 1000
        buckets = new RetryBuckets(List.of(stage(1_499, 0)), time, 1_000, IMetrics.nullMetrics());
        assertTrue(buckets.addToRetryBucket("T", "D1", 0, 0, 0));
        assertTrue(buckets.buckets().containsKey(1_000L));

        // timeout=1999 → bucket 1000 (still), dueAt preserved as 1999
        buckets = new RetryBuckets(List.of(stage(1_999, 0)), time, 1_000, IMetrics.nullMetrics());
        assertTrue(buckets.addToRetryBucket("T", "D2", 0, 0, 0));
        var rb = buckets.buckets().get(1_000L);
        assertNotNull(rb);
        assertEquals(1, rb.keys().size());
        assertEquals(1_999L, rb.keys().getFirst().dueAtMs());
    }

    @Test
    void negativeAttempt_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> buckets.addToRetryBucket("T", "D1", 0, 0, -1));
    }

    @Test
    void drainOnlyUpToNowBucket() {
        // Build ladder with two entries that quantize to bucket 0 and bucket 2000
        buckets = new RetryBuckets(
                List.of(stage(900, 0), stage(2_100, 0)),
                time,
                1_000,
                IMetrics.nullMetrics()
        );
        assertTrue(buckets.addToRetryBucket("T", "A", 0, 0, 0)); // dueAt=900 -> bucket 0
        assertTrue(buckets.addToRetryBucket("T", "B", 0, 0, 1)); // dueAt=2100 -> bucket 2000

        time.set(1_000); // nowBucket=1000, only bucket 0 is due
        var due1 = buckets.retryKeysForNow();
        assertEquals(1, due1.size());
        assertEquals("A", due1.get(0).domainId());

        // Next advance to 2000: bucket 2000 becomes due
        time.set(2_000);
        var due2 = buckets.retryKeysForNow();
        assertEquals(1, due2.size());
        assertEquals("B", due2.get(0).domainId());
    }

    @Test
    void earlierWithinSameBucket_keepsExistingDueAt() {
        // granularity 1000; both timeouts quantize to bucket 1000
        var time2 = new FakeTime(0);
        var b = new RetryBuckets(List.of(stage(1_500, 0)), time2, 1_000, IMetrics.nullMetrics());

        assertTrue(b.addToRetryBucket("T","D",0, 0, 0)); // dueAt=1500
        // schedule again with the same stage (same dueAt); expect unchanged
        assertTrue(b.addToRetryBucket("T","D",0, 0, 0));

        var rb = b.buckets().get(1_000L);
        assertEquals(1, rb.keys().size());
        assertEquals(1_500L, rb.keys().getFirst().dueAtMs());
    }

    @Test
    void jitterWithinBoundsAndBucketMatches() {
        var time2 = new FakeTime(10_000);
        var b = new RetryBuckets(List.of(stage(2_000, 500)), time2, 1_000, IMetrics.nullMetrics());
        assertTrue(b.addToRetryBucket("T","D",0, 10_000, 0));
        var entries = b.buckets().values().iterator().next().keys();
        var k = entries.getFirst();
        assertTrue(k.dueAtMs() >= 12_000 && k.dueAtMs() <= 12_500);
        assertEquals(Math.floorDiv(k.dueAtMs(), 1_000) * 1_000,
                b.buckets().keySet().iterator().next().longValue());
    }

    @Test
    void outOfLadderReturnsFalse_andIncrementsGiveUp_andDoesNotSchedule() {
        var counters = new ConcurrentHashMap<String, LongAdder>();
        IMetrics m = IMetrics.memoryMetrics(counters);

        // ladder size = 3; retriesSoFar == 3 => give up
        var b = new RetryBuckets(ladder(1_000, 5_000, 60_000), time, 1_000, m);

        boolean ok = b.addToRetryBucket("topicA", "D1", 0, 0L, 3);
        assertFalse(ok);
        assertTrue(b.buckets().isEmpty());

        // metrics
        assertEquals(1L, sum(counters, "retryBuckets.created"));
        assertEquals(1L, sum(counters, "retryBuckets.giveUp"));
        assertEquals(0L, sum(counters, "retryBuckets.added"));
        assertEquals(0L, sum(counters, "retryBuckets.removed"));
    }

    @Test
    void multipleGiveUps_incrementMetricEachTime() {
        var counters = new ConcurrentHashMap<String, LongAdder>();
        IMetrics m = IMetrics.memoryMetrics(counters);

        // ladder size = 1; any retriesSoFar >= 1 => give up
        var b = new RetryBuckets(ladder(1_000), time, 1_000, m);

        assertFalse(b.addToRetryBucket("T","D1",0, 0L, 1));
        assertFalse(b.addToRetryBucket("T","D2",0, 0L, 2));

        assertEquals(2L, sum(counters, "retryBuckets.giveUp"));
        assertTrue(b.buckets().isEmpty());
    }


    private static long sum(ConcurrentHashMap<String, LongAdder> counters, String name) {
        var a = counters.get(name);
        return a == null ? 0L : a.sum();
    }
}
