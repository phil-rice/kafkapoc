package com.example.cepstate.retry;

import com.example.cepstate.metrics.IMetrics;
import com.example.kafka.common.ITimeService;
import com.example.cepstate.worklease.WorkLeaseStage;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;

public final class RetryBuckets implements IRetryBuckets {
    private final List<WorkLeaseStage> stages;
    private final ITimeService timeService;
    private final long granularityMs;

    // Ordered buckets by absolute epoch millis (aligned to granularity).
    private final NavigableMap<Long, RetryBucket> buckets = new TreeMap<>();
    // Reverse index: where a given (topic,domainId) is currently scheduled.
    private final Map<IdentityKey, Long> where = new HashMap<>();

    private final ReentrantLock lock = new ReentrantLock();
    private final IMetrics metrics;

    public RetryBuckets(List<WorkLeaseStage> stages, ITimeService timeService, long granularityMs, IMetrics metrics) {
        this.metrics = metrics;
        if (stages == null || stages.isEmpty()) throw new IllegalArgumentException("stages must be non-empty");
        if (timeService == null) throw new IllegalArgumentException("timeService is required");
        if (granularityMs <= 0) throw new IllegalArgumentException("granularity must be > 0");
        this.stages = List.copyOf(stages);
        this.timeService = timeService;
        this.granularityMs = granularityMs;
        metrics.increment("retryBuckets.created");
    }

    @Override
    public boolean addToRetryBucket(String topic, String domainId, long offset /* unused by scheduler */, long leaseAcquireTime, int retriesSoFar) {
        if (retriesSoFar < 0) throw new IllegalArgumentException("retriesSoFar must be >= 0");
        if (retriesSoFar >= stages.size()) {
            metrics.increment("retryBuckets.giveUp");
            return false; // out of ladder → give up
        }

        var stage = stages.get(retriesSoFar);
        long timeout = stage.retryTimeOutMs();
        long jitterMax = Math.max(0L, stage.jitterms());
        long jitter = (jitterMax == 0L) ? 0L : ThreadLocalRandom.current().nextLong(jitterMax + 1);

        long dueAt = leaseAcquireTime + timeout + jitter;   // exact target time for this retry
        long bucketTime = bucketOf(dueAt);                  // quantized for the bucket map

        var id = new IdentityKey(topic, domainId);
        var entry = new RetryKey(topic, domainId, dueAt);

        metrics.increment("retryBuckets.added");
        metrics.increment("retryBuckets." + stage.name() + ".added");
        lock.lock();
        try {
            Long oldBucket = where.get(id);
            if (oldBucket != null) {
                // Already scheduled: keep the earlier schedule (move only if earlier).
                if (bucketTime >= oldBucket) {
                    metrics.increment("retryBuckets.earlierExists");
                    return true; // earlier (or equal) schedule already exists
                }
                // Move from old bucket to earlier bucket.
                var old = buckets.get(oldBucket);
                if (old != null) {
                    // Remove the old entry by identity (time differs)
                    metrics.increment("retryBuckets.moved");
                    old.keys().removeIf(k -> k.topic().equals(topic) && k.domainId().equals(domainId));
                    if (old.keys().isEmpty()) buckets.remove(oldBucket);
                }
            }

            var bucket = buckets.computeIfAbsent(bucketTime, RetryBucket::new);
            bucket.keys().addLast(entry);
            where.put(id, bucketTime);
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<RetryKey> retryKeysForNow() {
        long nowBucket = bucketOf(timeService.currentTimeMillis());

        lock.lock();
        try {
            var out = new ArrayList<RetryKey>();
            // Drain buckets in time order while they are due.
            while (!buckets.isEmpty()) {
                var first = buckets.firstEntry();
                if (first.getKey() > nowBucket) break;

                var dueBucket = first.getValue();
                for (var key : dueBucket.keys()) {
                    metrics.increment("retryBuckets.removed");
                    where.remove(new IdentityKey(key.topic(), key.domainId()));
                    out.add(key);
                }
                buckets.pollFirstEntry();
            }
            return out;
        } finally {
            lock.unlock();
        }
    }

    public Map<Long, RetryBucket> buckets() {
        return Collections.unmodifiableNavigableMap(buckets);
    }

    private long bucketOf(long epochMs) {
        return Math.floorDiv(epochMs, granularityMs) * granularityMs;
    }

}
