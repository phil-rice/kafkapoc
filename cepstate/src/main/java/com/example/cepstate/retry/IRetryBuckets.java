package com.example.cepstate.retry;

import com.example.cepstate.metrics.IMetrics;
import com.example.kafka.common.ITimeService;
import com.example.cepstate.worklease.WorkLeaseStage;

import java.util.*;

public interface IRetryBuckets {
    /**
     * Makes a retry key and puts it in a bucket. Bucket is selected based on retries-so-far.
     * Returns false if we've tried enough (i.e., out of ladder).
     * leaseAcquireTime is when this work last started.
     */
    boolean addToRetryBucket(String topic, String domainId, long offset, long leaseAcquireTime, int retriesSoFar);

    /** Returns all retry keys due as of "now". Each key includes its dueAtMs. */
    List<RetryKey> retryKeysForNow();

    static IRetryBuckets retryBuckets(List<WorkLeaseStage> stages, ITimeService timeService, int granularityMs, IMetrics metrics) {
        return new RetryBuckets(stages, timeService, granularityMs, metrics);
    }
}


/** Internal identity used for dedupe (excludes time). */
record IdentityKey(String topic, String domainId) {}

