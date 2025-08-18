package com.example.cepstate.retry;

import java.util.ArrayDeque;
import java.util.Deque;

public record RetryBucket(long bucketTime, Deque<RetryKey> keys) {
    RetryBucket(long bucketTime) {
        this(bucketTime, new ArrayDeque<>());
    }
}
