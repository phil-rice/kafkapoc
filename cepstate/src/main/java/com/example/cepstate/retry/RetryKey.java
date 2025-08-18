package com.example.cepstate.retry;

/**
 * Represents a key for retrying messages in a specific topic and domain.
* The dueAtMs should have jitter and backoff applied to it before this is created
 */
public record RetryKey(String topic, String domainId, long dueAtMs) {
}