package com.hcltech.rmg.interfaces.retry;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Immutable, serialisable retry policy with exponential backoff and jitter.
 * <p>
 * The delay is calculated as:
 * <pre>
 *   delay = baseDelay * multiplier^(attempt-1)
 *   delay = min(delay, maxDelay)
 * </pre>
 * Then, if {@code jitterFraction > 0}, a random factor in the range
 * {@code [1 - jitterFraction, 1 + jitterFraction]} is applied to spread
 * retries out in time.
 *
 * <h3>Examples (baseDelay=1s, multiplier=2.0, maxDelay=10s)</h3>
 * <ul>
 *   <li>No jitter ({@code jitterFraction=0.0})</li>
 *     <ul>
 *       <li>Attempt 1 → 1s</li>
 *       <li>Attempt 2 → 2s</li>
 *       <li>Attempt 3 → 4s</li>
 *       <li>Attempt 4 → 8s</li>
 *       <li>Attempt 5 → 10s (capped)</li>
 *     </ul>
 *   <li>With jitter=0.5 (±50%)</li>
 *     <ul>
 *       <li>Attempt 1 → between 0.5s and 1.5s</li>
 *       <li>Attempt 2 → between 1s and 3s</li>
 *       <li>Attempt 3 → between 2s and 6s</li>
 *       <li>Attempt 4 → between 4s and 12s, but capped at 10s → [4s, 10s]</li>
 *     </ul>
 * </ul>
 */
public record RetryPolicyConfig(
        Duration baseDelay,
        double multiplier,
        Duration maxDelay,
        int maxAttempts,
        double jitterFraction
) implements Serializable {

    public RetryPolicyConfig {
        Objects.requireNonNull(baseDelay, "baseDelay");
        Objects.requireNonNull(maxDelay, "maxDelay");
        if (multiplier < 1.0) throw new IllegalArgumentException("multiplier must be >= 1.0");
        if (maxAttempts <= 0) throw new IllegalArgumentException("maxAttempts must be > 0");
        if (jitterFraction < 0.0 || jitterFraction > 1.0)
            throw new IllegalArgumentException("jitterFraction must be between 0.0 and 1.0");
    }

    /** Convenience ctor with default jitter=0.5 (±50%). */
    public RetryPolicyConfig(Duration baseDelay, double multiplier, Duration maxDelay, int maxAttempts) {
        this(baseDelay, multiplier, maxDelay, maxAttempts, 0.5d);
    }

    /**
     * Compute next due time for a retry attempt.
     *
     * @param nextAttempt attempt number starting at 1
     * @param now         current reference time
     * @param rng         source of randomness for jitter
     * @return due time
     */
    public Instant dueAt(long nextAttempt, Instant now, IRandom rng) {
        if (nextAttempt > maxAttempts) return now;

        double ms = baseDelay.toMillis() * Math.pow(multiplier, Math.max(0, nextAttempt - 1));

        if (jitterFraction > 0.0) {
            double factor = 1.0 + (rng.nextDouble() * 2.0 - 1.0) * jitterFraction;
            if (factor < 0.0) factor = 0.0;
            ms = ms * factor;
        }

        long capped = Math.min((long) Math.round(ms), maxDelay.toMillis());
        return now.plusMillis(capped);
    }

}
