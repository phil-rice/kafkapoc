package com.hcltech.rmg.flinkadaptersold.context;

import com.hcltech.rmg.interfaces.retry.IRandom;
import com.hcltech.rmg.interfaces.retry.RetryPolicyConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class RetryPolicyConfigTest {

    private static final Instant NOW = Instant.parse("2025-09-02T12:00:00Z");

    static class FixedRandom implements IRandom {
        private final double value;
        FixedRandom(double value) { this.value = value; }
        @Override public double nextDouble() { return value; }
    }

    @Test
    void noJitterComputesExponentialDelays() {
        RetryPolicyConfig policy = new RetryPolicyConfig(
                Duration.ofSeconds(1), 2.0, Duration.ofSeconds(10), 5, 0.0);

        assertEquals(NOW.plusSeconds(1),  policy.dueAt(1, NOW, new FixedRandom(0.5)));
        assertEquals(NOW.plusSeconds(2),  policy.dueAt(2, NOW, new FixedRandom(0.5)));
        assertEquals(NOW.plusSeconds(4),  policy.dueAt(3, NOW, new FixedRandom(0.5)));
        assertEquals(NOW.plusSeconds(8),  policy.dueAt(4, NOW, new FixedRandom(0.5)));
        assertEquals(NOW.plusSeconds(10), policy.dueAt(5, NOW, new FixedRandom(0.5))); // capped
    }

    @Test
    void withJitterAppliesFractionToDelay() {
        RetryPolicyConfig policy = new RetryPolicyConfig(
                Duration.ofSeconds(10), 1.0, Duration.ofSeconds(10), 5, 0.5);

        // Attempt 1, base = 10s, jitter ±50% = [5s, 15s], capped at 10s → [5s, 10s]
        // rng=0.0 → factor=1-0.5=0.5 → 5s
        assertEquals(NOW.plusSeconds(5), policy.dueAt(1, NOW, new FixedRandom(0.0)));

        // rng=0.5 → factor=1.0 → 10s
        assertEquals(NOW.plusSeconds(10), policy.dueAt(1, NOW, new FixedRandom(0.5)));

        // rng=1.0 → factor=1+0.5=1.5 → 15s, but capped to 10s
        assertEquals(NOW.plusSeconds(10), policy.dueAt(1, NOW, new FixedRandom(1.0)));
    }

    @Test
    void attemptBeyondMaxAttemptsReturnsNow() {
        RetryPolicyConfig policy = new RetryPolicyConfig(
                Duration.ofSeconds(1), 2.0, Duration.ofSeconds(10), 3, 0.0);

        // maxAttempts=3 → attempt=4 should yield now
        assertEquals(NOW, policy.dueAt(4, NOW, new FixedRandom(0.5)));
    }

    @Test
    void rejectsInvalidConfig() {
        assertThrows(IllegalArgumentException.class,
                () -> new RetryPolicyConfig(Duration.ofSeconds(1), 0.5, Duration.ofSeconds(10), 3, 0.0));

        assertThrows(IllegalArgumentException.class,
                () -> new RetryPolicyConfig(Duration.ofSeconds(1), 2.0, Duration.ofSeconds(10), 0, 0.0));

        assertThrows(IllegalArgumentException.class,
                () -> new RetryPolicyConfig(Duration.ofSeconds(1), 2.0, Duration.ofSeconds(10), 3, -0.1));

        assertThrows(IllegalArgumentException.class,
                () -> new RetryPolicyConfig(Duration.ofSeconds(1), 2.0, Duration.ofSeconds(10), 3, 1.5));
    }
}
