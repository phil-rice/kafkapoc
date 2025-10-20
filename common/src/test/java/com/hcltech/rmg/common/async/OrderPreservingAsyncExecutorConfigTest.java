package com.hcltech.rmg.common.async;

import com.hcltech.rmg.common.ITimeService;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

final class OrderPreservingAsyncExecutorConfigTest {

    // Minimal stubs for required ports
    static final class StubCorrelator implements Correlator<String> {
        @Override public long correlationId(String env) { return 1L; }
        @Override public int laneHash(String env) { return 0; }
    }
    static final class StubFailure implements FailureAdapter<String, String> {
        @Override public String onFailure(String in, Throwable error) { return "fail"; }
        @Override public String onTimeout(String in, long elapsedNanos) { return "timeout"; }
    }
    static final class StubFR implements FutureRecordTypeClass<Object, String, String> {
        @Override public void completed(Object fr, String out) {}
        @Override public void timedOut(Object fr, String in, long elapsedNanos) {}
        @Override public void failed(Object fr, String in, Throwable error) {}
    }

    private OrderPreservingAsyncExecutorConfig<String, String, Object> cfg(
            int laneCount, int laneDepth, int maxInFlight, int threads,
            int cycles, long timeoutMillis
    ) {
        return new OrderPreservingAsyncExecutorConfig<>(
                laneCount, laneDepth, maxInFlight, threads, cycles, timeoutMillis,
                new StubCorrelator(), new StubFailure(), new StubFR(), ITimeService.real
        );
    }

    @Test
    void valid_config_passes() {
        assertDoesNotThrow(() ->
                cfg(256, 32, 128, 16, 3, 250)
        );
        // edge powers
        assertDoesNotThrow(() ->
                cfg(1, 1, 1, 1, 1, 0)
        );
    }

    @Test
    void laneCount_must_be_power_of_two_and_positive() {
        assertThrows(IllegalArgumentException.class, () -> cfg(0, 32, 8, 2, 2, 100));
        assertThrows(IllegalArgumentException.class, () -> cfg(3, 32, 8, 2, 2, 100));
    }

    @Test
    void laneDepth_must_be_power_of_two_and_positive() {
        assertThrows(IllegalArgumentException.class, () -> cfg(8, 0, 8, 2, 2, 100));
        assertThrows(IllegalArgumentException.class, () -> cfg(8, 6, 8, 2, 2, 100));
    }

    @Test
    void maxInFlight_must_be_positive() {
        assertThrows(IllegalArgumentException.class, () -> cfg(8, 8, 0, 1, 2, 100));
        assertThrows(IllegalArgumentException.class, () -> cfg(8, 8, -1, 1, 2, 100));
    }

    @Test
    void executorThreads_must_be_positive_and_leq_maxInFlight() {
        assertThrows(IllegalArgumentException.class, () -> cfg(8, 8, 4, 0, 2, 100));
        assertThrows(IllegalArgumentException.class, () -> cfg(8, 8, 4, 5, 2, 100));
        assertDoesNotThrow(() -> cfg(8, 8, 4, 4, 2, 100));
    }

    @Test
    void admissionCycleCap_must_be_positive() {
        assertThrows(IllegalArgumentException.class, () -> cfg(8, 8, 4, 2, 0, 100));
        assertThrows(IllegalArgumentException.class, () -> cfg(8, 8, 4, 2, -1, 100));
    }

    @Test
    void timeoutMillis_must_be_non_negative() {
        assertThrows(IllegalArgumentException.class, () -> cfg(8, 8, 4, 2, 2, -1));
        assertDoesNotThrow(() -> cfg(8, 8, 4, 2, 2, 0));
    }

    @Test
    void ports_must_not_be_null() {
        // correlator
        assertThrows(NullPointerException.class, () ->
                new OrderPreservingAsyncExecutorConfig<>(
                        8, 8, 4, 2, 2, 100,
                        null, new StubFailure(), new StubFR(), ITimeService.real
                )
        );
        // failure
        assertThrows(NullPointerException.class, () ->
                new OrderPreservingAsyncExecutorConfig<>(
                        8, 8, 4, 2, 2, 100,
                        new StubCorrelator(), null, new StubFR(), ITimeService.real
                )
        );
        // future record
        assertThrows(NullPointerException.class, () ->
                new OrderPreservingAsyncExecutorConfig<>(
                        8, 8, 4, 2, 2, 100,
                        new StubCorrelator(), new StubFailure(), null, ITimeService.real
                )
        );
        // time service
        assertThrows(NullPointerException.class, () ->
                new OrderPreservingAsyncExecutorConfig<>(
                        8, 8, 4, 2, 2, 100,
                        new StubCorrelator(), new StubFailure(), new StubFR(), null
                )
        );
    }
}
