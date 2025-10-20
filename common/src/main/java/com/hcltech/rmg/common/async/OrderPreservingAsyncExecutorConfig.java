package com.hcltech.rmg.common.async;

import com.hcltech.rmg.common.ITimeService;

import java.util.Objects;

/**
 * Configuration record for OrderPreservingAsyncExecutor.
 *
 * @param laneCount        number of lanes (power-of-two, >0)
 * @param laneDepth        depth per lane (power-of-two, >0)
 * @param maxInFlight      total max concurrent async launches (M, >0)
 * @param executorThreads  number of threads in the async executor (1..maxInFlight)
 * @param admissionCycleCap cooperative drain→retry cycles per event (>0)
 * @param timeoutMillis    SLA for lane head (>=0; 0 disables timeouts)
 * @param correlator       provides correlationId / laneHash (non-null)
 * @param failureAdapter   maps failures and timeouts to Out (non-null)
 * @param futureRecord     completes framework future records (non-null)
 * @param timeService      provides the current time in nanos (non-null)
 */
public record OrderPreservingAsyncExecutorConfig<In, Out, FR>(
        int laneCount,
        int laneDepth,
        int maxInFlight,
        int executorThreads,
        int admissionCycleCap,
        long timeoutMillis,
        Correlator<In> correlator,
        FailureAdapter<In, Out> failureAdapter,
        FutureRecordTypeClass<FR, In, Out> futureRecord,
        ITimeService timeService
) {
    public OrderPreservingAsyncExecutorConfig {
        if (laneCount <= 0 || Integer.bitCount(laneCount) != 1) {
            throw new IllegalArgumentException("laneCount must be a power of two > 0");
        }
        if (laneDepth <= 0 || Integer.bitCount(laneDepth) != 1) {
            throw new IllegalArgumentException("laneDepth must be a power of two > 0");
        }
        if (maxInFlight <= 0) {
            throw new IllegalArgumentException("maxInFlight must be > 0");
        }
        if (executorThreads <= 0) {
            throw new IllegalArgumentException("executorThreads must be > 0");
        }
        if (executorThreads > maxInFlight) {
            throw new IllegalArgumentException("executorThreads must be <= maxInFlight");
        }
        if (admissionCycleCap <= 0) {
            throw new IllegalArgumentException("admissionCycleCap must be > 0");
        }
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("timeoutMillis must be >= 0");
        }

        Objects.requireNonNull(correlator, "correlator");
        Objects.requireNonNull(failureAdapter, "failureAdapter");
        Objects.requireNonNull(futureRecord, "futureRecord");
        Objects.requireNonNull(timeService, "timeService");
    }
}
