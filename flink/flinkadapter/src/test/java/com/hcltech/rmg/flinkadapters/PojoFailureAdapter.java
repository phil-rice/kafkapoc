package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.common.async.FailureAdapter;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Generic FailureAdapter for plain POJOs (non-envelope).
 * Maps failures/timeouts to an Out via provided mappers.
 */
public final class PojoFailureAdapter<In, Out> implements FailureAdapter<In, Out> {

    private final BiFunction<In, Throwable, Out> failureMapper;
    private final Function<In, Out> timeoutMapper;

    private PojoFailureAdapter(BiFunction<In, Throwable, Out> failureMapper,
                               Function<In, Out> timeoutMapper) {
        this.failureMapper = Objects.requireNonNull(failureMapper, "failureMapper");
        this.timeoutMapper = Objects.requireNonNull(timeoutMapper, "timeoutMapper");
    }

    /** Same mapping for failure and timeout (e.g., prefix "fail:" on a field). */
    public static <In, Out> PojoFailureAdapter<In, Out> same(Function<In, Out> mapper) {
        return new PojoFailureAdapter<>((in, err) -> mapper.apply(in), mapper);
    }

    /** Custom mapping for failure vs timeout. */
    public static <In, Out> PojoFailureAdapter<In, Out> of(
            BiFunction<In, Throwable, Out> onFailure,
            Function<In, Out> onTimeout) {
        return new PojoFailureAdapter<>(onFailure, onTimeout);
    }

    @Override
    public Out onFailure(In in, Throwable error) {
        return Objects.requireNonNull(failureMapper.apply(in, error),
                "onFailure mapper returned null");
    }

    @Override
    public Out onTimeout(In in, long elapsedNanos) {
        return Objects.requireNonNull(timeoutMapper.apply(in),
                "onTimeout mapper returned null");
    }
}
