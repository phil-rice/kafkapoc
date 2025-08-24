package com.example.cepstate;

import com.example.cepstate.retry.IRetryBuckets;
import com.example.cepstate.worklease.WorkLease;
import com.example.kafka.common.ITimeService;

import java.util.function.Function;

public record ProcessMessageContext<Optics, M, C>(
        String topic,
        C startState,
        CepState<Optics, C> cepState,
        WorkLease workLease,
        IRetryBuckets buckets,
        ITimeService timeService,
        Function<M, String> domainIdFn
) {
    /**
     * Test helper factory: same as the canonical constructor but defaults {@code buckets} to null.
     */
    public static <Optics, M, C> ProcessMessageContext<Optics, M, C> test(
            String topic,
            C startState,
            CepState<Optics, C> cepState,
            WorkLease workLease,
            ITimeService timeService,
            Function<M, String> domainIdFn
    ) {
        return new ProcessMessageContext<>(topic, startState, cepState, workLease, null, timeService, domainIdFn);
    }
}
