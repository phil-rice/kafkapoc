package com.example.cepstate;

import java.util.function.Function;

public record ProcessMessageContext<Optics,M, C>(
        C startState,
        CepState<Optics,C> cepState,
        WorkLease workLease,
        Function<M, String> domainIdFn
) {
}
