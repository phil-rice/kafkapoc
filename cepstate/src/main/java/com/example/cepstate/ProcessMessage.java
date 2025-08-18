package com.example.cepstate;

import org.apache.commons.jxpath.JXPathContext;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/* M message, C cepState, S sideEffect */
public interface ProcessMessage<Optics,M, C, S> {
    CompletionStage<SideEffectsAndCepStateMutations<Optics, S>> process(C cepState, M message) throws Exception;

    static <M, C, S> CompletionStage<List<S>> processMessage(
            ProcessMessageContext<JXPathContext,M, C> context,
            ProcessMessage<JXPathContext,M, C, S> processMessage,
            M message,
            long offset
    ) {
        final String domainId = context.domainIdFn().apply(message);
        final String token = context.workLease().tryAcquire(domainId, offset);
        if (token == null) return CompletableFuture.completedFuture(null);

        CompletionStage<List<S>> chain =
                context.cepState().get(domainId, context.startState())
                        // process(…) may throw synchronously → turn into a failed stage
                        .thenCompose(cepState -> Exceptions.wrap(() -> processMessage.process(cepState, message)))
                        .thenCompose(result -> context.cepState()
                                .mutate(domainId, result.cepMutations())   // CompletionStage<Void>
                                .thenApply(v -> {
                                    context.workLease().succeed(domainId, token);
                                    return result.sideEffects();
                                }));

        return Exceptions.sideEffectOnFailure(chain, ex -> context.workLease().fail(domainId, token));
    }
}
