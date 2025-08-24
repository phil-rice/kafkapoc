package com.example.cepstate;

import com.example.cepstate.worklease.AcquireResult;
import com.example.cepstate.worklease.SuceedResult;
import org.apache.commons.jxpath.JXPathContext;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/* M message, C cepState, S sideEffect */
public interface ProcessMessage<Optics, M, C, S> {
    CompletionStage<SideEffectsAndCepStateMutations<Optics, S>> process(C cepState, M message) throws Exception;

    static <M, C, S> CompletionStage<List<S>> processMessage(
            ProcessMessageContext<JXPathContext, M, C> context,
            ProcessMessage<JXPathContext, M, C, S> processMessage,
            M message,
            long offset
    ) {
        final String domainId = context.domainIdFn().apply(message);
        final AcquireResult acquireResult = context.workLease().tryAcquire(domainId, offset);
        if (acquireResult.failed()) return CompletableFuture.completedFuture(null);
        var token = acquireResult.token();
        CompletionStage<List<S>> chain =
                context.cepState().get(domainId, context.startState())
                        // process(…) may throw synchronously → turn into a failed stage
                        .thenCompose(cepState -> Exceptions.wrap(() -> processMessage.process(cepState, message)))
                        .thenCompose(result -> context.cepState()
                                .mutate(domainId, result.cepMutations())   // CompletionStage<Void>
                                .thenApply(v -> {
                                    SuceedResult succeed = context.workLease().succeed(domainId, token);
                                    Long nextOffset = succeed.nextOffset();
                                    if (nextOffset != null) {
                                        context.buckets().addToRetryBucket(context.topic(), domainId, nextOffset, context.timeService().currentTimeMillis(), 0);
                                    }
                                    return result.sideEffects();
                                }));

        return Exceptions.sideEffectOnFailure(chain, ex -> {
            var failed = context.workLease().fail(domainId, token);
            if (failed.willRetry())
                context.buckets().addToRetryBucket(context.topic(), domainId, offset, context.timeService().currentTimeMillis(), failed.retryCount());

            Long nextOffset = failed.nextOffset();
            if (nextOffset != null)
                context.buckets().addToRetryBucket(context.topic(), domainId, nextOffset, context.timeService().currentTimeMillis(), 0);
        });
    }
}
