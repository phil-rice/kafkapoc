package com.hcltech.rmg.interfaces.pipeline;


import com.hcltech.rmg.interfaces.outcome.Outcome;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Our business logic implements this
 */
public interface IOneToOnePipeline<From, To> extends IAsyncPipeline<From, To> {
    CompletableFuture<Outcome<To>> process(From from);

    default IOneToManyPipeline<From, To> toOneToManyPipeline() {
        return (from) -> process(from).thenApply(o -> o.map(List::of));
    }
}
