package com.hcltech.rmg.flinkadapters.context;

import com.hcltech.rmg.flinkadapters.envelopes.RetryEnvelope;
import com.hcltech.rmg.interfaces.pipeline.ValueTC;

import java.util.concurrent.CompletionStage;

public interface IFlinkRetry {
    <T>CompletionStage<Void> retry(ValueTC<T> tc, RetryEnvelope<T> retryEnvelope);
}
