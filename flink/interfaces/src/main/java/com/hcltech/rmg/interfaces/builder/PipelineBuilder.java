package com.hcltech.rmg.interfaces.builder;

import com.hcltech.rmg.interfaces.pipeline.IPipeline;
import com.hcltech.rmg.interfaces.repository.PipelineStageDetails;
import com.hcltech.rmg.interfaces.retry.RetryPolicyConfig;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Root builder – sets up defaults and starts the pipeline chain.
 * Stores stage entries as PipelineStageDetails(fromClass, pipeline, retry, errorMsg, timeoutMs).
 */
public class PipelineBuilder<InitialFrom> {

    // kept for completeness
    private final BiFunction<String, Throwable, String> defaultErrorMsg;
    private final RetryPolicyConfig defaultRetry;
    private final long defaultTimeoutMs;

    public static <InitialFrom> PipelineBuilder<InitialFrom> builder(
            BiFunction<String, Throwable, String> errorMsgFn,
            RetryPolicyConfig retry,
            long defaultTimeoutMs) {
        return new PipelineBuilder<>(errorMsgFn, retry, defaultTimeoutMs);
    }

    public PipelineBuilder(BiFunction<String, Throwable, String> errorMsgFn,
                           RetryPolicyConfig retry,
                           long defaultTimeoutMs) {
        this.defaultErrorMsg = Objects.requireNonNull(errorMsgFn, "errorMsgFn");
        this.defaultRetry = Objects.requireNonNull(retry, "retry");
        this.defaultTimeoutMs = defaultTimeoutMs;
    }

    /**
     * First stage: requires Class token of InitialFrom, plus a one-to-many pipeline (async).
     */
    public <To> PipelineStageBuilder<InitialFrom, InitialFrom, To> stage(
            String name,
            Class<InitialFrom> fromClass,
            IPipeline<InitialFrom, To> pipeline
    ) {
        var map = new LinkedHashMap<String, PipelineStageDetails<?, ?>>();
        putUnique(map, name, new PipelineStageDetails<>(
                fromClass, pipeline, defaultRetry, defaultErrorMsg, defaultTimeoutMs));
        return new PipelineStageBuilder<>(map, defaultRetry, defaultErrorMsg, defaultTimeoutMs);
    }

    private static void putUnique(Map<String, PipelineStageDetails<?, ?>> map,
                                  String name,
                                  PipelineStageDetails<?, ?> details) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(details, "details");
        if (map.containsKey(name)) {
            throw new IllegalArgumentException("Duplicate stage name: " + name);
        }
        map.put(name, details);
    }

}
