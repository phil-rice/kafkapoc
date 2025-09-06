package com.hcltech.rmg.interfaces.repository;

import com.hcltech.rmg.interfaces.pipeline.IOneToManyPipeline;
import com.hcltech.rmg.interfaces.pipeline.IOneToOnePipeline;
import com.hcltech.rmg.interfaces.pipeline.ValueTC;
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

    private final ValueTC<InitialFrom> valueTC;                 // kept for completeness
    private final BiFunction<String, Throwable, String> defaultErrorMsg;
    private final RetryPolicyConfig defaultRetry;
    private final long defaultTimeoutMs;

    public static <InitialFrom> PipelineBuilder<InitialFrom> builder(
            ValueTC<InitialFrom> valueTC,
            BiFunction<String, Throwable, String> errorMsgFn,
            RetryPolicyConfig retry,
            long defaultTimeoutMs) {
        return new PipelineBuilder<>(valueTC, errorMsgFn, retry, defaultTimeoutMs);
    }

    public PipelineBuilder(ValueTC<InitialFrom> valueTC,
                           BiFunction<String, Throwable, String> errorMsgFn,
                           RetryPolicyConfig retry,
                           long defaultTimeoutMs) {
        this.valueTC = Objects.requireNonNull(valueTC, "valueTC");
        this.defaultErrorMsg = Objects.requireNonNull(errorMsgFn, "errorMsgFn");
        this.defaultRetry = Objects.requireNonNull(retry, "retry");
        this.defaultTimeoutMs = defaultTimeoutMs;
    }

    /**
     * First stage: requires Class token of InitialFrom, plus a one-to-many pipeline.
     */
    public <To> PipelineStageBuilder<InitialFrom, InitialFrom, To> oneToMany(
            String name,
            Class<InitialFrom> fromClass,
            IOneToManyPipeline<InitialFrom, To> pipeline
    ) {
        var map = new LinkedHashMap<String, PipelineStageDetails<?, ?>>();
        putUnique(map, name, new PipelineStageDetails<InitialFrom, To>(
                fromClass, pipeline, defaultRetry, defaultErrorMsg, defaultTimeoutMs));
        return new PipelineStageBuilder<>(valueTC, map, defaultRetry, defaultErrorMsg, defaultTimeoutMs);
    }

    /**
     * First stage: one-to-one overload (delegates to one-to-many).
     */
    public <To> PipelineStageBuilder<InitialFrom, InitialFrom, To> oneToOne(
            String name,
            Class<InitialFrom> fromClass,
            IOneToOnePipeline<InitialFrom, To> pipeline
    ) {
        return oneToMany(name, fromClass, pipeline.toOneToManyPipeline());
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

    public ValueTC<InitialFrom> valueTC() {
        return valueTC;
    }
}
