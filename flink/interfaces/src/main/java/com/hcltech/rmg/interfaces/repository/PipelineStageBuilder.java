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
 * Builder once the first stage has been added.
 * Type params:
 * - InitialFrom: the pipeline's overall input type
 * - From:        the input type for the *next* stage (i.e., previous stage's output)
 * - To:          the current pipeline's last stage output type
 */
public class PipelineStageBuilder<InitialFrom, From, To> {

    private final ValueTC<InitialFrom> valueTC;
    private final LinkedHashMap<String, PipelineStageDetails<?, ?>> pipelines;
    private final RetryPolicyConfig defaultRetry;                       // immutable root defaults
    private final BiFunction<String, Throwable, String> defaultErrorMsg;// immutable root defaults
    private final long defaultTimeoutMs;                                 // immutable root default

    public PipelineStageBuilder(ValueTC<InitialFrom> valueTC, LinkedHashMap<String, PipelineStageDetails<?, ?>> pipelines,
                                RetryPolicyConfig defaultRetry,
                                BiFunction<String, Throwable, String> defaultErrorMsg,
                                long defaultTimeoutMs) {
        this.valueTC = valueTC;
        this.pipelines = Objects.requireNonNull(pipelines, "pipelines");
        this.defaultRetry = Objects.requireNonNull(defaultRetry, "defaultRetry");
        this.defaultErrorMsg = Objects.requireNonNull(defaultErrorMsg, "defaultErrorMsg");
        this.defaultTimeoutMs = defaultTimeoutMs;
    }

    // ---- Add stage: One-to-Many (From -> NewTo) ----
    // Uses root defaults for retry, error message, and timeout.
    public <NewTo> PipelineStageBuilder<InitialFrom, To, NewTo> oneToMany(
            String name,
            Class<To> fromClass,
            IOneToManyPipeline<To, NewTo> pipeline
    ) {
        var details = new PipelineStageDetails<>(fromClass, pipeline, defaultRetry, defaultErrorMsg, defaultTimeoutMs);
        putUnique(pipelines, name, details);
        return new PipelineStageBuilder<>(valueTC, pipelines, defaultRetry, defaultErrorMsg, defaultTimeoutMs);
    }

    // Per-stage override: retry only
    public <NewTo> PipelineStageBuilder<InitialFrom, To, NewTo> oneToMany(
            String name,
            Class<To> fromClass,
            IOneToManyPipeline<To, NewTo> pipeline,
            RetryPolicyConfig retryOverride
    ) {
        var details = new PipelineStageDetails<>(
                fromClass,
                pipeline,
                Objects.requireNonNullElse(retryOverride, defaultRetry),
                defaultErrorMsg,
                defaultTimeoutMs
        );
        putUnique(pipelines, name, details);
        return new PipelineStageBuilder<>(valueTC, pipelines, defaultRetry, defaultErrorMsg, defaultTimeoutMs);
    }

    // Per-stage override: error message only
    public <NewTo> PipelineStageBuilder<InitialFrom, To, NewTo> oneToMany(
            String name,
            Class<To> fromClass,
            IOneToManyPipeline<To, NewTo> pipeline,
            BiFunction<String, Throwable, String> errorMsgOverride
    ) {
        var details = new PipelineStageDetails<>(
                fromClass,
                pipeline,
                defaultRetry,
                Objects.requireNonNullElse(errorMsgOverride, defaultErrorMsg),
                defaultTimeoutMs
        );
        putUnique(pipelines, name, details);
        return new PipelineStageBuilder<>(valueTC, pipelines, defaultRetry, defaultErrorMsg, defaultTimeoutMs);
    }

    // NEW: Per-stage override: timeout only
    public <NewTo> PipelineStageBuilder<InitialFrom, To, NewTo> oneToMany(
            String name,
            Class<To> fromClass,
            IOneToManyPipeline<To, NewTo> pipeline,
            long timeoutOverrideMs
    ) {
        var details = new PipelineStageDetails<>(
                fromClass,
                pipeline,
                defaultRetry,
                defaultErrorMsg,
                timeoutOverrideMs
        );
        putUnique(pipelines, name, details);
        return new PipelineStageBuilder<>(valueTC, pipelines, defaultRetry, defaultErrorMsg, defaultTimeoutMs);
    }

    // Per-stage override: both retry and error message
    public <NewTo> PipelineStageBuilder<InitialFrom, To, NewTo> oneToMany(
            String name,
            Class<To> fromClass,
            IOneToManyPipeline<To, NewTo> pipeline,
            RetryPolicyConfig retryOverride,
            BiFunction<String, Throwable, String> errorMsgOverride
    ) {
        var details = new PipelineStageDetails<>(
                fromClass,
                pipeline,
                Objects.requireNonNullElse(retryOverride, defaultRetry),
                Objects.requireNonNullElse(errorMsgOverride, defaultErrorMsg),
                defaultTimeoutMs
        );
        putUnique(pipelines, name, details);
        return new PipelineStageBuilder<>(valueTC, pipelines, defaultRetry, defaultErrorMsg, defaultTimeoutMs);
    }

    // NEW: Per-stage override: retry + error message + timeout
    public <NewTo> PipelineStageBuilder<InitialFrom, To, NewTo> oneToMany(
            String name,
            Class<To> fromClass,
            IOneToManyPipeline<To, NewTo> pipeline,
            RetryPolicyConfig retryOverride,
            BiFunction<String, Throwable, String> errorMsgOverride,
            Long timeoutOverrideMs
    ) {
        long effectiveTimeout = (timeoutOverrideMs != null) ? timeoutOverrideMs : defaultTimeoutMs;
        var details = new PipelineStageDetails<>(
                fromClass,
                pipeline,
                Objects.requireNonNullElse(retryOverride, defaultRetry),
                Objects.requireNonNullElse(errorMsgOverride, defaultErrorMsg),
                effectiveTimeout
        );
        putUnique(pipelines, name, details);
        return new PipelineStageBuilder<>(valueTC, pipelines, defaultRetry, defaultErrorMsg, defaultTimeoutMs);
    }

    // ---- Add stage: One-to-One (delegates to One-to-Many) ----
    public <NewTo> PipelineStageBuilder<InitialFrom, To, NewTo> oneToOne(
            String name,
            Class<To> fromClass,
            IOneToOnePipeline<To, NewTo> pipeline
    ) {
        return oneToMany(name, fromClass, pipeline.toOneToManyPipeline());
    }

    public <NewTo> PipelineStageBuilder<InitialFrom, To, NewTo> oneToOne(
            String name,
            Class<To> fromClass,
            IOneToOnePipeline<To, NewTo> pipeline,
            RetryPolicyConfig retryOverride
    ) {
        return oneToMany(name, fromClass, pipeline.toOneToManyPipeline(), retryOverride);
    }

    public <NewTo> PipelineStageBuilder<InitialFrom, To, NewTo> oneToOne(
            String name,
            Class<To> fromClass,
            IOneToOnePipeline<To, NewTo> pipeline,
            BiFunction<String, Throwable, String> errorMsgOverride
    ) {
        return oneToMany(name, fromClass, pipeline.toOneToManyPipeline(), errorMsgOverride);
    }

    // NEW: one-to-one with timeout only
    public <NewTo> PipelineStageBuilder<InitialFrom, To, NewTo> oneToOne(
            String name,
            Class<To> fromClass,
            IOneToOnePipeline<To, NewTo> pipeline,
            long timeoutOverrideMs
    ) {
        return oneToMany(name, fromClass, pipeline.toOneToManyPipeline(), timeoutOverrideMs);
    }

    public <NewTo> PipelineStageBuilder<InitialFrom, To, NewTo> oneToOne(
            String name,
            Class<To> fromClass,
            IOneToOnePipeline<To, NewTo> pipeline,
            RetryPolicyConfig retryOverride,
            BiFunction<String, Throwable, String> errorMsgOverride
    ) {
        return oneToMany(name, fromClass, pipeline.toOneToManyPipeline(), retryOverride, errorMsgOverride);
    }

    // NEW: one-to-one with retry + error + timeout
    public <NewTo> PipelineStageBuilder<InitialFrom, To, NewTo> oneToOne(
            String name,
            Class<To> fromClass,
            IOneToOnePipeline<To, NewTo> pipeline,
            RetryPolicyConfig retryOverride,
            BiFunction<String, Throwable, String> errorMsgOverride,
            Long timeoutOverrideMs
    ) {
        return oneToMany(name, fromClass, pipeline.toOneToManyPipeline(), retryOverride, errorMsgOverride, timeoutOverrideMs);
    }

    // ---- Build result ----
    public PipelineDetails<InitialFrom, To> build() {
        return new PipelineDetails<>(valueTC, new LinkedHashMap<>(pipelines));
    }

    private static void putUnique(Map<String, PipelineStageDetails<?, ?>> map,
                                  String name,
                                  PipelineStageDetails<?, ?> details) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(details, "details");
        if (map.containsKey(name)) {
            throw new IllegalArgumentException("Duplicate stage name: " + name +". Existing names: "+ map.keySet());
        }
        map.put(name, details);
    }
}
