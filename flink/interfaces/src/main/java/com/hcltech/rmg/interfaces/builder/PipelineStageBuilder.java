package com.hcltech.rmg.interfaces.builder;

import com.hcltech.rmg.interfaces.pipeline.IPipeline;
import com.hcltech.rmg.interfaces.repository.PipelineDetails;
import com.hcltech.rmg.interfaces.repository.PipelineStageDetails;
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

    private final LinkedHashMap<String, PipelineStageDetails<?, ?>> pipelines;
    private final RetryPolicyConfig defaultRetry;                       // immutable root defaults
    private final BiFunction<String, Throwable, String> defaultErrorMsg;// immutable root defaults
    private final long defaultTimeoutMs;                                 // immutable root default

    public PipelineStageBuilder(LinkedHashMap<String, PipelineStageDetails<?, ?>> pipelines,
                                RetryPolicyConfig defaultRetry,
                                BiFunction<String, Throwable, String> defaultErrorMsg,
                                long defaultTimeoutMs) {
        this.pipelines = Objects.requireNonNull(pipelines, "pipelines");
        this.defaultRetry = Objects.requireNonNull(defaultRetry, "defaultRetry");
        this.defaultErrorMsg = Objects.requireNonNull(defaultErrorMsg, "defaultErrorMsg");
        this.defaultTimeoutMs = defaultTimeoutMs;
    }

    // ---- Add stage: One-to-Many (From -> NewTo) ----
    // Uses root defaults for retry, error message, and timeout.
    public <NewTo> PipelineStageBuilder<InitialFrom, To, NewTo> stage(
            String name,
            Class<To> fromClass,
            IPipeline<To, NewTo> pipeline
    ) {
        var details = new PipelineStageDetails<>(fromClass, pipeline, defaultRetry, defaultErrorMsg, defaultTimeoutMs);
        putUnique(pipelines, name, details);
        return new PipelineStageBuilder<>(pipelines, defaultRetry, defaultErrorMsg, defaultTimeoutMs);
    }

    // Per-stage override: retry only
    public <NewTo> PipelineStageBuilder<InitialFrom, To, NewTo> stage(
            String name,
            Class<To> fromClass,
            IPipeline<To, NewTo> pipeline,
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
        return new PipelineStageBuilder<>(pipelines, defaultRetry, defaultErrorMsg, defaultTimeoutMs);
    }

    // Per-stage override: error message only
    public <NewTo> PipelineStageBuilder<InitialFrom, To, NewTo> stage(
            String name,
            Class<To> fromClass,
            IPipeline<To, NewTo> pipeline,
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
        return new PipelineStageBuilder<>(pipelines, defaultRetry, defaultErrorMsg, defaultTimeoutMs);
    }

    // NEW: Per-stage override: timeout only
    public <NewTo> PipelineStageBuilder<InitialFrom, To, NewTo> stage(
            String name,
            Class<To> fromClass,
            IPipeline<To, NewTo> pipeline,
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
        return new PipelineStageBuilder<>(pipelines, defaultRetry, defaultErrorMsg, defaultTimeoutMs);
    }

    // Per-stage override: both retry and error message
    public <NewTo> PipelineStageBuilder<InitialFrom, To, NewTo> stage(
            String name,
            Class<To> fromClass,
            IPipeline<To, NewTo> pipeline,
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
        return new PipelineStageBuilder<>(pipelines, defaultRetry, defaultErrorMsg, defaultTimeoutMs);
    }

    // NEW: Per-stage override: retry + error message + timeout
    public <NewTo> PipelineStageBuilder<InitialFrom, To, NewTo> stage(
            String name,
            Class<To> fromClass,
            IPipeline<To, NewTo> pipeline,
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
        return new PipelineStageBuilder<>(pipelines, defaultRetry, defaultErrorMsg, defaultTimeoutMs);
    }

    // ---- Build result ----
    public PipelineDetails<InitialFrom, To> build() {
        return new PipelineDetails<>(new LinkedHashMap<>(pipelines));
    }

    private static void putUnique(Map<String, PipelineStageDetails<?, ?>> map,
                                  String name,
                                  PipelineStageDetails<?, ?> details) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(details, "details");
        if (map.containsKey(name)) {
            throw new IllegalArgumentException("Duplicate stage name: " + name + ". Existing names: " + map.keySet());
        }
        map.put(name, details);
    }
}
