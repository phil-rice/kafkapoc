package com.hcltech.rmg.interfaces.repository;

import com.hcltech.rmg.interfaces.pipeline.IPipeline;
import com.hcltech.rmg.interfaces.retry.RetryPolicyConfig;

import java.util.function.BiFunction;

public record PipelineStageDetails<From, To>(
        Class<From> fromClass,
        IPipeline<From, To> pipeline,
        RetryPolicyConfig retry,
        /* stagename / throwable => msg*/
        BiFunction<String, Throwable, String> errorMsg,
        long timeOutMs
) {
}
