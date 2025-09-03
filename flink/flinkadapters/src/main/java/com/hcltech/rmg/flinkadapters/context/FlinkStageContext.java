package com.hcltech.rmg.flinkadapters.context;

import com.hcltech.rmg.interfaces.bizlogic.IHasValueTc;
import com.hcltech.rmg.interfaces.bizlogic.ValueTC;

public record FlinkStageContext<From>(
        FlinkPipelineContext pipelineContext,
        ValueTC<From> valueTc,
        String stageName,
        RetryPolicyConfig retryPolicy) implements IHasValueTc<From> {
}
