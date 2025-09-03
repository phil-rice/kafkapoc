package com.hcltech.rmg.flinkadapters.context;

import com.hcltech.rmg.common.ITimeService;

public record FlinkPipelineContext(
        IFlinkRetry retry,
        ITimeService timeService,
        IRandom random,
        FlinkCodecsFactory codecsFactory) {
}
