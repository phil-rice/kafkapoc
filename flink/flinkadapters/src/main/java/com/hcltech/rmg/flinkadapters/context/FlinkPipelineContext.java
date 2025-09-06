package com.hcltech.rmg.flinkadapters.context;

import com.hcltech.rmg.common.ITimeService;
import com.hcltech.rmg.interfaces.retry.IRandom;

public record FlinkPipelineContext(
        ITimeService timeService,
        IRandom random,
        FlinkCodecsFactory codecsFactory) {
}
