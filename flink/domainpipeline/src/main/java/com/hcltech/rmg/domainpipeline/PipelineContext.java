package com.hcltech.rmg.domainpipeline;

import com.hcltech.rmg.common.ITimeService;

import java.util.concurrent.ScheduledExecutorService;

public record PipelineContext (ScheduledExecutorService ses, ITimeService timeService){
}
