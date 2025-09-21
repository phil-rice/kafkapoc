package com.hcltech.rmg.domainpipeline;

import com.hcltech.rmg.common.ITimeService;
import com.hcltech.rmg.common.TestDomainTracker;
import com.hcltech.rmg.interfaces.builder.PipelineBuilder;
import com.hcltech.rmg.interfaces.repository.IPipelineRepository;
import com.hcltech.rmg.interfaces.repository.PipelineDetails;
import com.hcltech.rmg.interfaces.retry.RetryPolicyConfig;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.hcltech.rmg.domainpipeline.PipelineTestDomainTestStages.*;

/**
 * Flink frankly sucks when it comes to dependency injection. This is because the job manager has to serialise the tasks
 * and give them to the task managers.
 * <p>
 * Now our retry logic needs 'defined names' so that we can retry from a specific node. So given the flink limitations
 * and the need for defined names we need to have a global variable for the pipelines that the task managers can use
 * We will pass the class name around at the job manager level so that we can do depenedency injection for tests
 * <p>
 * This is a training / test repository. It allows us to develop and test against a sample.
 */
public class TestDomainRepository implements IPipelineRepository<TestDomainTracker, TestDomainTracker> {
    public static TestDomainRepository instance = new TestDomainRepository();


    public static PipelineDetails<TestDomainTracker, TestDomainTracker> details;

    static {
        ScheduledExecutorService ses = Executors.newScheduledThreadPool(100);
        PipelineContext context = new PipelineContext(ses, ITimeService.real);
        RetryPolicyConfig retryPolicyConfig = new RetryPolicyConfig(Duration.ofSeconds(1), 2.0, Duration.ofSeconds(10), 5, .5);
        details = PipelineBuilder.<TestDomainTracker>builder(
                        (stage, e) -> stage + " " + e.getClass().getSimpleName() + "-" + e.getLocalizedMessage(),
                        retryPolicyConfig,
                        2000)
                .stage("loadConfig", TestDomainTracker.class, sync(context, "loadConfig"))
                .stage("validate", TestDomainTracker.class, sync(context, "validate"))
                .stage("transform", TestDomainTracker.class, sync(context, "transform"))
                .stage("cepEnrichment", TestDomainTracker.class, sync(context, "cepEnrichment"))
                .stage("enrichment", TestDomainTracker.class, delayAsync(context, "enrichment", 500))
                .stage("bizLogic", TestDomainTracker.class, delayAsync(context, "bizLogic", 500))
                .stage("check", TestDomainTracker.class, check(t -> t.duration() > 990, "Was too quick {1}. {0}", t -> List.of(t,t.duration())))
                .build();
    }

    @Override
    public PipelineDetails<TestDomainTracker, TestDomainTracker> pipelineDetails() {
        return details;
    }
}
