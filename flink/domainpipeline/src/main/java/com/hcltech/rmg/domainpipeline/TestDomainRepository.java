package com.hcltech.rmg.domainpipeline;

import com.hcltech.rmg.common.TestDomainMessage;
import com.hcltech.rmg.common.TestDomainTracker;
import com.hcltech.rmg.interfaces.builder.PipelineBuilder;
import com.hcltech.rmg.interfaces.repository.IPipelineRepository;
import com.hcltech.rmg.interfaces.repository.PipelineDetails;
import com.hcltech.rmg.interfaces.retry.RetryPolicyConfig;

import java.time.Duration;
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
        RetryPolicyConfig retryPolicyConfig = new RetryPolicyConfig(Duration.ofSeconds(1), 2.0, Duration.ofSeconds(10), 5, .5);
        details = PipelineBuilder.<TestDomainTracker>builder(
                        (stage, e) -> stage + " " + e.getClass().getSimpleName() + "-" + e.getLocalizedMessage(),
                        retryPolicyConfig,
                        2000)
                .stage("validate", TestDomainTracker.class, sync("validate"))
                .stage("cepEnrichment", TestDomainTracker.class, sync("cepEnrichment"))
                .stage("enrichment", TestDomainTracker.class, delayAsync("enrichment", 500,ses))
                .stage("bizLogic", TestDomainTracker.class, delayAsync("bizLogic", 500,ses))
                .build();
    }

    @Override
    public PipelineDetails<TestDomainTracker, TestDomainTracker> pipelineDetails() {
        return details;
    }
}
