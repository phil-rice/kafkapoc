package com.hcltech.rmg.domainpipeline;

import com.hcltech.rmg.interfaces.outcome.Outcome;
import com.hcltech.rmg.interfaces.pipeline.ValueTC;
import com.hcltech.rmg.interfaces.repository.IPipelineRepository;
import com.hcltech.rmg.interfaces.repository.PipelineBuilder;
import com.hcltech.rmg.interfaces.repository.PipelineDetails;
import com.hcltech.rmg.interfaces.retry.RetryPolicyConfig;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

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
public class TrainingRepository implements IPipelineRepository<String, String> {
   public  static TrainingRepository instance = new TrainingRepository();

    public static ValueTC<String> stringValueTC = new ValueTC<String>() {
        @Override
        public String domainId(String value) {
            return value; // In this trivial example, the value itself is the domain ID
        }

        @Override
        public String msgId(String value) {
            return value; // In this trivial example, the value itself is the message ID
        }
    };
    public static PipelineDetails<String, String> details;

    static {
        RetryPolicyConfig retryPolicyConfig = new RetryPolicyConfig(Duration.ofSeconds(1), 2.0, Duration.ofSeconds(10), 5, .5);
        details = PipelineBuilder.builder(stringValueTC,
                        (stage,e) -> stage + " " + e.getClass().getSimpleName() + "-" + e.getLocalizedMessage(),
                        retryPolicyConfig, 500)
                .oneToOne("validate", String.class, e -> CompletableFuture.completedFuture(Outcome.value("validate-" + e)))
                .oneToOne("cepEnrichment", String.class, e -> CompletableFuture.completedFuture(Outcome.value("cep enrichment-" + e)))
                .oneToOne("enrichment", String.class, e -> CompletableFuture.completedFuture(Outcome.value("enrichment-" + e)))
                .oneToOne("bizLogic", String.class, e -> CompletableFuture.completedFuture(Outcome.value("bizlogic-" + e)))
                .build();
    }

    @Override
    public PipelineDetails<String, String> pipelineDetails() {
        return details;
    }
}
