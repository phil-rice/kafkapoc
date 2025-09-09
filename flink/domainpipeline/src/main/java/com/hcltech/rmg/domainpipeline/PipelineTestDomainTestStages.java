package com.hcltech.rmg.domainpipeline;

import com.hcltech.rmg.common.TestDomainMessage;
import com.hcltech.rmg.common.TestDomainTracker;
import com.hcltech.rmg.interfaces.outcome.Outcome;
import com.hcltech.rmg.interfaces.outcome.RetrySpec;
import com.hcltech.rmg.interfaces.pipeline.IOneToOnePipeline;
import com.hcltech.rmg.interfaces.pipeline.IOneToOneSyncPipeline;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public interface PipelineTestDomainTestStages {
    public static IOneToOnePipeline<TestDomainTracker, TestDomainTracker> delayAsync(
            String stageName, long ms, ScheduledExecutorService ses
    ) {
        return in -> {
            var cf = new CompletableFuture<Outcome<TestDomainTracker>>();
            ses.schedule(() -> cf.complete(Outcome.value(in.withTrack(stageName))), ms, MILLISECONDS);
            return cf;
        };
    }



    public static IOneToOnePipeline<TestDomainTracker, TestDomainTracker> delay(String stageName, long ms) {
        return in -> {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return CompletableFuture.completedFuture(Outcome.error(stageName + ": interrupted"));
            }
            return CompletableFuture.completedFuture(Outcome.value(in.withTrack(stageName)));
        };
    }

    /**
     * If the payload contains "Error:<stageName>" -> Errors
     * If it contains "Retry:<stageName>" -> Retry
     * Otherwise -> Value with prefix prepended.
     */
    public static IOneToOnePipeline<TestDomainTracker, TestDomainTracker> async(
            String stageName) {

        final String errToken = "Error:" + stageName;
        final String retryToken = "Retry:" + stageName;

        return in -> CompletableFuture.completedFuture(Outcome.value(in.withTrack(stageName)));
    }

    /**
     * If the payload contains "Error:<stageName>" -> Errors
     * If it contains "Retry:<stageName>" -> Retry
     * Otherwise -> Value with prefix prepended.
     */
    public static IOneToOneSyncPipeline<TestDomainTracker, TestDomainTracker> sync(
            String stageName) {

        final String errToken = "Error:" + stageName;
        final String retryToken = "Retry:" + stageName;

        return in -> Outcome.value(in.withTrack(stageName));

    }
}
