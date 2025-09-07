package com.hcltech.rmg.domainpipeline;

import com.hcltech.rmg.interfaces.outcome.Outcome;
import com.hcltech.rmg.interfaces.outcome.RetrySpec;
import com.hcltech.rmg.interfaces.pipeline.IOneToOnePipeline;
import com.hcltech.rmg.interfaces.pipeline.IOneToOneSyncPipeline;

import java.util.concurrent.CompletableFuture;

public interface PipelineTestStages {


    public static IOneToOnePipeline<String, String> delay(String stageName, long ms) {
        return in -> {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return CompletableFuture.completedFuture(Outcome.error(stageName + ": interrupted"));
            }
            return CompletableFuture.completedFuture(Outcome.value(stageName + "=" + in));
        };
    }

    /**
     * If the payload contains "Error:<stageName>" -> Errors
     * If it contains "Retry:<stageName>" -> Retry
     * Otherwise -> Value with prefix prepended.
     */
    public static IOneToOnePipeline<String, String> async(
            String stageName, String prefix) {

        final String errToken = "Error:" + stageName;
        final String retryToken = "Retry:" + stageName;

        return in -> {
            if (in.contains(errToken)) {
                return CompletableFuture.completedFuture(
                        Outcome.error(stageName + ": forced error"));
            }
            if (in.contains(retryToken)) {
                return CompletableFuture.completedFuture(
                        Outcome.retry(new RetrySpec("forced retry at " + stageName)));
            }
            return CompletableFuture.completedFuture(
                    Outcome.value(prefix + in));
        };
    }

    /**
     * If the payload contains "Error:<stageName>" -> Errors
     * If it contains "Retry:<stageName>" -> Retry
     * Otherwise -> Value with prefix prepended.
     */
    public static IOneToOneSyncPipeline<String, String> sync(
            String stageName, String prefix) {

        final String errToken = "Error:" + stageName;
        final String retryToken = "Retry:" + stageName;

        return in -> {
            if (in.contains(errToken)) {
                return Outcome.error(stageName + ": forced error");
            }
            if (in.contains(retryToken)) {
                return Outcome.retry(new RetrySpec("forced retry at " + stageName));
            }
            return Outcome.value(prefix + in);
        };
    }
}
