package com.hcltech.rmg.domainpipeline.lease;

import com.hcltech.rmg.interfaces.outcome.Outcome;
import com.hcltech.rmg.interfaces.pipeline.IOneToManySyncPipeline;

import java.util.List;

public class LeasePipeline<M> implements IOneToManySyncPipeline<M, M> {

    private final LeaseCoordinator<M> leaseCoordinator;
    private final MessageTC<M> messageTC;

    public static <M> LeasePipeline<M> leasePipeline(LeaseCoordinator<M> coordinator, MessageTC<M> messageTC) {
        return new LeasePipeline<>(coordinator, messageTC);
    }

    public LeasePipeline(LeaseCoordinator<M> leaseCoordinator, MessageTC<M> messageTC) {
        this.leaseCoordinator = leaseCoordinator;
        this.messageTC = messageTC;
    }

    @Override
    public Outcome<List<M>> process(M m) {
        var hasLease = leaseCoordinator.acquire(messageTC.domainId(m), messageTC.messageId(m), messageTC.messageAcquireTime(m));
        Outcome<List<M>> result = hasLease ? Outcome.value(List.of(m)) : Outcome.value(List.of());
        return result;
    }
}
