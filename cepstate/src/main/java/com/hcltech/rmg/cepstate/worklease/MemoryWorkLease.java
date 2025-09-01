package com.hcltech.rmg.cepstate.worklease;

import com.hcltech.rmg.common.ITimeService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class MemoryWorkLease implements WorkLease {
    private final Map<String, WorkLeaseRecord> leases = new ConcurrentHashMap<>();
    private final ITokenGenerator tokenGenerator;
    private final ITimeService timeService;
    private final long maxRetries;
    private final Object createLock = new Object();

    public MemoryWorkLease(ITokenGenerator tokenGenerator, ITimeService timeService, long maxRetries) {
        this.tokenGenerator = tokenGenerator;
        this.timeService = timeService;
        this.maxRetries = maxRetries;
    }

    @Override
    public AcquireResult tryAcquire(String domainId, long offset) {
        var existingLease = leases.get(domainId);
        if (existingLease == null) {
            synchronized (createLock) {
                existingLease = leases.get(domainId);//double check locking
                if (existingLease == null) {
                    String token = tokenGenerator.next(domainId, offset);
                    leases.put(domainId, new WorkLeaseRecord(new LeaseRecord(domainId, offset, token, timeService.currentTimeMillis(), 0)));
                    return new AcquireResult("lease.acquired.firstTime", token); //
                }
            }
        }
        synchronized (existingLease) {
            var lr = existingLease.leaseRecordOrNull();
            if (lr != null) {
                if (offset == lr.currentOffset()) { //You already have the lease for this offset. This is a probably a retry
                    return new AcquireResult("lease.alreadyAcquired", lr.token());//Sure go ahead, you can retry this offset
                } else { //You have the lease for a different offset, so you can't process this one
                    existingLease.addToBacklog(offset);
                    return new AcquireResult("lease.addToBacklog"); //Nope you can't continue, but you have been added to backlog
                }
            }
            if (existingLease.processed().contains(offset))
                return new AcquireResult("lease.itemAlreadyProcessed");
            if (existingLease.failed().contains(offset))
                return new AcquireResult("lease.itemAlreadyFailed");
            //Now if there is a backlog item... then this item should be at the head
            var peek = existingLease.backlog().isEmpty() ? null : existingLease.backlog().peek();
            if (peek != null && peek != offset)
                throw new IllegalStateException("Backlog ordering exception for " + domainId + " offset is" + offset + " backlog is " + existingLease.backlog());
            if (peek != null)existingLease.backlog().pop();//we can ignore the value because we know it's the offset
            //we can acquire the lease: no one else is claiming it
            if (existingLease.processed().contains(offset))
                return new AcquireResult("lease.alreadyProcessed");//Nope you can't continue, you have already been processed
            var now = timeService.currentTimeMillis();
            var token = tokenGenerator.next(domainId, offset);
            existingLease.nowProcessing(new LeaseRecord(domainId, offset, token, now, 0));
            return new AcquireResult("lease.acquired.afterFirstTime", token); //Sure go ahead, you can process this offset
        }
    }


    @Override
    public SuceedResult succeed(String domainId, String token) {
        var existingLease = leases.get(domainId);
        if (existingLease == null)
            throw new IllegalStateException("No lease for " + domainId + " in succeed");
        synchronized (existingLease) {
            var lr = existingLease.leaseRecordOrNull();
            if (lr == null)
                throw new IllegalStateException("Lease record is null, when suceeding with " + domainId + " token: " + token);
            if (!lr.token().equals(token))
                throw new IllegalStateException("Invalid token. Expected " + lr.token() + " but had token");
            existingLease.addToProcessed(lr.currentOffset());
            existingLease.clearLeaseRecord();
            if (existingLease.backlog().isEmpty())
                return new SuceedResult("lease.succeed.noBacklog", null); // no backlog, nothing to process
            var backlogItem = existingLease.backlog().peek();
            return new SuceedResult("lease.succeed.nextBacklog", backlogItem); // next item in backlog to process
        }
    }

    @Override
    public FailResult fail(String domainId, String token) {
        var existingLease = leases.get(domainId);
        if (existingLease == null)
            throw new IllegalStateException("No lease for " + domainId + " in succeed");
        synchronized (existingLease) {
            var lr = existingLease.leaseRecordOrNull();
            if (lr == null)
                throw new IllegalStateException("Lease record is null in succeed for" + domainId);
            if (!lr.token().equals(token))
                throw new IllegalStateException("Token mismatch in fail. Expecting " + lr.token() + " but had token");
            if (lr.retryCount() >= maxRetries) { //giveup
                existingLease.addToFailed(lr.currentOffset());
                existingLease.clearLeaseRecord();
                if (existingLease.backlog().isEmpty())
                    return new FailResult("lease.fail.giveUp", null, false, 0); // give up, no more retries
                else
                    return new FailResult("lease.fail.giveUpWithBacklog", existingLease.backlog().peek(), false, 0); // give up, but there is backlog
            }
            //we can retry
            existingLease.retry();
            return new FailResult("lease.fail.retryScheduled", null, true, lr.retryCount() + 1);
        }
    }
}
