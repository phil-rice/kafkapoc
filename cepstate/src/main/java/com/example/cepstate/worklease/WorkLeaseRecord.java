package com.example.cepstate.worklease;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

/**
 * Note LeaseRecord can be null here. That implies that the work lease is not currently leased.
 *
 * @Param leaseRecordOrNull - the current lease record, or null if not leased
 * @Param processed - offsets that have been processed successfully
 * @Param backlog - offsets that are yet to be processed
 * @Param retryCount - the number of times the lease has been retried. 0 if this is the first time
 *
 */
public final class WorkLeaseRecord {
    private LeaseRecord leaseRecordOrNull;
    private final Set<Long> processed;
    private final Set<Long> failed;
    private final Deque<Long> backlog;

    public WorkLeaseRecord(LeaseRecord leaseRecordOrNull, Set<Long> processed, Set<Long> failed, Deque<Long> backlog
    ) {
        this.leaseRecordOrNull = leaseRecordOrNull;
        this.processed = processed;
        this.failed = failed;
        this.backlog = backlog;
    }

    public WorkLeaseRecord(LeaseRecord leaseRecordOrNull) {
        this(leaseRecordOrNull, new HashSet<>(), new HashSet<>(), new ArrayDeque<>());
    }


    public void nowProcessing(LeaseRecord leaseRecord) {
        this.leaseRecordOrNull = leaseRecord;
    }

    public void addToBacklog(Long offset) {
        if (processed.contains(offset)) return;
        if (backlog.contains(offset)) return;
        backlog.add(offset);

    }

    public void retry() {
        this.leaseRecordOrNull = leaseRecordOrNull == null ? null : leaseRecordOrNull.withIncreasedRetryCount();
    }

    public void addToProcessed(Long offset) {
        processed.add(offset);
    }

    public void clearLeaseRecord() {
        this.leaseRecordOrNull = null;
    }
    public void addToFailed(Long offset) {
        failed.add(offset);
    }

    public LeaseRecord leaseRecordOrNull() {
        return leaseRecordOrNull;
    }

    public Set<Long> processed() {
        return processed;
    }

    public Set<Long> failed() {
        return failed;
    }

    public Deque<Long> backlog() {
        return backlog;
    }

    @Override
    public String toString() {
        return "WorkLeaseRecord[" +
                "leaseRecordOrNull=" + leaseRecordOrNull + ", " +
                "processed=" + processed + ", " +
                "failed=" + failed + ", " +
                "backlog=" + backlog + ']';
    }

}
