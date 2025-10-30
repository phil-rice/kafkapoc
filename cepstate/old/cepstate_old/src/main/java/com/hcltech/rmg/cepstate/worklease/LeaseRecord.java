package com.hcltech.rmg.cepstate.worklease;

public record LeaseRecord(
    String domainId,
    long currentOffset,           // offset being processed
    String token,                 // fencing epoch/token
    long acquiredAt,
    int retryCount
) {

    public LeaseRecord withIncreasedRetryCount() {
        return new LeaseRecord(
            domainId,
            currentOffset,
            token,
            acquiredAt,
            retryCount + 1
        );
    }
}