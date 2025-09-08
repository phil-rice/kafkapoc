package com.hcltech.rmg.common;

public record TestDomainTracker(String domainId, long count, String trackerId) {
    public TestDomainTracker withTrack(String passedThroughId) {
        return new TestDomainTracker(domainId, count, trackerId + " -> " + passedThroughId);
    }
}
