package com.hcltech.rmg.common;

public record TestDomainTracker(String domainId, long count, String trackerId, long startTime, long latest) {
    public TestDomainTracker withTrack(ITimeService timeService, String passedThroughId) {
        return new TestDomainTracker(domainId, count, trackerId + " -> " + passedThroughId, startTime, timeService.currentTimeMillis());
    }

    public long duration() {
        return latest - startTime;
    }

    public TestDomainTracker withStart(long startTime) {
        return new TestDomainTracker(domainId, count, trackerId, startTime, latest);
    }
}
