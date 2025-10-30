package com.hcltech.rmg.flinkadapters;

public class OrderPreservingAsyncExecutorContractTest extends AbstractEnvelopeAsyncProcessingFunctionTest {
    @Override protected int poolThreads() { return 8; }
    @Override protected int laneCount() { return 64; }
    @Override protected int laneDepth() { return 8; }
    @Override protected int maxInFlight() { return 64; }
    @Override protected int timeoutMs() { return 200; }
}
