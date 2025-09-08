package com.hcltech.rmg.flinkadapters.envelopes;

public record PartLane(int partition, int lane) implements java.io.Serializable {}
