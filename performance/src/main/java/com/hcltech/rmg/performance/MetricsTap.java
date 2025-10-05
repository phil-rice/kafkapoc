// src/main/java/com/hcltech/rmg/performance/MetricsTap.java
package com.hcltech.rmg.performance;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;

public final class MetricsTap<T> extends RichMapFunction<T, T> {
  public enum Kind { VALUES, ERRORS, RETRIES }

  private final String label;
  private final Kind kind;

  private transient Counter counter;
  private transient MeterView meter;

  public MetricsTap(String label, Kind kind) { this.label = label; this.kind = kind; }

  @Override
  public void open(OpenContext ctx) {
    var g = getRuntimeContext().getMetricGroup().addGroup("perf").addGroup(label).addGroup(kind.name().toLowerCase());
    this.counter = g.counter("events");
    this.meter   = new MeterView(counter, 60); // 60s EWMA
    g.gauge("rate_per_sec", meter::getRate);
  }

  @Override
  public T map(T value) {
    counter.inc();
    switch (kind) {
      case VALUES -> PerfStats.incValues();
      case ERRORS -> PerfStats.incErrors();
      case RETRIES -> PerfStats.incRetries();
    }
    return value;
  }
}
