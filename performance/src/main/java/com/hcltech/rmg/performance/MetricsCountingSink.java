// src/main/java/com/hcltech/rmg/performance/MetricsCountingSink.java
package com.hcltech.rmg.performance;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;

import java.io.Serializable;

public final class MetricsCountingSink<T> extends RichSinkFunction<T> implements Serializable {
  public enum Kind { VALUES, ERRORS, RETRIES }

  private static final long serialVersionUID = 1L;

  private final String label;
  private final Kind kind;

  // per-subtask metrics
  private transient Counter counter;
  private transient MeterView meter;

  public MetricsCountingSink(String label, Kind kind) {
    this.label = label;
    this.kind  = kind;
  }

  @Override
  public void open(OpenContext ctx) {
    // metrics hierarchy: perf.<label>.<kind>
    var mg = getRuntimeContext().getMetricGroup()
        .addGroup("perf").addGroup(label).addGroup(kind.name().toLowerCase());

    this.counter = mg.counter("events");
    this.meter   = new MeterView(counter, 60);      // 60s EWMA
    mg.gauge("rate_per_sec", meter::getRate);       // per-subtask rate

    // job-wide gauges (read from PerfStats)
    var agg = getRuntimeContext().getMetricGroup().addGroup("perf").addGroup("aggregate");
    agg.gauge("values_total",  PerfStats::values);
    agg.gauge("errors_total",  PerfStats::errors);
    agg.gauge("retries_total", PerfStats::retries);
  }

  @Override
  public void invoke(T value, Context ctx) {
    // subtask metrics
    counter.inc();

    // global totals (JVM-wide)
    switch (kind) {
      case VALUES -> PerfStats.incValues();
      case ERRORS -> PerfStats.incErrors();
      case RETRIES -> PerfStats.incRetries();
    }
  }
}
