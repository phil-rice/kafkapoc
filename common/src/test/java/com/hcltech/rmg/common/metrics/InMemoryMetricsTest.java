package com.hcltech.rmg.common.metrics;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class InMemoryMetricsTest {

  @Test
  void increment_adds_one() {
    var m = new InMemoryMetrics();
    m.increment("foo");
    m.increment("foo");
    m.increment("bar");

    assertEquals(2L, m.counters.get("foo"));
    assertEquals(1L, m.counters.get("bar"));
  }

  @Test
  void histogram_records_values_in_order() {
    var m = new InMemoryMetrics();
    m.histogram("lat", 120);
    m.histogram("lat", 80);
    m.histogram("lat", 200);

    assertEquals(3, m.histograms.get("lat").size());
    assertEquals(120L, m.histograms.get("lat").get(0));
    assertEquals(80L,  m.histograms.get("lat").get(1));
    assertEquals(200L, m.histograms.get("lat").get(2));
  }
}
