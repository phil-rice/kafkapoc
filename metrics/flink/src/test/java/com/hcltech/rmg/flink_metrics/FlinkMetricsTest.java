// src/test/java/com/hcltech/rmg/flink_metrics/FlinkMetricsTest.java
package com.hcltech.rmg.flink_metrics;

import com.hcltech.rmg.metrics.Metrics;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class FlinkMetricsTest {

    private MetricGroup scopedGroup(MetricGroup base) {
        // the ctor calls addGroup 4 times; just return the same mock to simplify
        when(base.addGroup(anyString(), anyString())).thenReturn(base);
        return base;
    }

    @Test
    void increment_createsCounterOnce_andIncTwice_noMeterWhenFlagFalse() {
        MetricGroup base = scopedGroup(mock(MetricGroup.class));
        Counter counter = mock(Counter.class);
        when(base.counter("processed")).thenReturn(counter);

        Metrics m = new FlinkMetrics(
                base, "rmg", "module", "Op", 0,
                /*maxNames*/ 100, /*createMeters*/ false,
                /*histFactory*/ name -> mock(Histogram.class),
                /*meterFactory*/ (n, c) -> mock(MeterView.class));

        m.increment("processed");
        m.increment("processed");

        verify(base, times(1)).counter("processed");   // created once
        verify(counter, times(2)).inc();               // incremented twice
        verify(base, never()).meter(anyString(), any()); // no meter when flag=false
    }

    @Test
    void increment_withMeters_registersMeterOnce_andUsesFactory() {
        MetricGroup base = scopedGroup(mock(MetricGroup.class));
        Counter counter = mock(Counter.class);
        when(base.counter("processed")).thenReturn(counter);

        MeterView mv = mock(MeterView.class);
        ArgumentCaptor<MeterView> mvCap = ArgumentCaptor.forClass(MeterView.class);

        Metrics m = new FlinkMetrics(
                base, "rmg", "module", "Op", 0,
                100, /*createMeters*/ true,
                name -> mock(Histogram.class),
                (n, c) -> {
                    assertEquals("processed.rate", n);
                    assertSame(counter, c);
                    return mv;
                });

        m.increment("processed");
        m.increment("processed");

        // meter registered once with the instance from our factory
        verify(base, times(1)).meter(eq("processed.rate"), mvCap.capture());
        assertSame(mv, mvCap.getValue());
    }

    @Test
    void histogram_registersHistogramOnce_andUpdatesWithValue() {
        MetricGroup base = scopedGroup(mock(MetricGroup.class));
        Histogram h = mock(Histogram.class);
        when(base.histogram(eq("latency_ms"), any())).thenReturn(h);

        Metrics m = new FlinkMetrics(
                base, "rmg", "module", "Op", 0,
                100, false,
                name -> {
                    assertEquals("latency_ms", name); // factory receives metric name
                    return h;
                },
                (n, c) -> mock(MeterView.class));

        m.histogram("latency_ms", 42);
        m.histogram("latency_ms", 7);

        verify(base, times(1)).histogram(eq("latency_ms"), same(h)); // created once
        verify(h, times(1)).update(42);
        verify(h, times(1)).update(7);
    }

    @Test
    void badName_throws() {
        MetricGroup base = scopedGroup(mock(MetricGroup.class));
        Metrics m = new FlinkMetrics(
                base, "rmg", "module", "Op", 0,
                100, false,
                name -> mock(Histogram.class),
                (n, c) -> mock(MeterView.class));

        assertThrows(IllegalArgumentException.class, () -> m.increment("bad name"));
    }

    @Test
    void capExceeded_throws() {
        MetricGroup base = scopedGroup(mock(MetricGroup.class));
        when(base.counter(anyString())).thenReturn(mock(Counter.class));
        when(base.histogram(anyString(), any())).thenReturn(mock(Histogram.class));

        Metrics m = new FlinkMetrics(
                base, "rmg", "module", "Op", 0,
                /*maxNames*/ 2, false,
                name -> mock(Histogram.class),
                (n, c) -> mock(MeterView.class));

        m.increment("a");
        m.histogram("b", 1L);
        assertThrows(IllegalStateException.class, () -> m.increment("c"));
    }
}
