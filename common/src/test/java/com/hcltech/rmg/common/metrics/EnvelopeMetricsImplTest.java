package com.hcltech.rmg.common.metrics;

import com.hcltech.rmg.common.ITimeService;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class EnvelopeMetricsImplTest {

    // Minimal typeclass stub so we don’t need real Envelopes
    record TestTC(String metricName, long startMs) implements IEnvelopeMetricsTC<Object> {
        @Override public String metricName(Object envelope) { return metricName; }
        @Override public long startProcessingTime(Object envelope) { return startMs; }
    }

    @Test
    void records_success_and_latency() {
        var mem = new InMemoryMetrics();
        var time = ITimeService.fixed(1_250L);                      // now = 1250ms
        var tc   = new TestTC("success", 1_000L);                   // start = 1000ms
        var sut  = EnvelopeMetrics.create(time, mem, tc);

        sut.addToMetricsAtEnd(new Object());

        assertEquals(1L, mem.counters.get("message.processed.total"));
        assertEquals(1L, mem.counters.get("message.processed.success"));
        assertEquals(1,  mem.histograms.get("message.processed.latency").size());
        assertEquals(250L, mem.histograms.get("message.processed.latency").get(0));
    }

    @Test
    void clamps_negative_latency_to_zero() {
        var mem = new InMemoryMetrics();
        var time = ITimeService.fixed(500L);                        // now earlier than start
        var tc   = new TestTC("error", 600L);                       // start = 600ms
        var sut  = EnvelopeMetrics.create(time, mem, tc);

        sut.addToMetricsAtEnd(new Object());

        assertEquals(1L, mem.counters.get("message.processed.total"));
        assertEquals(1L, mem.counters.get("message.processed.error"));
        assertEquals(0L,  mem.histograms.get("message.processed.latency").get(0));
    }

    @Test
    void multiple_calls_accumulate_counts_and_histogram_points() {
        var mem = new InMemoryMetrics();
        var time = ITimeService.fixed(2_000L);
        var sutSuccess = EnvelopeMetrics.create(time, mem, new TestTC("success", 1_800L)); // 200ms
        var sutRetry   = EnvelopeMetrics.create(time, mem, new TestTC("retry",   1_500L)); // 500ms

        sutSuccess.addToMetricsAtEnd(new Object());
        sutRetry.addToMetricsAtEnd(new Object());
        sutSuccess.addToMetricsAtEnd(new Object());

        assertEquals(3L, mem.counters.get("message.processed.total"));
        assertEquals(2L, mem.counters.get("message.processed.success"));
        assertEquals(1L, mem.counters.get("message.processed.retry"));

        var lat = mem.histograms.get("message.processed.latency");
        assertEquals(3, lat.size());
        // not asserting exact order, but you can if you want:
        // values should be [200, 500, 200] in this invocation order
    }
}
