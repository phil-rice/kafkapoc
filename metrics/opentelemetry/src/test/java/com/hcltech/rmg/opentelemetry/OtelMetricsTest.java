package com.hcltech.rmg.opentelemetry;

import com.hcltech.rmg.metrics.Metrics;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.*;

class OtelMetricsTest {

    private InMemoryMetricReader reader;
    private OpenTelemetrySdk otel;

    @BeforeEach
    void setUp() {
        reader = InMemoryMetricReader.create();
        SdkMeterProvider mp = SdkMeterProvider.builder()
                .registerMetricReader(reader)
                .build();
        otel = OpenTelemetrySdk.builder()
                .setMeterProvider(mp)
                .build();
        GlobalOpenTelemetry.resetForTest();
        GlobalOpenTelemetry.set(otel);
    }

    @AfterEach
    void tearDown() {
        GlobalOpenTelemetry.resetForTest();
        otel.close();
    }

    @Test
    void increment_emitsLongCounter_andAccumulates() {
        Metrics m = new OtelMetrics();

        m.increment("message.processed.total");
        m.increment("message.processed.total");
        m.increment("other.counter");

        Collection<MetricData> out = reader.collectAllMetrics();

        MetricData total = find(out, "message.processed.total");
        assertEquals(MetricDataType.LONG_SUM, total.getType());
        long totalSum = total.getLongSumData().getPoints().stream()
                .mapToLong(LongPointData::getValue).sum();
        assertEquals(2L, totalSum);
        assertTrue(total.getLongSumData().isMonotonic());

        MetricData other = find(out, "other.counter");
        long otherSum = other.getLongSumData().getPoints().stream()
                .mapToLong(LongPointData::getValue).sum();
        assertEquals(1L, otherSum);
    }

    @Test
    void histogram_emitsLongHistogram_withCountAndSum() {
        Metrics m = new OtelMetrics();

        m.histogram("message.processed.latency", 120);
        m.histogram("message.processed.latency", 80);

        Collection<MetricData> out = reader.collectAllMetrics();
        MetricData latency = find(out, "message.processed.latency");

        assertEquals(MetricDataType.HISTOGRAM, latency.getType());
        Collection<HistogramPointData> points = latency.getHistogramData().getPoints();
        assertEquals(1, points.size()); // one time series (no attributes)
        HistogramPointData p = points.iterator().next();
        assertEquals(2L, p.getCount());
        assertEquals(200.0, p.getSum(), 1e-6); // sums are double in data model
    }

    // --- helper ---
    private static MetricData find(Collection<MetricData> coll, String name) {
        for (MetricData md : coll) {
            if (md.getName().equals(name)) return md;
        }
        throw new AssertionError("Metric not found: " + name);
    }
}
