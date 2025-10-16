package com.hcltech.rmg.flink_metrics;

import com.hcltech.rmg.metrics.Metrics;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

class FlinkMetricsFactoryTest {
    @Test
    void factoryCreatesFlinkMetrics_boundToGroupAndOperator() {
        MetricGroup group = mock(MetricGroup.class);
        when(group.addGroup(anyString(), anyString())).thenReturn(group); // <-- chain safely

        Counter counter = mock(Counter.class);
        when(group.counter("processed")).thenReturn(counter);            // <-- used by increment()

        FlinkMetricsParams p = new FlinkMetricsParams(group, FlinkMetricsFactoryTest.class, 0);

        FlinkMetricsFactory f = new FlinkMetricsFactory("rmg","notification", 100, false);
        Metrics m = f.create(p);

        m.increment("processed");

        verify(group, times(1)).counter("processed");
        verify(counter, times(1)).inc();
    }

}
