package com.hcltech.rmg.kafka;

import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.metrics.MetricGroup;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.ToLongFunction;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class WatermarkStrategyProviderTest {

    // Helper: Flink 2.0 requires a Context with a MetricGroup.
    private static TimestampAssignerSupplier.Context ctx() {
        TimestampAssignerSupplier.Context ctx = mock(TimestampAssignerSupplier.Context.class);
        when(ctx.getMetricGroup()).thenReturn(mock(MetricGroup.class));
        return ctx;
    }

    // ---------- Null contract tests ----------

    @Test
    void boundedOutOfOrder_requires_maxOutOfOrder() {
        ToLongFunction<String> extractor = s -> 42L;
        NullPointerException ex = assertThrows(
                NullPointerException.class,
                () -> WatermarkStrategyProvider.boundedOutOfOrder(null, extractor)
        );
        assertEquals("maxOutOfOrder", ex.getMessage());
    }

    @Test
    void boundedOutOfOrder_requires_timestampExtractor() {
        Duration max = Duration.ofSeconds(5);
        NullPointerException ex = assertThrows(
                NullPointerException.class,
                () -> WatermarkStrategyProvider.boundedOutOfOrder(max, null)
        );
        assertEquals("timestampExtractor", ex.getMessage());
    }

    @Test
    void boundedOutOfOrderWithIdleness_requires_idleness() {
        Duration max = Duration.ofSeconds(5);
        ToLongFunction<String> extractor = s -> 1L;
        NullPointerException ex = assertThrows(
                NullPointerException.class,
                () -> WatermarkStrategyProvider.boundedOutOfOrderWithIdleness(max, null, extractor)
        );
        assertEquals("idleness", ex.getMessage());
    }

    @Test
    void monotonous_requires_timestampExtractor() {
        NullPointerException ex = assertThrows(
                NullPointerException.class,
                () -> WatermarkStrategyProvider.monotonous(null)
        );
        assertEquals("timestampExtractor", ex.getMessage());
    }

    @Test
    void custom_requires_fn() {
        NullPointerException ex = assertThrows(
                NullPointerException.class,
                () -> WatermarkStrategyProvider.custom(null)
        );
        assertEquals("fn", ex.getMessage());
    }

    // ---------- Wiring tests (Flink 2.0 API) ----------

    @Test
    void boundedOutOfOrder_uses_given_extractor_for_timestamp_assigner() {
        Duration max = Duration.ofMillis(300);
        @SuppressWarnings("unchecked")
        ToLongFunction<String> extractor = mock(ToLongFunction.class);

        when(extractor.applyAsLong("a")).thenReturn(100L);
        when(extractor.applyAsLong("b")).thenReturn(200L);

        WatermarkStrategyProvider<String> p =
                WatermarkStrategyProvider.boundedOutOfOrder(max, extractor);
        WatermarkStrategy<String> strategy = p.get();

        var assigner = strategy.createTimestampAssigner(ctx());

        long t1 = assigner.extractTimestamp("a", 0L);
        long t2 = assigner.extractTimestamp("b", 100L);

        assertEquals(100L, t1);
        assertEquals(200L, t2);
        verify(extractor).applyAsLong("a");
        verify(extractor).applyAsLong("b");
    }

    @Test
    void monotonous_uses_given_extractor_for_timestamp_assigner() {
        @SuppressWarnings("unchecked")
        ToLongFunction<Integer> extractor = mock(ToLongFunction.class);
        when(extractor.applyAsLong(7)).thenReturn(700L);

        WatermarkStrategy<Integer> strategy =
                WatermarkStrategyProvider.monotonous(extractor).get();

        var assigner = strategy.createTimestampAssigner(ctx());

        long ts = assigner.extractTimestamp(7, -1L);

        assertEquals(700L, ts);
        verify(extractor).applyAsLong(7);
    }

    @Test
    void custom_applies_function_starting_from_noWatermarks() {
        AtomicBoolean called = new AtomicBoolean(false);

        UnaryOperator<WatermarkStrategy<String>> fn = base -> {
            called.set(true);
            // Make the returned strategy observable in the test by adding an assigner.
            return base.withTimestampAssigner((e, prev) -> 999L);
        };

        WatermarkStrategy<String> s = WatermarkStrategyProvider.custom(fn).get();

        var assigner = s.createTimestampAssigner(ctx());
        long ts = assigner.extractTimestamp("anything", 0L);

        assertTrue(called.get(), "custom function should be invoked");
        assertEquals(999L, ts);
    }

    @Test
    void none_returns_strategy_usable_after_adding_assigner() {
        WatermarkStrategy<String> s = WatermarkStrategyProvider.<String>none().get();

        // Add an assigner to make it usable for timestamp extraction in this test.
        var withAssigner = s.withTimestampAssigner((e, prev) -> 5L);

        var assigner = withAssigner.createTimestampAssigner(ctx());
        long ts = assigner.extractTimestamp("x", 0L);

        assertEquals(5L, ts);
    }
}
