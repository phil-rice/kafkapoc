package com.hcltech.rmg.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.function.UnaryOperator;

public interface WatermarkStrategyProvider<T> extends Supplier<WatermarkStrategy<T>> {

  // --- Common presets ---

  static <T> WatermarkStrategyProvider<T> none() {
    return WatermarkStrategy::noWatermarks;
  }

  /** Event-time with bounded out-of-order and a timestamp extractor. */
  static <T> WatermarkStrategyProvider<T> boundedOutOfOrder(
      Duration maxOutOfOrder, ToLongFunction<T> timestampExtractor) {
    Objects.requireNonNull(maxOutOfOrder, "maxOutOfOrder");
    Objects.requireNonNull(timestampExtractor, "timestampExtractor");
    return () -> WatermarkStrategy
        .<T>forBoundedOutOfOrderness(maxOutOfOrder)
        .withTimestampAssigner((e, ts) -> timestampExtractor.applyAsLong(e));
  }

  /** As above, plus idleness to prevent a quiet partition from stalling WMs. */
  static <T> WatermarkStrategyProvider<T> boundedOutOfOrderWithIdleness(
      Duration maxOutOfOrder, Duration idleness, ToLongFunction<T> timestampExtractor) {
    Objects.requireNonNull(idleness, "idleness");
    return () -> WatermarkStrategy
        .<T>forBoundedOutOfOrderness(maxOutOfOrder)
        .withIdleness(idleness)
        .withTimestampAssigner((e, ts) -> timestampExtractor.applyAsLong(e));
  }

  /** Monotonic (non-decreasing) timestamps per subtask. Rare, but useful when true. */
  static <T> WatermarkStrategyProvider<T> monotonous(ToLongFunction<T> timestampExtractor) {
    Objects.requireNonNull(timestampExtractor, "timestampExtractor");
    return () -> WatermarkStrategy
        .<T>forMonotonousTimestamps()
        .withTimestampAssigner((e, ts) -> timestampExtractor.applyAsLong(e));
  }

  /** Start from any base strategy and tweak with a builder. */
  static <T> WatermarkStrategyProvider<T> custom(UnaryOperator<WatermarkStrategy<T>> fn) {
    Objects.requireNonNull(fn, "fn");
    return () -> fn.apply(WatermarkStrategy.noWatermarks());
  }
}
