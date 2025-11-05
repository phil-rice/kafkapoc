#!/usr/bin/env bash
# gc_overhead_loop_ms.sh
# Continuously shows JVM GC time and overhead percentage in milliseconds.

set -euo pipefail

ENDPOINT="${1:-http://localhost:9400/metrics}"
INTERVAL="${2:-5}"

echo "Polling $ENDPOINT every ${INTERVAL}s..."
echo "Press Ctrl+C to stop."
echo

print_metrics() {
  local METRICS
  if ! METRICS="$(curl -fsS "$ENDPOINT")"; then
    echo "$(date '+%H:%M:%S')  Error: failed to fetch metrics"
    return
  fi

  first_metric_value() {
    local name="$1"
    echo "$METRICS" \
      | grep -E "^${name}(\{| )" \
      | head -n1 \
      | awk '{print $NF}'
  }

  sum_metric() {
    local name="$1"
    echo "$METRICS" \
      | grep -E "^${name}(\{| )" \
      | awk '{s+=$NF} END {printf "%.9f\n", s+0}'
  }

  START_TS="$(first_metric_value process_start_time_seconds || true)"
  UPTIME_SEC="$(first_metric_value process_uptime_seconds || true)"
  ELAPSED=""

  if [[ -n "${START_TS:-}" ]]; then
    NOW="$(date +%s)"
    ELAPSED="$(awk -v now="$NOW" -v start="$START_TS" 'BEGIN{printf "%.6f", now-start}')"
  elif [[ -n "${UPTIME_SEC:-}" ]]; then
    ELAPSED="$(awk -v up="$UPTIME_SEC" 'BEGIN{printf "%.6f", up+0}')"
  fi

  PAUSE_SUM_MICRO="$(sum_metric jvm_gc_pause_seconds_sum || true)"
  PAUSE_SUM_PROM="$(sum_metric jvm_gc_collection_seconds_sum || true)"

  GC_PAUSE_SEC="0"
  if [[ -n "${PAUSE_SUM_MICRO:-}" && "$PAUSE_SUM_MICRO" != "0.000000000" ]]; then
    GC_PAUSE_SEC="$PAUSE_SUM_MICRO"
  elif [[ -n "${PAUSE_SUM_PROM:-}" && "$PAUSE_SUM_PROM" != "0.000000000" ]]; then
    GC_PAUSE_SEC="$PAUSE_SUM_PROM"
  fi

  # Convert to milliseconds for readability
  GC_PAUSE_MS="$(awk -v s="$GC_PAUSE_SEC" 'BEGIN{printf "%.3f", s*1000}')"
  ELAPSED_MS="$(awk -v s="$ELAPSED" 'BEGIN{printf "%.3f", s*1000}')"

  OVERHEAD_PCT=""
  if [[ -n "$ELAPSED" && "$ELAPSED" != "0" ]]; then
    OVERHEAD_PCT="$(awk -v gc="$GC_PAUSE_SEC" -v t="$ELAPSED" 'BEGIN{printf "%.6f", (gc*100.0)/t}')"
  fi

  TS="$(date '+%H:%M:%S')"
  if [[ -n "$OVERHEAD_PCT" ]]; then
    printf "%s  GC Pause: %10.3f ms | Elapsed: %12.3f ms | Overhead: %8.6f%%\n" \
      "$TS" "$GC_PAUSE_MS" "$ELAPSED_MS" "$OVERHEAD_PCT"
  else
    printf "%s  GC Pause: %10.3f ms\n" "$TS" "$GC_PAUSE_MS"
  fi
}

while true; do
  print_metrics
  sleep "$INTERVAL"
done
