#!/usr/bin/env bash
# Periodically capture key Flink Prometheus metrics and tee them to screen + log.
# Survives curl errors; rotates every 2 seconds by default.
#
# Usage:
#   ./flink-snap.sh -p all -o "EnvelopeAsyncProcessing" -P 9400 -i 2
#
# Ctrl+C to stop.  Logs go to ./logs/flink-snap_<profile>_<op>_<timestamp>.log

set -u
PROFILE="ops"                 # ops | source | storage | all
OP="EnvelopeAsyncProcessing"  # operator/task name hint
PORT=9400
INTERVAL=2
LOG_DIR="./logs"
LOG_FILE=""

while getopts ":p:o:P:i:f:" opt; do
  case "$opt" in
    p) PROFILE="$OPTARG" ;;
    o) OP="$OPTARG" ;;
    P) PORT="$OPTARG" ;;
    i) INTERVAL="$OPTARG" ;;
    f) LOG_FILE="$OPTARG" ;;
    *) echo "Usage: $0 -p {ops|source|storage|all} -o OP -P PORT -i SEC [-f logfile]"; exit 1 ;;
  esac
done

mkdir -p "$LOG_DIR"
TS=$(date +"%Y%m%d-%H%M%S")
: "${LOG_FILE:=$LOG_DIR/flink-snap_${PROFILE}_${OP// /_}_${TS}.log}"

echo "# flink-snap profile=$PROFILE op=\"$OP\" port=$PORT interval=${INTERVAL}s started=$(date -Is)" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

safe_curl() {
  curl -m 1 -sS "http://localhost:${PORT}/metrics" 2>/dev/null || true
}

print_hdr() {
  echo "=== $(date -Is) — PROFILE=${PROFILE} OP='${OP}' PORT=${PORT} ==="
}

section() { echo -e "\n[$1]"; }

tick() {
  print_hdr

  case "$PROFILE" in
    ops|all)
      section "Operator rates & time split"
      safe_curl | \
        egrep "($OP|task_name=\"$OP\"|operator=\"${OP}\"|${OP}.*operator|${OP}.*subtask)" | \
        egrep -i 'numRecords(In|Out)PerSecond|accumulate(BackPressured|Busy|Idle)TimeMs' | \
        sed -E 's/.*(numRecordsInPerSecond|numRecordsOutPerSecond|accumulateBackPressuredTimeMs|accumulateBusyTimeMs|accumulateIdleTimeMs)[^}]+"?[^"]*"?},?[[:space:]]*([0-9.e+-]+)/\1=\2/' \
        | sort || echo "(no operator metrics)"

      section "Async pool (if exposed)"
      safe_curl | egrep 'async\.(pool|simple|direct)|\{.*(active|queue|poolSize|largest)=' || echo "(no pool metrics)"
      ;;&

    source|all)
      section "Kafka / upstream feed"
      safe_curl | egrep -i 'records_consume.*persecond|records_consumed_total|records_lag_max|numRecordsOutPerSecond.*task_name="Map"|numRecordsOutPerSecond\{[^}]*source' \
        | sed -E 's/.*(records_consume[^}]*persecond|records_consumed_total|records_lag_max|numRecordsOutPerSecond)[^}]+"?[^"]*"?},?[[:space:]]*([0-9.e+-]+)/\1=\2/' \
        | sort || echo "(no source metrics)"
      ;;&

    storage|all)
      section "Backpressure (accumulated)"
      safe_curl | egrep -i 'accumulateBackPressuredTimeMs' | grep -F "$OP" || echo "(no backpressure metrics)"

      section "RocksDB (if enabled)"
      safe_curl | grep -i 'rocksdb' | egrep 'compaction|immutable|live_sst|stall|write|wal|flush' | head -n 40 || echo "(no rocksdb metrics)"
      ;;&
  esac

  echo -e "\n"
}

trap 'echo "Stopping. Log: $LOG_FILE"; exit 0' INT TERM

while :; do
  tick | tee -a "$LOG_FILE"
  sleep "$INTERVAL"
done
