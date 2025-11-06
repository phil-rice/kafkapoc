#!/usr/bin/env bash
# Flink latency table (µs) every 2 s — aligned columns, robust label parsing.

ENDPOINT="http://localhost:9400/metrics"
INTERVAL=2
METRIC="NormalPipelineFunction_asyncInvoke_nanos"

while true; do
  clear
  echo "=== Flink latency metrics ($(date '+%H:%M:%S')) ==="

  # fetch once
  RAW="$(curl -s "$ENDPOINT" | grep "$METRIC" | grep 'quantile=')"

  # Extract tab-separated lines: subtask \t quantile \t value(ns)
  LINES="$(printf "%s\n" "$RAW" | awk '
    {
      subidx=""; q=""; val=$NF;
      if (match($0,/subtask_index="([0-9]+)"/,m)) subidx=m[1];
      else if (match($0,/subtask="([0-9]+)"/,m))   subidx=m[1];
      if (match($0,/quantile="([0-9.]+)"/,n))      q=n[1];
      if (subidx!="" && q!="") printf "%s\t%s\t%s\n", subidx, q, val;
    }')"

  SUBS="$(printf "%s\n" "$LINES" | awk -F'\t' '{print $1}' | sort -n | uniq)"
  QS="$(printf "%s\n" "$LINES" | awk -F'\t' '{print $2}' | sort -n | uniq)"

  echo
  echo "-- Per-subtask latency (µs) --"

  {
    # header
    printf "subtask"
    for q in $QS; do printf "\t%s" "$q"; done
    printf "\n"

    # rows
    for s in $SUBS; do
      printf "%s" "$s"
      for q in $QS; do
        val_ns="$(printf "%s\n" "$LINES" \
          | awk -F'\t' -v S="$s" -v Q="$q" '$1==S && $2==Q {print $3; exit}')"
        if [ -n "$val_ns" ]; then
          awk -v v="$val_ns" 'BEGIN{printf "\t%.3f", v/1000.0}'  # ns → µs
        else
          printf "\t-"
        fi
      done
      printf "\n"
    done
  } | column -t

  echo
  echo "-- Summary across subtasks (µs) --"

  # compact avg / max summary
  printf "%-8s" ""
  for q in $QS; do printf "%10s" "$q"; done
  printf "\n"

  printf "%-8s" "avg"
  for q in $QS; do
    avg_ns="$(printf "%s\n" "$LINES" | awk -F'\t' -v Q="$q" \
      '$2==Q {sum+=$3; n++} END{if(n>0) printf "%.9f", sum/n}')"
    if [ -n "$avg_ns" ]; then
      awk -v v="$avg_ns" 'BEGIN{printf "%10.3f", v/1000.0}'
    else
      printf "%10s" "-"
    fi
  done
  printf "\n"

  printf "%-8s" "max"
  for q in $QS; do
    max_ns="$(printf "%s\n" "$LINES" | awk -F'\t' -v Q="$q" \
      '$2==Q {if($3>m) m=$3} END{if(m>0) printf "%.9f", m}')"
    if [ -n "$max_ns" ]; then
      awk -v v="$max_ns" 'BEGIN{printf "%10.3f", v/1000.0}'
    else
      printf "%10s" "-"
    fi
  done
  printf "\n"

  echo
  sleep "$INTERVAL"
done
