# Event Flow Overview

## 1. Ingest
- **Source:** Kafka (parallelism = partitions, e.g. 12).
- **Timestamps & Watermarks:**  
  `forBoundedOutOfOrderness(say 5s)` + `withIdleness`  
  → guarantees event-time correctness and supports out-of-order events.

---

## 2. Key Extraction
- **Goal:** Find the domain key early (before heavy work).
- **Method:** Lightweight **StAX/SAX parser** to pluck the ID.
- **Result:** `(key, rawXml)` pairs.

---

## 3. KeyBy
- Route events by domain key.
- Ensures all state for a key is handled by the same subtask.

---

## 4. (Optional) Resequencer
- Buffer events per key in a small priority queue.
- Emit in timestamp order once watermark passes.
- Buffer horizon: ~5–6 seconds (out-of-orderness bound).
- Prevents async work from stalling watermarks.

---

## 5. Async OrderedWait Processing
Single **`AsyncDataStream.orderedWait`** operator (high concurrency).  
Inside this stage:

1. **XML/XSD validation** (CPU pool).
2. **Parse & extract fields** (CPU).
3. **Load CEP state copy** from RocksDB (cheap, in-memory).
4. **CEL validation** (CPU).
5. **Enrichment** (async I/O, idempotent).
6. **Business logic**:
    - Generate CEP mutation events (append-only).
    - Generate side-effect commands.
    - Capture errors.

> **Note:** Only append to RocksDB back on the operator thread.  
> Use `MapState<Long, Event>` for CEP event log (delta only, not a full snapshot).

---

## 6. CEP State Updates
- Append mutation events into RocksDB `MapState`.
- Update sequence counter in `ValueState<Long>`.
- Checkpointing: incremental (small, delta-only).
- Optional: enable **state changelog backend** for even lighter checkpoints.

---

## 7. Outputs
Split into multiple sinks:

- **CEP commands/mutations:** stay in RocksDB (per-key state).
- **Side-effect commands:** → Kafka outbox (transactional sink for exactly-once).
- **Errors/retries:** → Kafka DLQ or retry topic.

---

# Concurrency & Backpressure
- Source parallelism = partitions.
- Only one shuffle (`keyBy`).
- Concurrency via **orderedWait capacity** (hundreds–thousands).
- Pools:
    - CPU pool ≈ #cores (for XML/XSD + CEL).
    - I/O pool larger (or async client).
- `timeout`: just above “slow but acceptable”.
- Backpressure flows naturally from sinks → source.

---

# Properties
- ✅ **Event-time correctness** (5s disorder).
- ✅ **Minimal checkpoint size** (delta events only).
- ✅ **Per-key ordering** (`keyBy + orderedWait`).
- ✅ **Scalable**: concurrency in-operator, not via massive shuffle.
- ✅ **Exactly-once**: RocksDB + transactional Kafka sinks.
- ✅ **Geo-resilient**: externalized + incremental checkpoints (+ changelog optional).
- ✅ **Configurable**: rules/XSD/CEL delivered via broadcast config with versioning.

---

**Pipeline in one line:**

