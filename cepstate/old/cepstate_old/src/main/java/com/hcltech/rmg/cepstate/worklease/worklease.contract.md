# Work Lease — Contract & Invariants


## Summary
- This component enforces **exclusive, FIFO processing per `domainId`** using a lease + fencing token.
- It maintains a **backlog** and **idempotence**; it **does not** own backoff timing or scheduling.
- **External orchestration** decides when to (re)run things and calls `tryAcquire(domainId, offset)`.
- `succeed` / `fail` **do not hand off**; they clear the lease and return a **hint** (peek at backlog head).
- **No “sleeping” state** is modeled here. Timing/backoff is computed outside from `acquiredAtMillis` and `retryCount`.
- **Hard bugs throw** (token mismatch, out-of-order acquire, missing lease on finish).

---

## Responsibilities & Non-Responsibilities

**This component**
- One in-flight offset per domain (exclusive lease + fencing).
- FIFO backlog per domain with de-duplication.
- Idempotence (terminal offsets aren’t reprocessed).
- Token validation on `succeed` / `fail`.

**External orchestrator**
- Backoff/jitter/timers and *when* to (re)attempt.
- Calls `tryAcquire` to progress work.
- Consumes “next backlog head” hints from `succeed` / `fail`.

> No automatic hand-off inside `succeed` / `fail`.

---

## Per-Domain Data Model

- **LeaseRecord**
    - `currentOffset: long`
    - `token: String`
    - `acquiredAtMillis: long` — set on first acquisition of **this offset**; **never changes**.
    - `retryCount: int` — number of `fail` calls for `currentOffset`.

- **processed: Set<Long>**  
  Offsets that are **terminal** (includes **SUCCESS** and **FAILED_FINAL**) for idempotence.

- **failed: Set<Long>** *(optional ledger)*  
  If kept separately, `tryAcquire` **must** treat `failed` offsets as terminal as well.  
  *(You may instead store failed-final directly in `processed` and drop this set.)*

- **backlog: Deque<Long> (FIFO)**  
  Offsets queued behind the current one. **No duplicates; no entries present in `processed` or `failed`.**

---

## Observable States

- **In-flight:** `leaseRecord != null` → domain is working the `currentOffset` (subject to external backoff).
- **Idle:** `leaseRecord == null` → nothing in flight; orchestrator may advance backlog.

> There is **no “sleeping” state** in this component.

---

## Global Invariants (per domain)

1. **Single in-flight unit**  
   At most one `LeaseRecord`, referencing exactly one `currentOffset`.

2. **Disjointness**  
   `currentOffset ∉ (processed ∪ failed ∪ backlog)`  
   `processed ∩ backlog = ∅`, `failed ∩ backlog = ∅`.

3. **FIFO discipline**  
   Backlog is only drained in `tryAcquire`, and **only** by removing the **head** that matches the offset being acquired.

4. **Idempotence**  
   If `offset ∈ processed ∪ failed`, it is never re-enqueued nor re-acquired.

5. **Token fencing**  
   `succeed` / `fail` are valid **only** when `token == lease.token`.

6. **Stable monitor**  
   The per-domain `WorkLeaseRecord` used for locking is **never replaced** after creation.

---

## Method Contracts

### `AcquireResult tryAcquire(domainId, offset)`

**Under the per-domain lock:**

1) **Terminal checks**
    - If `offset ∈ processed` ⇒ return `ALREADY_PROCESSED`.
    - If `offset ∈ failed` ⇒ return `ALREADY_FAILED`.

2) **Active lease exists**
    - If `offset == lease.currentOffset` ⇒ return `ALREADY_ACQUIRED(token)`.
    - Else ⇒ `addToBacklog(offset)` (dedupe + idempotence inside) and return `ENQUEUED`.

3) **No active lease**
    - If backlog **non-empty**:
        - Let `head = backlog.peekFirst()`.  
          If `head != offset` ⇒ **throw `BacklogOrderingException(domainId, expected=head, got=offset)`**.  
          Else **remove head** (pop) and continue.
    - Generate `token`, set `lease = (offset, token, acquiredAtMillis = now, retryCount = 0)`.  
      Return `ACQUIRED(token)`.

**Postconditions**
- On `ACQUIRED`: a lease exists for `offset`, and that offset is not in backlog.
- On `ENQUEUED` / `ALREADY_*`: invariants remain satisfied.

---

### `SucceedResult succeed(domainId, token)`

**Under the per-domain lock:**

1) If no `leaseRecord` ⇒ **IllegalStateException("No lease…")**.
2) If `token != lease.token` ⇒ **IllegalStateException("Token mismatch…")**.
3) Add `lease.currentOffset` to `processed`.
4) Clear `leaseRecord`.
5) If backlog empty ⇒ return `NO_BACKLOG`.  
   Else ⇒ return `NEXT_HINT(backlog.peekFirst())` (do **not** remove it).

**Postconditions**
- Offset is terminal (`processed`).
- Lease is cleared; backlog unchanged.

---

### `FailResult fail(domainId, token)`

**Under the per-domain lock:**

1) If no `leaseRecord` ⇒ **IllegalStateException**.
2) If `token != lease.token` ⇒ **IllegalStateException**.

3) **Final give-up** when `retryCount >= maxRetries`:
    - Add offset to **failed** *(and/or to `processed` if you unify terminal sets)*.
    - Clear `leaseRecord`.
    - If backlog empty ⇒ return `GIVE_UP_NO_BACKLOG`.  
      Else ⇒ return `GIVE_UP_NEXT_HINT(backlog.peekFirst())`.

4) **Retry scheduled** (non-final):
    - Increment `retryCount` (leave `acquiredAtMillis` **unchanged** by design).
    - Keep the lease on the same `currentOffset`.
    - Return `RETRY_SCHEDULED(updatedRetryCount)`.

**Postconditions**
- Final failure: offset terminal; lease cleared.
- Non-final: `retryCount` increased; lease retained.

---

## Error Policy (Hard Bugs → Exceptions)

- **BacklogOrderingException** — `tryAcquire` attempted on a non-head while backlog non-empty.
- **TokenMismatchException** — token differs from active lease on `succeed` / `fail`.
- **LeaseNotFoundException / IllegalStateException** — `succeed` / `fail` called with no active lease.

*(In production you may convert to result codes; tests should treat these as hard errors.)*

---

## Locking Rules

- Creation serialized (double-checked with a global `createLock`, or an atomic create-if-absent step).
- After creation, **all** reads/writes of `lease`, `processed`, and `backlog` occur under `synchronized (workLeaseRecord)`.
- Do not hold the lock across external I/O; record intents inside the lock, perform side-effects after unlocking.

---

## Helper Semantics

`addToBacklog(offset)` must:
- Return immediately if `offset ∈ processed ∪ failed`.
- Return immediately if `offset` is already present in the backlog.
- Otherwise append at the tail (FIFO).

---

## Sanity Test Scenarios

1. **Happy path**  
   `tryAcquire(A,1)` → `ACQUIRED`; `succeed(A,token)` → `NO_BACKLOG`.

2. **Queue then orchestrated hand-off**  
   `tryAcquire(A,1)` → `ACQUIRED`; `tryAcquire(A,2)` → `ENQUEUED`;  
   `succeed(A,token)` → `NEXT_HINT(2)`;  
   `tryAcquire(A,2)` (head matches, popped) → `ACQUIRED`.

3. **Out-of-order acquire (hard bug)**  
   After (2), calling `tryAcquire(A,3)` before 2 ⇒ **BacklogOrderingException**.

4. **Retry**  
   `fail(A,token)` (under limit) → `RETRY_SCHEDULED(n)`; lease retained; `acquiredAtMillis` unchanged.

5. **Final give-up**  
   After reaching `maxRetries`, `fail(A,token)` → `GIVE_UP_*`; lease cleared; `offset ∈ failed` (and `processed` if unified).

6. **Idempotence**  
   Once an offset is terminal, any `tryAcquire(A,thatOffset)` returns `ALREADY_*` and does not re-enqueue.
