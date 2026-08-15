# Outboxify State Machine & Lifecycle Specification

## 1. Overview

The Outboxify transactional outbox engine orchestrates state transitions for outbox event records across two complementary dispatch paths:
1. **Fast-Path (Commit Hook Dispatch):** Triggered immediately after the enclosing database transaction commits. Provides ultra-low latency (< 50ms) publishing.
2. **Slow-Path (Background Poller):** Queries pending records in batches using concurrency-safe row locking (`FOR UPDATE SKIP LOCKED` / `READPAST`). Catches up uncommitted, retried, or failed records.
3. **Watchdog (Reaper Job):** Identifies rows stuck in `PROCESSING` status beyond a configured timeout threshold (e.g., due to pod crashes or network partitions) and safely transitions them back to `FAILED` or `NEW`.

---

## 2. Canonical State Model

| State | Description | Next Permitted States |
| :--- | :--- | :--- |
| `NEW` | Record is committed into database. Ready to be dispatched. | `PROCESSING`, `SENT`, `FAILED` |
| `PROCESSING` | Record locked/claimed by poller batch worker. Message is in-flight to broker. | `SENT`, `FAILED`, `NEW` (via Reaper) |
| `SENT` | Broker acknowledged receipt (e.g., Kafka ACK `acks=all`). Terminal success state. | *(None)* |
| `FAILED` | Broker send encountered error or retry threshold exceeded. Eligible for poller retry if retry count < max retries. | `PROCESSING`, `DEAD_LETTER` |

---

## 3. Transition Rules & Sequence

```
                        +-----------------------------+
                        |      DB Record Created      |
                        |        Status: NEW          |
                        +--------------+--------------+
                                       |
                +----------------------+----------------------+
                |                                             |
     [Fast-Path: Commit Hook]                       [Slow-Path: Background Poller]
                |                                             |
                v                                             v
      (afterCommit Trigger)                        (Fetch SKIP LOCKED Batch)
                |                                             |
                v                                             v
       Send to Kafka Async                         Mark Batch: PROCESSING
                |                                             |
       +--------+--------+                                    v
       |                 |                          Send Batch to Kafka
   (Kafka ACK)      (Kafka Fail)                              |
       |                 |                            +-------+-------+
       v                 v                            |               |
  Mark: SENT        Mark: FAILED                 (Kafka ACK)     (Kafka Fail / Crash)
                         |                            |               |
                         |                            v               v
                         +------------------->   Mark: SENT     [Reaper Watchdog]
                                                                 Resets to FAILED/NEW
```

### 3.1 Fast-Path Transitions
1. Application begins transaction and inserts domain entity / outbox record with `status = 'NEW'`.
2. Transaction commits successfully.
3. `afterCommit` hook invokes `BrokerPublisher.publish(...)` asynchronously.
4. On Kafka ACK: Update record `status = 'SENT'`, `processed_at = CURRENT_TIMESTAMP`.
5. On Kafka Error: Update record `status = 'FAILED'`, increment `retry_count`, record `last_error`. The slow-path poller will pick up the record on next cycle.

### 3.2 Slow-Path Transitions
1. Poller queries batch of records with `status IN ('NEW', 'FAILED') AND retry_count < max_retries` using dialect-specific locking (`FOR UPDATE SKIP LOCKED`).
2. Atomically marks the batch `status = 'PROCESSING'`, `updated_at = CURRENT_TIMESTAMP`.
3. Dispatches batch to broker asynchronously with non-blocking futures.
4. On batch ACK: Marks succeeded IDs as `SENT`, `processed_at = CURRENT_TIMESTAMP`.
5. On individual record failure: Marks as `FAILED`, increments `retry_count`, sets `last_error`.

### 3.3 Reaper Watchdog Transitions
1. Reaper runs periodically (e.g., every 10s).
2. Finds records where `status = 'PROCESSING'` AND `updated_at < (NOW - processing_timeout)`.
3. If `retry_count + 1 < max_retries`, transitions record back to `FAILED` (or `NEW`) and increments `retry_count`.
4. If `retry_count + 1 >= max_retries`, transitions record to `FAILED` with error message `"PROCESSING_TIMEOUT_EXCEEDED"`.
