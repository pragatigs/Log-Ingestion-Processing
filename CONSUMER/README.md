# logs-worker — Kafka Consumer / Log Processing Job

A short-lived Kubernetes worker that consumes log messages from Kafka, processes them concurrently, writes them to per-type output files, and exits. Scaled automatically by KEDA based on Kafka consumer lag.

This document explains **why** the system is built this way, **what** each piece does, **where** it lives in the codebase, and **how** the pieces fit together — written for a reviewer seeing this for the first time.

---

## 1. What problem this solves

Log messages of different types (`info`, `debug`, `error`, `warn`) arrive on a Kafka topic, one type per partition. Instead of running a permanently-on consumer service, this system spins up a pod **only when there is a backlog**, has it process a small batch, and then **exits** — no idle pods, no standing compute cost when there's nothing to do.

---

## 2. Why a Job, not a Deployment

| | Deployment | Job |
|---|---|---|
| Lifecycle | Runs forever, restarts on exit | Runs to completion, then stops |
| Fits "process batch and exit"? | No — fights the exit | Yes — this is what it's for |

A Deployment would treat a clean exit as a failure to fix and keep restarting the pod forever. A `Job` (and KEDA's `ScaledJob`, which creates Jobs dynamically) is built for exactly this run-to-completion pattern.

---

## 3. Why KEDA, and why `ScaledJob` specifically

KEDA watches **Kafka consumer group lag** — how many messages are waiting, unprocessed, for `log-processing-group` — and creates new Job instances proportional to that backlog.

KEDA has two resource types:
- `ScaledObject` — scales **Deployment replicas**. Wrong fit here.
- `ScaledJob` — creates **new Job instances** on demand. This is what's used.

**How scaling is calculated:** `desired job count = ceil(total lag ÷ lagThreshold)`. `lagThreshold` is set to the same value as the batch size (`KAFKA_MAX_POLL_RECORDS`), so "N messages waiting" reliably maps to "N ÷ batchSize pods."

Location: `templates/worker/scaledjob.yaml`

---

## 4. Component breakdown — what each class does and why it's separate

```
LogPoller           → reads from Kafka (single-threaded)
LogMessageProcessor  → processes ONE message (partition → file, with dedup)
BatchExecutor        → runs LogMessageProcessor concurrently across a batch
OffsetGuard          → Redis-backed dedup, prevents double-processing on retry
OffsetCommitter       → commits Kafka offsets after a batch fully succeeds
LogsApplication (main)→ wires all of the above together, controls exit code
```

Each class has exactly one responsibility. This matters because the concurrency model is **deliberately asymmetric** — reading is single-threaded, processing is multi-threaded — and mixing these responsibilities into one class would make that asymmetry easy to break by accident.

### 4.1 `LogPoller` — reads from Kafka (single-threaded, by necessity)

**Why single-threaded:** `KafkaConsumer` is explicitly **not thread-safe** — the client library itself forbids concurrent use of one instance. This isn't a design choice, it's a hard constraint from Kafka's client.

**What it does:** creates a `KafkaConsumer`, subscribes to the topic, polls in a loop until either the batch size (default 3, configurable) is reached or a time budget (10s) expires — then closes the consumer. If fewer messages are available than the batch size within the time budget, it processes whatever it collected rather than waiting indefinitely.

**Where:** `com.processing.logs.consumer.LogPoller`

### 4.2 `LogMessageProcessor` — processes one message

**What it does:** given a single `ConsumerRecord`, maps its partition to a log type and output file (`0→info, 1→debug, 2→error, 3→warn`, anything else → an `anonymous` file with a warning logged), runs the Redis dedup check, writes to the file if not a duplicate, and marks it processed in Redis.

**Why partition-based file routing:** each Kafka partition is a dedicated channel for one log type, by producer-side convention (the producer controls this — the consumer trusts, but explicitly logs, anything landing outside the expected 0–3 range rather than silently dropping it).

**Where:** `com.processing.logs.processor.LogMessageProcessor`

### 4.3 `BatchExecutor` — parallel processing of an already-collected batch

**Why multithreading exists here, specifically:** a single batch (≤3 messages) can span multiple partitions/log types (e.g., 1 info + 1 debug + 1 error). Processing these concurrently, one thread per message, is the actual performance win — reading from Kafka is already fast and sequential; it's the *processing* (writes, dedup checks) that benefits from parallelism.

**What it does:** creates a fixed thread pool (size = batch size), submits one task per record, waits for all to finish via `Future.get()`, and returns whether the whole batch succeeded. Failure is **all-or-nothing** — if any message fails, the batch is not committed, and Kubernetes retries the whole Job.

**Where:** `com.processing.logs.executor.BatchExecutor`

### 4.4 `OffsetGuard` — Redis-backed idempotency

**Why this exists:** a pod can process a message, then fail to commit its Kafka offset (e.g., due to a consumer-group rebalance mid-commit — a real, observed failure mode under frequent pod churn). Kafka then correctly redelivers that message to the next pod. Without a separate dedup layer, this causes duplicate writes.

**Why Redis, not in-process memory:** each pod is a brand-new JVM; in-memory state dies with the pod. A retried pod has no visibility into what a *previous, now-gone* pod already did. The dedup marker has to live somewhere that outlives any single pod — Redis, a shared external service.

**Why a Redis Set (`SISMEMBER`/`SADD`), not a single "highest offset" marker:** an earlier version tracked just the highest offset seen per partition, skipping anything ≤ that value. This broke under concurrency — if a higher-offset message's thread finished before a lower-offset one (with no order guarantee between threads), the lower offset would be incorrectly treated as already-done and silently skipped. A Set checks *exact* offset membership, independent of ordering.

**Why entries are removed after commit (`SREM`), not left to accumulate:** an ever-growing Set doesn't scale to millions of messages. Once a Kafka commit succeeds, that offset is durably guaranteed not to be redelivered — the dedup entry has served its purpose and is removed immediately. A refreshed TTL (24h) on the whole key is a secondary safety net only, for the rare case where a pod crashes after committing but before cleaning up.

**Ordering that matters:** check → write file → **only if write succeeds** → mark in Redis. Marking before a successful write would risk silently losing a message forever (if the write later fails, Redis would already say "done").

**Where:** `com.processing.logs.processor.OffsetGuard`

### 4.5 `OffsetCommitter` — commits Kafka offsets after a batch succeeds

**Why a separate, new `KafkaConsumer` instance, not the same one from `LogPoller`:** committing an offset is a stateless operation from Kafka's perspective — any consumer in the correct group can do it, it doesn't require being the exact instance that originally read the message. `LogPoller` already closes its consumer before returning; reusing it would require keeping it alive longer, entangling two otherwise-independent responsibilities. A fresh, short-lived consumer used only to commit keeps both classes simple.

**What it does:** builds a `Map<TopicPartition, OffsetAndMetadata>` from the batch's records — the **highest raw offset seen per partition, plus one** (Kafka's "next offset to read" convention) — and calls `commitSync()`.

**Where:** `com.processing.logs.consumer.OffsetCommitter`

### 4.6 `main()` — orchestration and exit code

**Why exit codes matter:** Kubernetes Jobs decide `Completed` vs `Failed` based on the process's **exit code**, not on log output. `main()` explicitly calls `System.exit(0)` on full success and `System.exit(1)` on any batch failure, so Kubernetes' `backoffLimit` retry mechanism actually engages when something goes wrong.

**Flow:** poll → process (parallel) → if all succeeded: commit offsets, then clean up Redis markers for this batch → exit 0. If any failed: skip commit and cleanup entirely, exit 1 (so Kafka redelivers and Redis correctly still shows those specific messages as unprocessed).

**Where:** `com.processing.logs.LogsApplication`

---

## 5. Storage — why a `hostPath` volume

A pod's own container filesystem is ephemeral — it disappears when the pod is cleaned up. Output files need to survive past any single pod's lifetime and be visible to future pods. A `hostPath` volume, mounted at `/tmp` in every worker pod, backs these files with storage on the Minikube node itself, independent of pod lifecycle. (Production equivalent would likely be a `PersistentVolumeClaim` against real cluster storage, not `hostPath`, which is Minikube/local-specific.)

---

## 6. Key configuration, and why each matters

| Property | Value | Why |
|---|---|---|
| `enable.auto.commit` | `false` | Commits must happen explicitly, only after a batch is fully processed — not on a timer regardless of success. |
| `auto.offset.reset` | `earliest` | A brand-new consumer group must start from the beginning of the topic, not skip existing backlog (Kafka's own default here is `latest`, which silently skips existing messages — a real bug encountered and fixed during development). |
| `max.poll.records` | configurable, default `3` | Caps how many messages one pod picks up per run — this is the same number as the "batch size" referenced throughout. |
| `group.id` | `log-processing-group` | Must be identical across every pod — this is what makes Kafka distribute partitions across pods rather than have each pod read everything independently. |

---

## 7. Known trade-offs, stated explicitly

- **At-least-once, not exactly-once, at the Kafka level** — the Redis dedup layer is what upgrades this to effectively exactly-once for file writes specifically; without it, retries after a commit failure would duplicate.
- **Cross-pod file-write concurrency** is not separately locked — relies on Kafka's consumer-group guarantee that a given partition is owned by exactly one consumer at a time during stable operation, so two pods should not be writing the same offset simultaneously in practice.
- **Unrecognized partitions** (outside 0–3) are logged as a warning and written to a fallback file, rather than failing the batch — a deliberate choice while the producer is still evolving and not fully trusted.

---

## 8. Push all into one

An aggregator (planned as a `CronJob`) to periodically merge the shared tmp files into long-term output files. Not required for correctness of the pipeline above — it's a downstream housekeeping step.