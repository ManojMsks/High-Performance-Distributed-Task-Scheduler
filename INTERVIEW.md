# Distributed Task Scheduler — Interview Deep-Dive Cheatsheet

> A complete technical reference for communicating the design of this system in senior/staff SDE interviews.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Q: Split-Brain During Leader Election](#2-how-does-your-engine-handle-split-brain-scenarios-during-leader-election)
3. [Q: Redis Streams vs Pub/Sub vs List Queues](#3-why-redis-streams-over-pubsub-or-list-queues)
4. [Q: At-Least-Once Without Duplicate Processing](#4-how-do-you-guarantee-at-least-once-execution-without-duplicate-processing)
5. [Q: Dual-Sweeper Stale Lock Design](#5-how-does-the-dual-sweeper-design-clean-up-stale-locks)
6. [Q: Horizontal Scaling & Backpressure](#6-how-does-the-system-scale-horizontally)
7. [Q: Priority Queue Without Starvation](#7-how-is-strict-priority-enforced-without-starving-low-priority-tasks)
8. [Q: Why PostgreSQL + Redis Duality](#8-why-use-both-postgresql-and-redis-instead-of-just-one)
9. [Q: Failure Modes & Recovery](#9-walk-me-through-every-failure-mode-and-how-you-recover)
10. [Q: Operational Observability](#10-how-do-you-observe-and-debug-this-in-production)
11. [Key Numbers to Know](#11-key-numbers-to-know)

---

## 1. System Overview

```
+-------------+   POST /v1/tasks   +------------------------------------------+
¦  REST Client¦------------------? ¦            Producer                      ¦
+-------------+                    ¦  validate ? Postgres(PENDING) ? route     ¦
                                   +-------------------------------------------+
                                  immediate   ¦                  ¦ scheduledAt > now
                                   XADD ?    ¦                  ? ZADD (score=ms)
                              +----------------+     +-------------------------+
                              ¦  Priority       ¦     ¦  Delayed ZSET           ¦
                              ¦  Streams (3)    ¦?----¦  scheduler:delayed:zset ¦
                              ¦  HIGH/MED/LOW   ¦     ¦  (SchedulerService)     ¦
                              +-----------------+     +-------------------------+
                                      ¦ XREADGROUP (consumer group)
                              +-------?--------------------------------------+
                              ¦           WorkerService (N replicas)          ¦
                              ¦  lock ? RUNNING ? execute handler ? ACK/NACK ¦
                              +----------------------------------------------+
                               success¦                  failure¦
                                      ?                        ?
                             Postgres(COMPLETED)        retry ZADD / DLQ XADD
```

**Core principle:** Postgres is the single source of truth for task state. Redis is the hot path for performance. Inconsistencies are healed by the Recovery service.

---

## 2. "How does your engine handle split-brain scenarios during leader election?"

### The Problem

In a distributed system with multiple scheduler instances, split-brain occurs when two nodes simultaneously believe they are the leader and both promote tasks — causing double-execution.

### Our Solution: Atomic Redis SET NX PX

```lua
-- Acquire lock atomically (cannot race)
SET scheduler:lock:scheduler-leader <schedulerId> NX PX <ttl_ms>
```

**Why this is safe:**

| Property | How it's guaranteed |
|---|---|
| **Mutual exclusion** | `SET NX` is atomic — only one writer succeeds |
| **Automatic expiry** | `PX <ttl>` means the lock self-releases if the holder crashes |
| **Fencing against zombie leaders** | Compare-and-delete Lua script checks identity before DEL |
| **Renewal** | Leader renews at TTL/3 — 3× safety margin before expiry |

**Lock renewal Lua script:**
```lua
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], tonumber(ARGV[2]))
else
  return 0  -- lost the lock; stop renewing
end
```

### What happens under partition?

- **Leader loses Redis connectivity:** It can't renew ? lock expires ? standby takes over after TTL.
- **Network heals:** Old leader finds its identity gone from Redis ? marks `isLeader = false` ? stops promoting.
- **Concurrent promotion window:** There is a theoretical window between lock expiry and standby acquisition. Tasks are protected by a **per-task idempotency lock** (`scheduler:lock:task:{id}`) that prevents double-execution even if the scheduler promotes a task twice.

### Redlock for production clusters

In a Redis Cluster (multiple masters), `SET NX` is insufficient (network partition can create two primaries). The solution is **Redlock** (write majority of N masters atomically). For this project, single-node Redis with AOF persistence is the chosen tradeoff — acceptable for most production deployments.

---

## 3. "Why Redis Streams over Pub/Sub or List queues?"

### Comparison Matrix

| Feature | RPUSH/BRPOP (List) | PUBLISH/SUBSCRIBE | Redis Streams (XADD/XREADGROUP) |
|---|---|---|---|
| **Persistence** | ? Yes | ? No (fire-and-forget) | ? Yes |
| **Consumer groups** | ? No | ? No | ? Yes |
| **At-least-once delivery** | ? Pop = lose on crash | ? Miss if no subscriber | ? PEL (pending entry list) |
| **Multiple consumers** | ? One consumer per message | ? Fan-out | ? Partition across group |
| **Acknowledgement** | ? None | ? None | ? XACK |
| **PEL recovery (XCLAIM)** | ? No | ? No | ? Yes |
| **Message ordering** | FIFO | FIFO | Monotonic ID ordering |
| **Backpressure / MAXLEN** | ? Unbounded | ? N/A | ? MAXLEN |
| **Historical replay** | ? Destructive read | ? None | ? XRANGE |

### Key semantic: XREADGROUP + PEL

```
XREADGROUP GROUP workers worker-1 COUNT 10 STREAMS scheduler:stream:high >
```

- The `>` means "give me messages not yet delivered to any consumer".
- Messages are held in the **PEL** (pending-entries list) until `XACK`.
- If `worker-1` crashes before `XACK`, the message stays in the PEL with an idle timer.
- `XCLAIM` lets the recovery service take ownership after the idle threshold.

This provides **at-least-once delivery with replay capability** — impossible with Lists or Pub/Sub.

---

## 4. "How do you guarantee at-least-once execution without duplicate processing?"

### The Two-Layer Guard

**Layer 1 — Stream consumer groups (delivery guarantee):**
- `XREADGROUP` delivers each message to exactly one consumer per group.
- Messages stay in PEL until `XACK` — never silently dropped.
- Crashed workers ? RecoveryService reclaims via `XCLAIM` ? re-executes.

**Layer 2 — Per-task distributed lock (execution deduplication):**
```lua
-- acquireTaskLock: only the first winner executes
SET scheduler:lock:task:{taskId} {workerId} NX PX {ttl_ms}
```

**Execution flow:**
```
1. XREADGROUP ? worker receives message
2. Postgres: task.status ? {QUEUED, RETRYING}?  ? No: ACK + skip
3. acquireTaskLock(taskId, workerId)             ? Fail: ACK + skip (another worker has it)
4. Postgres: UPDATE status = RUNNING             ? Committed
5. Execute handler
6. finally: XACK + releaseTaskLock
```

**Result:** Even if a message is delivered twice (e.g., after recovery), only one execution completes because the second worker fails to acquire the lock (or finds status already COMPLETED).

**This gives us:** At-least-once delivery (Step 1) + idempotent execution (Steps 2–3).

### Edge case: lock expires during execution

- Long-running handler finishes after the lock TTL expires.
- Recovery might re-queue the task while the original worker is still running.
- **Mitigation:** Lock renewal timer runs at `TTL/3` intervals, extending the lock for as long as the handler is alive. A handler that truly hangs indefinitely will eventually be dead-lettered.

---

## 5. "How does the dual-sweeper design clean up stale locks in PostgreSQL and Redis?"

### Why Two Sweepers?

There are **two independent inconsistency problems**:

| Problem | Location | Symptom |
|---|---|---|
| Stale task state | **PostgreSQL** | Task stuck in `RUNNING` with expired worker heartbeat |
| Orphaned stream message | **Redis PEL** | Message idle in PEL, worker dead, never ACKed |

One sweeper cannot fix both. We need two complementary, idempotent jobs.

### Job 1: Postgres Recovery

```
1. SELECT tasks WHERE status = RUNNING AND lockedAt < (now - STALE_THRESHOLD)
2. For each stale task:
     GET scheduler:heartbeat:worker:{lockedById}
     ? if key EXISTS: worker is slow but alive ? skip
     ? if key MISSING: worker is dead ? recover
3. Recovery:
     retryCount < maxRetries: ZADD delayed:zset (with backoff), UPDATE RETRYING
     else:                    XADD dead-letter,   UPDATE DEAD_LETTER
```

### Job 2: Redis PEL Cleanup

```
1. XPENDING scheduler:stream:{priority} workers-group - + 50
2. Filter entries with idle_ms >= STALE_THRESHOLD_MS
3. For each stale entry:
     XCLAIM ? transfers ownership to recovery consumer
     XACK   ? removes from PEL permanently
```

**Coordination:** Job 1 updates Postgres (task will be re-queued). Job 2 cleans Redis (no stuck PEL entries). They run concurrently via `Promise.all` on the same sweep cycle.

**Idempotency:** If Job 1 runs twice for the same task (e.g., two recovery instances during a restart), the second update finds the task already in `RETRYING` and is a no-op. Job 2 XCLAIM fails gracefully if the entry was already claimed (race condition, benign).

### Heartbeat-gated recovery — false positive prevention

```
heartbeat key exists?
  YES ? worker is SLOW, not dead ? skip (prevents premature recovery)
  NO  ? worker is DEAD ? recover the task
```

Without heartbeat gating, a slow worker executing a valid task could have its lock "recovered" by the sweeper, causing duplicate execution. The heartbeat TTL is set 3× larger than the heartbeat emission interval to prevent false negatives under transient network hiccups.

---

## 6. "How does the system scale horizontally?"

### Workers (stateless — scale freely)

```bash
docker compose up -d --scale worker=10
```

- Workers are **completely stateless**. Each creates a consumer group member under the shared `workers` group name.
- Redis distributes stream messages across all consumer group members automatically.
- No central coordination needed — workers independently call `XREADGROUP`.

### Scheduler (leader-elected — only one active)

- Multiple scheduler replicas can run (handled by docker deploy.replicas).
- Only the leader (lock holder) promotes delayed tasks.
- Standby instances poll for the lock and take over within one TTL window (~30s).

### API (stateless — load-balance freely)

- Multiple API replicas behind a load balancer (nginx, AWS ALB).
- All state is in Postgres/Redis — no session affinity required.
- Socket.IO requires sticky sessions for WebSocket connections (or use Redis adapter: `socket.io-redis`).

### Database (Postgres vertical + read replicas)

- Task creation: always primary.
- Status reads (`GET /v1/tasks/:id`): can route to read replicas.
- Critical indexes: `(status, priority, scheduledAt)` for poller queries, `(lockedAt)` for recovery sweeps.

---

## 7. "How is strict priority enforced without starving low-priority tasks?"

### Priority-First Non-Blocking Sweep

```typescript
// Worker fetches in strict order:
for (const streamKey of [HIGH, MEDIUM, LOW]) {
  const msgs = await XREADGROUP(streamKey, COUNT maxCount, STREAMS ">")
  if (msgs.length > 0) return msgs  // stop at first non-empty queue
}
// All empty ? blocking XREADGROUP across all 3 with timeout
await XREADGROUP(BLOCK 5000ms, STREAMS high medium low, "> > >")
```

**Result:**
- HIGH tasks are always pulled before MEDIUM or LOW if any are available.
- LOW tasks are never starved: if HIGH and MEDIUM are empty, LOW gets consumed.
- The blocking multi-stream read ensures the worker sleeps efficiently (no busy-wait) when all queues are empty.

### Backpressure

- `MAXLEN` on streams prevents memory exhaustion from queue buildup.
- `WORKER_CONCURRENCY` semaphore limits in-flight tasks per worker replica.

---

## 8. "Why use both PostgreSQL and Redis instead of just one?"

### Role separation (each tool at its strength)

| Concern | PostgreSQL | Redis |
|---|---|---|
| **Durability** | WAL + ACID transactions | AOF + RDB snapshots |
| **Query flexibility** | Complex joins, GROUP BY | None |
| **Throughput (reads)** | ~10k–50k QPS | ~500k–1M QPS |
| **Task metadata** | Full history, logs, retries | Hot path only |
| **Distributed locks** | Pessimistic (advisory locks) | NX PX (µs latency) |
| **Fan-out / pub-sub** | LISTEN/NOTIFY (limited) | Native |
| **Recovery queries** | `WHERE status=RUNNING AND lockedAt < ?` | Cannot express |

**Using only Redis:** No ACID guarantees; complex queries impossible; recovery is hard to audit; no relational integrity.

**Using only Postgres:** No efficient blocking queue; lock acquisition requires table-level locking; ~100× slower for queue operations; no TTL semantics on rows.

---

## 9. "Walk me through every failure mode and how you recover"

| Failure | Detection | Recovery |
|---|---|---|
| **Worker crashes mid-execution** | Heartbeat TTL expires | RecoveryService: XCLAIM PEL + re-queue to ZSET |
| **Worker OOM kill** | Same as crash | Same as above |
| **Scheduler crashes** | Leader lock expires after TTL | Standby scheduler acquires lock, resumes promotion |
| **Redis restart** | AOF replay | Streams/ZSET restored; task state in Postgres is authoritative |
| **Postgres restart** | Connection pool reconnect | Workers pause, retry; no data loss (WAL) |
| **Network partition (worker ? Redis)** | Worker can't renew lock ? lock expires | RecoveryService detects expired heartbeat ? re-queues |
| **Double delivery (stream replay)** | — | Per-task lock + status check prevents double execution |
| **Poisonous message (always fails)** | Max retries exhausted | Dead-letter queue (XADD + DEAD_LETTER status) |
| **Full Redis memory** | MAXLEN truncates oldest stream entries | Monitor with /v1/metrics; alert on DLQ growth |

---

## 10. "How do you observe and debug this in production?"

### Live Dashboard (WebSocket)

- Real-time queue depth sparklines, worker heartbeat ages, task status donut.
- Log feed with level filtering (INFO / WARN / ERROR).
- Task status transition stream for spotting anomalies.

### REST API Metrics

```bash
curl http://localhost:3000/v1/metrics
# ? { queues, delayed, deadLetter, byStatus }

curl http://localhost:3000/v1/health
# ? { status: "healthy"|"degraded", postgres, redis, workerCount }
```

### Admin CLI

```bash
npm run admin status             # live cluster snapshot
npm run admin dlq:inspect        # audit all dead-lettered tasks
npm run admin dlq:replay --id X  # manually replay a specific DLQ entry
npm run admin tasks:list --status RUNNING  # find stuck tasks
```

### Structured Logging (Winston JSON)

Every operation logs: `taskId`, `workerId`, `priority`, `duration`, `attempt`, `error`.
Route to Datadog / ELK / CloudWatch for alerting.

### Key alerts to configure

| Signal | Threshold | Action |
|---|---|---|
| DLQ depth | > 0 | Page on-call |
| `RUNNING` tasks with age > 5× avg | > 5 | Investigate stale lock |
| Worker count | 0 | Critical alert |
| HIGH queue depth | > 1000 | Scale workers |
| Scheduler lock not renewed | > TTL | Restart scheduler |

---

## 11. Key Numbers to Know

| Parameter | Default | Reasoning |
|---|---|---|
| Lock TTL | 30s | Max expected handler duration |
| Heartbeat interval | 15s | Emitted at TTL/2 for safety |
| Heartbeat TTL | 45s | 3× interval — survives 2 missed heartbeats |
| Scheduler poll | 1s | Sub-second promotion accuracy |
| Stale lock threshold | 90s | 3× lock TTL — well past expiry |
| Recovery poll | 30s | Balance between latency and DB load |
| Retry backoff | `min(2^n * 1s, 1h)` | Exponential with 1-hour ceiling |
| Max retries | 3 | Configurable per task |
| Worker concurrency | 5 | Per-replica; scale via `--scale` |
| Stream MAXLEN | ~10k | Configurable in redis.conf |
| Scheduler batch | 100 | Tasks promoted per tick |

---

*Built with: Node.js 22 · TypeScript 5 · Prisma 5 · Redis 7.2 Streams · PostgreSQL 16 · Express · Socket.IO · Docker*
