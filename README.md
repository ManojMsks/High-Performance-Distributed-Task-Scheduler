# Distributed Task Scheduler & Execution Engine

A production-grade, fault-tolerant distributed task scheduler built with **Node.js (TypeScript)**, **Redis 7.2**, **PostgreSQL 16**, and **Docker**.

## Architecture Overview

```
+-------------------------------------------------------------+
¦                        API Server                           ¦
¦                    (Express + TypeScript)                   ¦
+-------------------------------------------------------------+
             ¦ enqueue                   ¦ status/results
             ?                           ?
+---------------------+       +-----------------------------+
¦    Scheduler        ¦       ¦      PostgreSQL 16           ¦
¦  (Delayed ZSET      ¦------?¦  Tasks / Workers / Logs     ¦
¦   ? Stream Promo)   ¦       +-----------------------------+
+---------------------+
         ¦ XADD
         ?
+-------------------------------------------------------------+
¦                       Redis 7.2                             ¦
¦  scheduler:delayed:zset      (Sorted Set — delayed tasks)   ¦
¦  scheduler:stream:high       (Stream — priority queue)      ¦
¦  scheduler:stream:medium     (Stream — priority queue)      ¦
¦  scheduler:stream:low        (Stream — priority queue)      ¦
¦  scheduler:lock:task:{id}    (String — distributed lock)    ¦
¦  scheduler:worker:heartbeat:{id} (String — TTL liveness)   ¦
+-------------------------------------------------------------+
         ¦ XREADGROUP
         ?
+-------------------------------------------------------------+
¦                      Worker Pool                            ¦
¦   Worker 1   Worker 2   Worker 3   …   Worker N             ¦
¦   (concurrency per worker configurable via env)             ¦
+-------------------------------------------------------------+
```

## Key Design Decisions

| Concern | Choice | Rationale |
|---|---|---|
| Task queues | Redis Streams + Consumer Groups | Backpressure, ack semantics, replay |
| Delayed tasks | Redis Sorted Set (score = schedule timestamp) | O(log N) range scans |
| Distributed locking | Lua compare-and-delete | Atomic, clock-drift-safe |
| Worker liveness | Heartbeat TTL keys | Auto-expiry on crash |
| Persistence | PostgreSQL (source of truth) + Redis (hot path) | ACID + throughput |
| Schema | Prisma ORM | Type-safe, migrations, composite indexes |

## Phase 1 — Repository Setup, Schema & Redis Key Design

### File Tree

```
.
+-- docker/
¦   +-- docker-compose.yml         # Postgres 16 + Redis 7.2 with healthchecks
¦   +-- redis/
¦   ¦   +-- redis.conf             # AOF persistence, maxmemory 512MB, slow-log
¦   +-- postgres/
¦       +-- init/
¦           +-- 00-extensions.sql  # pg_stat_statements, uuid-ossp
+-- prisma/
¦   +-- schema.prisma              # Task, Worker, TaskLog + composite indexes
+-- src/
¦   +-- config/
¦   ¦   +-- index.ts               # Zod-validated env config (fail-fast)
¦   +-- infra/
¦   ¦   +-- logger.ts              # Winston (pretty dev / JSON prod + rotating files)
¦   ¦   +-- db.ts                  # Prisma singleton
¦   ¦   +-- redis/
¦   ¦       +-- redis-client.ts    # ioredis singleton + retry strategy + events
¦   ¦       +-- redis-keys.ts      # Key namespace + atomic lock/heartbeat ops
¦   +-- types/
¦   ¦   +-- index.ts               # Branded IDs, DTOs, Result<T,E>
¦   +-- index.ts                   # API server bootstrap
+-- .env.example
+-- .gitignore
+-- package.json
+-- tsconfig.json
+-- README.md
```

## Quick Start

```bash
# 1. Install dependencies
npm install

# 2. Configure environment
cp .env.example .env
# Edit .env with your settings

# 3. Start infrastructure
docker compose -f docker/docker-compose.yml up -d

# 4. Generate Prisma client & run migrations
npm run prisma:generate
npm run prisma:migrate

# 5. Start the API server (dashboard + REST API)
npm run dev

# 6. In separate terminals, start the processes that actually do the work.
#    The API server on its own only accepts tasks â€” nothing executes them
#    until at least a worker (and, for delayed/retry tasks, a scheduler)
#    is running:
npm run worker      # executes tasks pulled from the priority streams
npm run scheduler    # promotes delayed/retrying tasks into the streams
npm run recovery      # reclaims tasks orphaned by a crashed worker
```

All five processes (API, worker, scheduler, recovery, plus Postgres/Redis)
need to be running for the system to actually process anything end to end.
`worker` can be run multiple times in parallel to scale out; `scheduler` and
`recovery` should normally run as a single instance each (the scheduler
uses a distributed leader lock so extra replicas just stand by).

## Environment Variables

See [`.env.example`](.env.example) for all available configuration options with descriptions.
