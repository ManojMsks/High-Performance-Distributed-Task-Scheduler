/**
 * src/core/recovery.ts
 *
 * Stale Lock & Orphaned Task Recovery Service
 *
 * Runs as a periodic sweeper (every RECOVERY_POLL_INTERVAL_MS) with two jobs:
 *
 * 1. recoverOrphanedTasks()
 *    Finds tasks stuck in RUNNING status where:
 *      - lockedAt < (now - STALE_LOCK_THRESHOLD_SECONDS)   (old enough to be suspicious)
 *      - worker heartbeat key has expired in Redis          (worker is genuinely dead)
 *    Then either re-queues (RETRYING) or dead-letters (DEAD_LETTER) the task.
 *
 * 2. recoverStalePendingEntries()
 *    Uses XPENDING to find stream entries that have been in a consumer's PEL
 *    (pending-entries list) for longer than the stale threshold — i.e., delivered
 *    but never ACKed by a crashed worker.  These entries are XCLAIMed and then
 *    XACKed so they don't accumulate in the PEL; the Postgres-level recovery
 *    (job 1) handles re-queueing the actual task.
 *
 * The two jobs are complementary: job 1 fixes the Postgres state, job 2 cleans
 * the Redis stream PEL.  Both must run to achieve full consistency.
 */

import { TaskPriority, TaskStatus, LogLevel } from "@prisma/client";
import { getDb } from "../infra/db";
import { getRedisClient } from "../infra/redis/redis-client";
import {
  RedisKeys,
  STREAM_CONSUMER_GROUP,
  getWorkerHeartbeat,
} from "../infra/redis/redis-keys";
import { logger } from "../infra/logger";
import { config } from "../config";
import type Redis from "ioredis";

// -----------------------------------------------------------------------------
// Internal types
// -----------------------------------------------------------------------------

/** Raw shape returned by ioredis XPENDING (range form). */
type RawPendingEntry = [
  id:            string,
  consumerName:  string,
  idleMs:        number,
  deliveryCount: number,
];

interface StaleTask {
  id:         string;
  type:       string;
  priority:   TaskPriority;
  retryCount: number;
  maxRetries: number;
  lockedById: string | null;
}

// -----------------------------------------------------------------------------
// RecoveryService
// -----------------------------------------------------------------------------

export class RecoveryService {
  private readonly redis: Redis;
  private running     = false;
  private sweepTimer: NodeJS.Timeout | null = null;

  /** Consumer name used when XCLAIM-ing PEL entries for inspection. */
  private readonly recoveryConsumer = `recovery:${process.pid}`;

  constructor() {
    this.redis = getRedisClient();
  }

  async start(): Promise<void> {
    this.running = true;
    logger.info("[Recovery] Started", {
      pollIntervalMs:     config.RECOVERY_POLL_INTERVAL_MS,
      staleLockThreshold: config.STALE_LOCK_THRESHOLD_SECONDS,
    });
    this.scheduleSweep();
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.sweepTimer !== null) {
      clearTimeout(this.sweepTimer);
      this.sweepTimer = null;
    }
    logger.info("[Recovery] Stopped");
  }

  // -- Scheduling -------------------------------------------------------------

  private scheduleSweep(): void {
    if (!this.running) return;
    this.sweepTimer = setTimeout(() => {
      void this.sweep().finally(() => this.scheduleSweep());
    }, config.RECOVERY_POLL_INTERVAL_MS);
  }

  private async sweep(): Promise<void> {
    try {
      // Run both jobs concurrently — they touch different parts of the system
      await Promise.all([
        this.recoverOrphanedTasks(),
        this.recoverStalePendingEntries(),
      ]);
    } catch (err) {
      logger.error("[Recovery] Sweep error", { error: (err as Error).message });
    }
  }

  // -- Job 1: Orphaned task recovery (Postgres-level) -------------------------

  /**
   * Scans for tasks stuck in RUNNING state with an old lockedAt timestamp.
   * Only treats a task as truly orphaned if the owning worker's heartbeat key
   * has expired in Redis — a slow-but-alive worker is not a stale worker.
   */
  private async recoverOrphanedTasks(): Promise<void> {
    const db          = getDb();
    const staleBefore = new Date(
      Date.now() - config.STALE_LOCK_THRESHOLD_SECONDS * 1_000,
    );

    const staleTasks = await db.task.findMany({
      where: {
        status:   TaskStatus.RUNNING,
        lockedAt: { lt: staleBefore },
      },
      select: {
        id:         true,
        type:       true,
        priority:   true,
        retryCount: true,
        maxRetries: true,
        lockedById: true,
      },
    });

    if (staleTasks.length === 0) return;

    logger.warn("[Recovery] Detected potentially orphaned tasks", {
      count: staleTasks.length,
    });

    for (const task of staleTasks) {
      // Confirm the owning worker's heartbeat has actually expired
      if (task.lockedById !== null) {
        const heartbeat = await getWorkerHeartbeat(this.redis, task.lockedById);
        if (heartbeat !== null) {
          // Heartbeat still fresh — worker is alive, just slow; skip
          logger.debug("[Recovery] Worker still alive, skipping", {
            taskId:   task.id,
            workerId: task.lockedById,
          });
          continue;
        }
      }

      await this.recoverTask(task);
    }
  }

  /**
   * Handles a single confirmed-orphaned task.
   * - retryCount < maxRetries  ? ZADD to delayed ZSET + PG RETRYING
   * - retryCount >= maxRetries ? XADD to DLQ stream + PG DEAD_LETTER
   */
  private async recoverTask(task: StaleTask): Promise<void> {
    const db = getDb();

    if (task.retryCount < task.maxRetries) {
      // Exponential backoff, capped at 1 hour
      const backoffMs = Math.min(Math.pow(2, task.retryCount) * 1_000, 3_600_000);
      const retryAt   = new Date(Date.now() + backoffMs);

      await this.redis.zadd(RedisKeys.delayedQueue(), retryAt.getTime(), task.id);

      await db.task.update({
        where: { id: task.id },
        data: {
          status:     TaskStatus.RETRYING,
          retryCount: { increment: 1 },
          lockedById: null,
          lockedAt:   null,
        },
      });

      await db.taskLog.create({
        data: {
          taskId:  task.id,
          level:   LogLevel.WARN,
          event:   "task.recovered",
          message: `Orphaned task recovered — retry ${task.retryCount + 1}/${task.maxRetries} in ${backoffMs}ms`,
          metadata: {
            retryAt:   retryAt.toISOString(),
            backoffMs,
            lockedById: task.lockedById,
          },
        },
      });

      logger.info("[Recovery] Task re-queued for retry", {
        taskId:  task.id,
        attempt: task.retryCount + 1,
        retryAt: retryAt.toISOString(),
      });
    } else {
      // Max retries exhausted — send to dead-letter queue
      await this.redis.xadd(
        RedisKeys.deadLetterStream(),
        "*",
        "taskId",        task.id,
        "type",          task.type,
        "priority",      task.priority,
        "reason",        "orphaned_max_retries_exceeded",
        "deadLetteredAt", new Date().toISOString(),
      );

      await db.task.update({
        where: { id: task.id },
        data: {
          status:      TaskStatus.DEAD_LETTER,
          completedAt: new Date(),
          lockedById:  null,
          lockedAt:    null,
        },
      });

      await db.taskLog.create({
        data: {
          taskId:  task.id,
          level:   LogLevel.ERROR,
          event:   "task.dead_lettered",
          message: `Orphaned task dead-lettered after exhausting ${task.maxRetries} retries`,
          metadata: {
            retryCount: task.retryCount,
            maxRetries: task.maxRetries,
            lockedById: task.lockedById,
          },
        },
      });

      logger.error("[Recovery] Orphaned task dead-lettered", {
        taskId:     task.id,
        retryCount: task.retryCount,
        maxRetries: task.maxRetries,
      });
    }
  }

  // -- Job 2: Redis Stream PEL cleanup ----------------------------------------

  /**
   * Scans the PEL (Pending Entries List) of each priority stream for entries
   * that have been idle longer than the stale lock threshold.
   *
   * These are messages delivered to a consumer (worker) that crashed before
   * ACKing them.  We XCLAIM + XACK them to keep the PEL clean; the Postgres
   * recovery in job 1 handles the actual task re-queueing.
   */
  private async recoverStalePendingEntries(): Promise<void> {
    const streams = [
      RedisKeys.priorityStream(TaskPriority.HIGH),
      RedisKeys.priorityStream(TaskPriority.MEDIUM),
      RedisKeys.priorityStream(TaskPriority.LOW),
    ];

    const minIdleMs = config.STALE_LOCK_THRESHOLD_SECONDS * 1_000;

    for (const streamKey of streams) {
      await this.processStalePELEntries(streamKey, minIdleMs);
    }
  }

  private async processStalePELEntries(
    streamKey: string,
    minIdleMs: number,
  ): Promise<void> {
    try {
      // XPENDING with range: returns at most 50 pending entries across all consumers
      const rawPending = (await this.redis.xpending(
        streamKey,
        STREAM_CONSUMER_GROUP,
        "-",
        "+",
        50,
      )) as RawPendingEntry[];

      if (!Array.isArray(rawPending) || rawPending.length === 0) return;

      const staleIds = rawPending
        .filter((entry) => {
          const idleMs = typeof entry[2] === "number" ? entry[2] : Number(entry[2]);
          return idleMs >= minIdleMs;
        })
        .map((entry) => entry[0]);

      if (staleIds.length === 0) return;

      logger.warn("[Recovery] Reclaiming stale PEL entries", {
        streamKey,
        count: staleIds.length,
      });

      // XCLAIM transfers ownership to the recovery consumer; the idle timer resets
      for (const entryId of staleIds) {
        try {
          await this.redis.xclaim(
            streamKey,
            STREAM_CONSUMER_GROUP,
            this.recoveryConsumer,
            minIdleMs,
            entryId,
          );

          // XACK removes the entry from the PEL permanently
          // The task itself will be re-queued by recoverOrphanedTasks() via Postgres
          await this.redis.xack(streamKey, STREAM_CONSUMER_GROUP, entryId);

          logger.debug("[Recovery] PEL entry reclaimed and ACK-ed", {
            streamKey,
            entryId,
          });
        } catch (err) {
          // Entry may have been claimed by another recovery instance — benign
          logger.debug("[Recovery] Could not XCLAIM entry (race condition, skipping)", {
            streamKey,
            entryId,
            error: (err as Error).message,
          });
        }
      }
    } catch (err) {
      const message = (err as Error).message;
      // NOGROUP / ERR no such key = stream doesn't exist yet (before first task)
      if (
        !message.includes("NOGROUP") &&
        !message.includes("ERR no such key") &&
        !message.includes("-NOGROUP")
      ) {
        logger.error("[Recovery] XPENDING error", { streamKey, error: message });
      }
    }
  }

  /**
   * Publicly exposed sweep trigger — used by the E2E verification suite
   * and any external health-check tooling that needs an on-demand run.
   */
  public async runSweep(): Promise<void> {
    return this.sweep();
  }
}