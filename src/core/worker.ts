/**
 * src/core/worker.ts
 *
 * Worker Execution Engine
 *
 * Consumes tasks from priority Redis Streams using XREADGROUP with a shared
 * consumer group.  Enforces strict priority ordering (HIGH ? MEDIUM ? LOW)
 * via non-blocking sweeps before falling back to a blocking multi-stream read.
 *
 * Concurrency model: a simple counter semaphore allows up to `concurrency`
 * tasks to be in-flight simultaneously within a single worker process.
 *
 * Failure semantics:
 *   retryCount < maxRetries  ?  exponential-backoff ZADD to delayed ZSET (RETRYING)
 *   retryCount >= maxRetries ?  XADD to dead-letter stream + PG DEAD_LETTER
 *
 * Lock safety:
 *   - acquireTaskLock must succeed before RUNNING is written to Postgres.
 *   - A periodic renewTimer prevents the lock from expiring during execution.
 *   - releaseTaskLock is called in the finally block; it is a no-op if the
 *     lock already expired (the recovery service will handle re-queueing).
 */

import { TaskPriority, TaskStatus, WorkerStatus, LogLevel } from "@prisma/client";
import os from "os";
import { getDb } from "../infra/db";
import { getRedisClient, getBlockingRedisClient } from "../infra/redis/redis-client";
import {
  RedisKeys,
  STREAM_CONSUMER_GROUP,
  LOCK_TTL_SECONDS,
  acquireTaskLock,
  releaseTaskLock,
  renewTaskLock,
  sendWorkerHeartbeat,
} from "../infra/redis/redis-keys";
import { logger } from "../infra/logger";
import { config } from "../config";
import type Redis from "ioredis";

// -----------------------------------------------------------------------------
// Public Types
// -----------------------------------------------------------------------------

/** Context object passed to every task handler invocation. */
export interface TaskContext {
  taskId:        string;
  type:          string;
  workerId:      string;
  /** Zero-indexed retry number (0 = first attempt). */
  retryCount:    number;
  /** Human-friendly attempt number (retryCount + 1). */
  attemptNumber: number;
  /**
   * Extend the distributed lock TTL.
   * Call periodically in long-running handlers to prevent stale-lock recovery
   * from incorrectly treating the task as orphaned.
   */
  renewLock: () => Promise<void>;
}

/** Signature all task handlers must implement. */
export type TaskHandlerFn = (
  payload: Record<string, unknown>,
  ctx:     TaskContext,
) => Promise<void>;

// -----------------------------------------------------------------------------
// Internal Types
// -----------------------------------------------------------------------------

interface ConsumedMessage {
  streamKey: string;
  entryId:   string;
  taskId:    string;
  type:      string;
  priority:  TaskPriority;
}

/** ioredis XREADGROUP return shape (entries may be null for deleted messages). */
type XReadGroupResponse = Array<[string, Array<[string, string[] | null]>]> | null;

// -----------------------------------------------------------------------------
// Priority ordering helper
// -----------------------------------------------------------------------------

const PRIORITY_ORDER: Record<string, number> = {
  [TaskPriority.HIGH]:   0,
  [TaskPriority.MEDIUM]: 1,
  [TaskPriority.LOW]:    2,
};

// -----------------------------------------------------------------------------
// WorkerService
// -----------------------------------------------------------------------------

export class WorkerService {
  private readonly redis:         Redis;
  private readonly blockingRedis: Redis;
  private readonly handlers: Map<string, TaskHandlerFn> = new Map();

  private workerId      = "";
  private running       = false;
  private activeCount   = 0;
  private heartbeatTimer: NodeJS.Timeout | null = null;

  private readonly concurrency:    number;
  private readonly blockTimeoutMs: number;

  constructor() {
    this.redis          = getRedisClient();
    this.blockingRedis  = getBlockingRedisClient();
    this.concurrency    = config.WORKER_CONCURRENCY;
    this.blockTimeoutMs = config.WORKER_BLOCK_TIMEOUT_MS;
  }

  /**
   * Register a handler for a task type.
   * Must be called before `start()`.
   * Returns `this` for fluent chaining.
   */
  register(type: string, handler: TaskHandlerFn): this {
    this.handlers.set(type, handler);
    logger.debug("[Worker] Handler registered", { type });
    return this;
  }

  // -- Lifecycle --------------------------------------------------------------

  async start(): Promise<void> {
    const db = getDb();

    // Create a Worker record in Postgres — its UUID becomes the workerId
    const worker = await db.worker.create({
      data: {
        hostname:    os.hostname(),
        pid:         process.pid,
        status:      WorkerStatus.IDLE,
        concurrency: this.concurrency,
      },
      select: { id: true },
    });
    this.workerId = worker.id;

    logger.info("[Worker] Started", {
      workerId:    this.workerId,
      hostname:    os.hostname(),
      pid:         process.pid,
      concurrency: this.concurrency,
    });

    await this.initConsumerGroups();
    this.startHeartbeat();

    this.running = true;
    void this.runConsumerLoop(); // non-blocking: start() resolves after init
  }

  async stop(): Promise<void> {
    logger.info("[Worker] Shutting down…", {
      workerId:    this.workerId,
      activeCount: this.activeCount,
    });

    this.running = false;

    // Graceful drain: wait up to 30 s for in-flight tasks to finish
    const deadline = Date.now() + 30_000;
    while (this.activeCount > 0 && Date.now() < deadline) {
      await sleep(200);
    }

    if (this.heartbeatTimer !== null) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }

    // Remove heartbeat key immediately so recovery doesn't wait for TTL
    if (this.workerId !== "") {
      await this.redis.del(RedisKeys.workerHeartbeat(this.workerId));
    }

    const db = getDb();
    if (this.workerId !== "") {
      await db.worker.update({
        where: { id: this.workerId },
        data:  { status: WorkerStatus.OFFLINE },
      });
    }

    logger.info("[Worker] Shutdown complete", { workerId: this.workerId });
  }

  // -- Consumer group initialisation -----------------------------------------

  /**
   * Ensures the consumer group exists on all three priority streams.
   * "0" start ID = new groups start from the beginning of the stream, so if
   * any tasks were ever XADDed before the very first worker in this group's
   * history came up, they'll still be delivered instead of silently skipped.
   * (Only matters on true first-ever creation — this call is a no-op via
   * BUSYGROUP on every subsequent worker start/restart, since the group
   * persists on the stream once created.)
   * MKSTREAM    = auto-create the stream if it doesn't yet exist.
   */
  private async initConsumerGroups(): Promise<void> {
    const streams = [
      RedisKeys.priorityStream(TaskPriority.HIGH),
      RedisKeys.priorityStream(TaskPriority.MEDIUM),
      RedisKeys.priorityStream(TaskPriority.LOW),
    ];

    for (const streamKey of streams) {
      try {
        await this.redis.xgroup("CREATE", streamKey, STREAM_CONSUMER_GROUP, "0", "MKSTREAM");
        logger.debug("[Worker] Consumer group created", { streamKey });
      } catch (err) {
        const message = (err as Error).message;
        if (!message.includes("BUSYGROUP")) throw err;
        // BUSYGROUP = group already exists — expected on worker restart
        logger.debug("[Worker] Consumer group already exists", { streamKey });
      }
    }
  }

  // -- Heartbeat --------------------------------------------------------------

  private startHeartbeat(): void {
    // Emit once immediately so the key exists before the first lock check
    void sendWorkerHeartbeat(this.redis, this.workerId, config.WORKER_HEARTBEAT_TTL_SECONDS);

    this.heartbeatTimer = setInterval(() => {
      void sendWorkerHeartbeat(this.redis, this.workerId, config.WORKER_HEARTBEAT_TTL_SECONDS);
    }, config.WORKER_HEARTBEAT_INTERVAL_MS);
  }

  // -- Consumer loop ----------------------------------------------------------

  private async runConsumerLoop(): Promise<void> {
    logger.info(`[Worker ${this.workerId}] Starting consumer polling loop...`);
    while (this.running) {
      try {
        // Back-pressure: yield when all concurrency slots are busy
        if (this.activeCount >= this.concurrency) {
          await sleep(50);
          continue;
        }

        const slotsAvailable = this.concurrency - this.activeCount;
        const messages = await this.fetchMessages(Math.min(slotsAvailable, 10));

        if (messages.length > 0) {
          logger.debug(`[Worker ${this.workerId}] Fetched ${messages.length} messages`);
        }

        for (const msg of messages) {
          if (!this.running) break;
          this.activeCount++;
          // Fire-and-forget; decrement counter in finally to maintain semaphore
          void this.processMessage(msg).finally(() => { this.activeCount--; });
        }
      } catch (err) {
        logger.error(`[Worker ${this.workerId}] Uncaught error in consumer loop`, err);
        await sleep(1000);
      }
    }

    // Final drain after running = false
    while (this.activeCount > 0) await sleep(100);
  }

  /**
   * Priority-ordered fetch:
   *  1. Non-blocking sweep: HIGH ? MEDIUM ? LOW (return immediately if any hit)
   *  2. If all empty: blocking XREADGROUP across all three (sorted by priority)
   */
  private async fetchMessages(maxCount: number): Promise<ConsumedMessage[]> {
    const highKey   = RedisKeys.priorityStream(TaskPriority.HIGH);
    const mediumKey = RedisKeys.priorityStream(TaskPriority.MEDIUM);
    const lowKey    = RedisKeys.priorityStream(TaskPriority.LOW);
    // Non-blocking priority sweep
    for (const streamKey of [highKey, mediumKey, lowKey]) {
      try {
        logger.debug(`[Worker ${this.workerId}] xreadgroup (non-blocking) on ${streamKey}`);
        const raw = (await this.blockingRedis.xreadgroup(
          "GROUP", STREAM_CONSUMER_GROUP, this.workerId,
          "COUNT", maxCount,
          "STREAMS", streamKey, ">",
        )) as XReadGroupResponse;

        if (raw !== null && raw.length > 0) {
          const streamResult = raw[0];
          if (streamResult !== undefined && streamResult[1].length > 0) {
            logger.debug(`[Worker ${this.workerId}] Received ${streamResult[1].length} messages from ${streamKey}`);
            return streamResult[1]
              .map(([id, fields]) => parseStreamEntry(streamKey, id, fields))
              .filter((m): m is ConsumedMessage => m !== null);
          }
        }
      } catch (err: any) {
        if (err.message?.includes('Command timed out')) {
          continue;
        }
        logger.error('[Worker Loop Error]', err);
        await sleep(1000);
      }
    }

    // All queues empty - block across all three and sort result by priority
    try {
      logger.debug(`[Worker ${this.workerId}] xreadgroup (blocking) across all streams for ${this.blockTimeoutMs}ms...`);
      const raw = (await this.blockingRedis.xreadgroup(
        "GROUP", STREAM_CONSUMER_GROUP, this.workerId,
        "COUNT", maxCount,
        "BLOCK", this.blockTimeoutMs,
        "STREAMS", highKey, mediumKey, lowKey,
        ">", ">", ">",
      )) as XReadGroupResponse;

      if (raw === null) return [];

      const messages: ConsumedMessage[] = [];
      for (const [streamKey, entries] of raw) {
        if (entries.length > 0) {
          logger.debug(`[Worker ${this.workerId}] Received ${entries.length} messages from ${streamKey} (blocking)`);
        }
        for (const [id, fields] of entries) {
          const m = parseStreamEntry(streamKey, id, fields);
          if (m !== null) messages.push(m);
        }
      }

      // Sort within batch so HIGH messages execute before MEDIUM / LOW
      return messages.sort(
        (a, b) => (PRIORITY_ORDER[a.priority] ?? 2) - (PRIORITY_ORDER[b.priority] ?? 2),
      );
    } catch (err: any) {
      if (err.message?.includes('Command timed out')) {
        // Suppress timeout error on blocking reads and continue loop
        return [];
      }
      logger.error('[Worker Loop Error]', err);
      await sleep(1000); // Backoff delay
      return [];
    }
  }

  // -- Message execution ------------------------------------------------------

  private async processMessage(msg: ConsumedMessage): Promise<void> {
    const { streamKey, entryId, taskId } = msg;
    logger.debug(`[Worker ${this.workerId}] Processing message ${entryId} for task ${taskId} from ${streamKey}`);
    const db = getDb();

    // 1. Load full task record
    const task = await db.task.findUnique({
      where:  { id: taskId },
      select: {
        id:         true,
        type:       true,
        status:     true,
        payload:    true,
        retryCount: true,
        maxRetries: true,
      },
    });

    if (task === null) {
      logger.warn("[Worker] Task not found in DB — ACK-ing orphaned entry", { taskId });
      await this.redis.xack(streamKey, STREAM_CONSUMER_GROUP, entryId);
      return;
    }

    // Already in a terminal or cancelled state — nothing to do
    if (
      task.status === TaskStatus.COMPLETED  ||
      task.status === TaskStatus.CANCELLED  ||
      task.status === TaskStatus.DEAD_LETTER
    ) {
      logger.info("[Worker] Task already terminal — ACK-ing", { taskId, status: task.status });
      await this.redis.xack(streamKey, STREAM_CONSUMER_GROUP, entryId);
      return;
    }

    // 2. Acquire distributed lock (idempotency guard against duplicate delivery)
    const locked = await acquireTaskLock(this.redis, taskId, this.workerId);
    if (!locked) {
      logger.warn("[Worker] Could not acquire lock — another worker owns it", { taskId });
      await this.redis.xack(streamKey, STREAM_CONSUMER_GROUP, entryId);
      return;
    }

    const startedAt = Date.now();

    // 3. Mark RUNNING in Postgres
    await db.task.update({
      where: { id: taskId },
      data: {
        status:     TaskStatus.RUNNING,
        lockedById: this.workerId,
        lockedAt:   new Date(),
        executedAt: new Date(),
      },
    });

    await db.taskLog.create({
      data: {
        taskId,
        workerId: this.workerId,
        level:    LogLevel.INFO,
        event:    "task.started",
        message:  `Worker ${this.workerId} started executing task`,
        metadata: { retryCount: task.retryCount, attemptNumber: task.retryCount + 1 },
      },
    });

    // 4. Lock renewal timer — keeps lock alive during long execution
    const renewIntervalMs = Math.floor((LOCK_TTL_SECONDS * 1_000) / 3);
    const renewTimer = setInterval(() => {
      void renewTaskLock(this.redis, taskId, this.workerId);
    }, renewIntervalMs);

    // 5. Execute handler
    try {
      const handler = this.handlers.get(task.type);
      if (handler === undefined) {
        throw new Error(`No handler registered for task type "${task.type}"`);
      }

      const ctx: TaskContext = {
        taskId,
        type:          task.type,
        workerId:      this.workerId,
        retryCount:    task.retryCount,
        attemptNumber: task.retryCount + 1,
        renewLock:     async () => {
          await renewTaskLock(this.redis, taskId, this.workerId);
        },
      };

      await handler(task.payload as Record<string, unknown>, ctx);

      // -- Success ----------------------------------------------------------
      const durationMs = Date.now() - startedAt;

      await db.task.update({
        where: { id: taskId },
        data: {
          status:      TaskStatus.COMPLETED,
          completedAt: new Date(),
          lockedById:  null,
          lockedAt:    null,
          result:      { success: true, durationMs },
        },
      });

      await db.taskLog.create({
        data: {
          taskId,
          workerId: this.workerId,
          level:    LogLevel.INFO,
          event:    "task.completed",
          message:  "Task completed successfully",
          metadata: { durationMs },
        },
      });

      logger.info("[Worker] Task completed", { taskId, durationMs });
    } catch (err) {
      const error      = err as Error;
      const durationMs = Date.now() - startedAt;

      logger.error("[Worker] Task failed", {
        taskId,
        error:      error.message,
        durationMs,
      });

      await this.handleFailure(taskId, task.type, msg.priority, task.retryCount, task.maxRetries, error);
    } finally {
      clearInterval(renewTimer);
      // ACK regardless of outcome — retry/DLQ path handles re-delivery via ZADD
      await this.redis.xack(streamKey, STREAM_CONSUMER_GROUP, entryId);
      await releaseTaskLock(this.redis, taskId, this.workerId);
    }
  }

  /**
   * Routes a failed task to either exponential-backoff retry or the dead-letter queue.
   */
  private async handleFailure(
    taskId:     string,
    type:       string,
    priority:   TaskPriority,
    retryCount: number,
    maxRetries: number,
    error:      Error,
  ): Promise<void> {
    const db = getDb();

    if (retryCount < maxRetries) {
      // Exponential backoff: 2^attempt seconds, capped at 1 hour
      const backoffMs = Math.min(Math.pow(2, retryCount) * 1_000, 3_600_000);
      const retryAt   = new Date(Date.now() + backoffMs);

      await this.redis.zadd(RedisKeys.delayedQueue(), retryAt.getTime(), taskId);

      await db.task.update({
        where: { id: taskId },
        data: {
          status:     TaskStatus.RETRYING,
          retryCount: { increment: 1 },
          lockedById: null,
          lockedAt:   null,
          result:     { success: false, error: error.message },
        },
      });

      await db.taskLog.create({
        data: {
          taskId,
          workerId: this.workerId,
          level:    LogLevel.WARN,
          event:    "task.retrying",
          message:  `Task failed — retry ${retryCount + 1}/${maxRetries} in ${backoffMs}ms`,
          metadata: {
            error:   error.message,
            stack:   error.stack,
            retryAt: retryAt.toISOString(),
            backoffMs,
          },
        },
      });

      logger.warn("[Worker] Task scheduled for retry", {
        taskId,
        attempt: retryCount + 1,
        maxRetries,
        retryAt: retryAt.toISOString(),
      });
    } else {
      // Dead-letter: max retries exhausted
      await this.redis.xadd(
        RedisKeys.deadLetterStream(),
        "*",
        "taskId",        taskId,
        "type",          type,
        "priority",      priority,
        "reason",        "max_retries_exceeded",
        "errorMessage",  error.message,
        "deadLetteredAt", new Date().toISOString(),
      );

      await db.task.update({
        where: { id: taskId },
        data: {
          status:      TaskStatus.DEAD_LETTER,
          completedAt: new Date(),
          lockedById:  null,
          lockedAt:    null,
          result:      { success: false, error: error.message, deadLettered: true },
        },
      });

      await db.taskLog.create({
        data: {
          taskId,
          workerId: this.workerId,
          level:    LogLevel.ERROR,
          event:    "task.dead_lettered",
          message:  `Task exhausted ${maxRetries} retries — moved to dead-letter queue`,
          metadata: { error: error.message, maxRetries },
        },
      });

      logger.error("[Worker] Task dead-lettered", { taskId, maxRetries });
    }
  }
}

// -----------------------------------------------------------------------------
// Module-level helpers
// -----------------------------------------------------------------------------

/**
 * Converts a flat ioredis field array ["k1","v1","k2","v2",...] into a
 * ConsumedMessage, or returns null for tombstone (deleted) entries.
 */
function parseStreamEntry(
  streamKey: string,
  entryId:   string,
  fields:    string[] | null,
): ConsumedMessage | null {
  // Null fields = Redis stream tombstone (entry was deleted, skip it)
  if (fields === null) return null;

  const map: Record<string, string> = {};
  for (let i = 0; i + 1 < fields.length; i += 2) {
    const k = fields[i];
    const v = fields[i + 1];
    if (k !== undefined && v !== undefined) map[k] = v;
  }

  const { taskId, type, priority } = map;
  if (taskId === undefined || type === undefined || priority === undefined) {
    logger.warn("[Worker] Malformed stream entry — missing required fields", {
      streamKey,
      entryId,
      fields: map,
    });
    return null;
  }

  return {
    streamKey,
    entryId,
    taskId,
    type,
    priority: priority as TaskPriority,
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

