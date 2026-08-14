/**
 * src/core/scheduler.ts
 *
 * Delayed Task Promotion Scheduler
 *
 * Polls the Redis Sorted Set (scheduler:delayed:zset) and promotes tasks whose
 * scheduledAt score <= Date.now() into the appropriate priority stream.
 *
 * Distributed leader lock (scheduler:lock:scheduler-leader) ensures only one
 * scheduler instance promotes tasks at a time — others stand by and take over
 * automatically if the leader crashes (TTL expiry).
 *
 * Architecture:
 *   ZRANGEBYSCORE [-inf, now] --Lua (atomic pop)--? XADD stream:{priority}
 *                                                  --? PG status = QUEUED
 */

import { TaskStatus } from "@prisma/client";
import type Redis from "ioredis";
import { getDb } from "../infra/db";
import { getRedisClient } from "../infra/redis/redis-client";
import { RedisKeys, LOCK_TTL_SECONDS } from "../infra/redis/redis-keys";
import { enqueueToStream } from "./producer";
import { logger } from "../infra/logger";
import { config } from "../config";

// -----------------------------------------------------------------------------
// Lua Scripts
// -----------------------------------------------------------------------------

/**
 * Atomically scans the delayed ZSET for ready tasks and removes them in one
 * round-trip, preventing double-promotion under concurrent scheduler instances.
 *
 * KEYS[1] = delayed zset key
 * ARGV[1] = current Unix-ms timestamp (max score)
 * ARGV[2] = max batch size
 * Returns  = array of taskId strings
 */
const POP_READY_TASKS_SCRIPT = `
local members = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, tonumber(ARGV[2]))
if #members == 0 then return {} end
redis.call("ZREM", KEYS[1], unpack(members))
return members
`;

/**
 * Generic SET NX PX lock acquisition.
 * Returns 1 on success, 0 if already held.
 */
const ACQUIRE_LOCK_SCRIPT = `
if redis.call("SET", KEYS[1], ARGV[1], "NX", "PX", tonumber(ARGV[2])) then
  return 1
else
  return 0
end
`;

/**
 * Compare-and-PEXPIRE for lock renewal.
 * Returns 1 if renewed (still owner), 0 if expired or stolen.
 */
const RENEW_LOCK_SCRIPT = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], tonumber(ARGV[2]))
else
  return 0
end
`;

// -----------------------------------------------------------------------------
// SchedulerService
// -----------------------------------------------------------------------------

export class SchedulerService {
  private readonly redis: Redis;
  private readonly schedulerId: string;

  private isLeader = false;
  private running  = false;
  private pollTimer:  NodeJS.Timeout | null = null;
  private renewTimer: NodeJS.Timeout | null = null;

  constructor() {
    this.redis       = getRedisClient();
    this.schedulerId = `scheduler:${process.pid}:${Date.now()}`;
  }

  async start(): Promise<void> {
    this.running = true;
    logger.info("[Scheduler] Starting", { schedulerId: this.schedulerId });
    this.scheduleNextTick();
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.pollTimer  !== null) { clearTimeout(this.pollTimer);   this.pollTimer  = null; }
    if (this.renewTimer !== null) { clearInterval(this.renewTimer); this.renewTimer = null; }
    await this.releaseLeaderLock();
    logger.info("[Scheduler] Stopped", { schedulerId: this.schedulerId });
  }

  // -- Tick scheduling --------------------------------------------------------

  private scheduleNextTick(): void {
    if (!this.running) return;
    this.pollTimer = setTimeout(() => {
      void this.tick().finally(() => this.scheduleNextTick());
    }, config.SCHEDULER_POLL_INTERVAL_MS);
  }

  private async tick(): Promise<void> {
    try {
      // Step 1: ensure leadership
      if (!this.isLeader) {
        this.isLeader = await this.acquireLeaderLock();
        if (!this.isLeader) {
          logger.debug("[Scheduler] Not leader, skipping tick");
          return;
        }
        this.startLeaderRenewal();
        logger.info("[Scheduler] Became leader", { schedulerId: this.schedulerId });
      }

      // Step 2: atomically pop ready task IDs from ZSET
      const taskIds = await this.popReadyTaskIds(Date.now(), config.SCHEDULER_BATCH_SIZE);
      if (taskIds.length === 0) return;

      logger.info("[Scheduler] Promoting delayed tasks", { count: taskIds.length });

      // Step 3: load metadata + enqueue to streams in parallel
      await this.promoteTasks(taskIds);
    } catch (err) {
      logger.error("[Scheduler] Tick error", { error: (err as Error).message });
    }
  }

  // -- Delayed task promotion -------------------------------------------------

  /**
   * Executes the atomic Lua pop script.
   * Returns an array of taskId strings whose score <= nowMs.
   */
  private async popReadyTaskIds(nowMs: number, batchSize: number): Promise<string[]> {
    const result = await this.redis.eval(
      POP_READY_TASKS_SCRIPT,
      1,
      RedisKeys.delayedQueue(),
      String(nowMs),
      String(batchSize),
    );

    if (!Array.isArray(result)) return [];
    return (result as Array<string | Buffer>).map((v) => v.toString());
  }

  /**
   * Loads task metadata from Postgres, enqueues each to its priority stream,
   * and updates the status to QUEUED.
   *
   * Uses Promise.allSettled so a single failure doesn't abort the whole batch —
   * failed tasks are re-inserted into the ZSET with a short delay.
   */
  private async promoteTasks(taskIds: string[]): Promise<void> {
    const db = getDb();

    const tasks = await db.task.findMany({
      where: {
        id:     { in: taskIds },
        status: { in: [TaskStatus.PENDING, TaskStatus.RETRYING] },
      },
      select: { id: true, type: true, priority: true },
    });

    // taskIds that exist in the ZSET but not in PG (cancelled/deleted) are silently dropped
    await Promise.allSettled(
      tasks.map(async (task) => {
        try {
          await enqueueToStream(task.id, task.type, task.priority);
          await db.task.update({
            where: { id: task.id },
            data:  { status: TaskStatus.QUEUED },
          });
          logger.debug("[Scheduler] Task promoted", {
            taskId:   task.id,
            priority: task.priority,
          });
        } catch (err) {
          // Back-off 5 s before retry so we don't hammer a degraded system
          await this.redis.zadd(
            RedisKeys.delayedQueue(),
            Date.now() + 5_000,
            task.id,
          );
          logger.error("[Scheduler] Promotion failed — re-queued", {
            taskId: task.id,
            error:  (err as Error).message,
          });
        }
      }),
    );
  }

  // -- Leader lock ------------------------------------------------------------

  private async acquireLeaderLock(): Promise<boolean> {
    const result = await this.redis.eval(
      ACQUIRE_LOCK_SCRIPT,
      1,
      RedisKeys.schedulerLeaderLock(),
      this.schedulerId,
      String(LOCK_TTL_SECONDS * 1_000),
    );
    return result === 1;
  }

  private startLeaderRenewal(): void {
    if (this.renewTimer !== null) return; // Already running

    // Renew at 1/3 of TTL to give plenty of headroom before expiry
    const intervalMs = Math.floor((LOCK_TTL_SECONDS * 1_000) / 3);
    this.renewTimer = setInterval(() => { void this.renewLeaderLock(); }, intervalMs);
  }

  private async renewLeaderLock(): Promise<void> {
    const result = await this.redis.eval(
      RENEW_LOCK_SCRIPT,
      1,
      RedisKeys.schedulerLeaderLock(),
      this.schedulerId,
      String(LOCK_TTL_SECONDS * 1_000),
    );

    if (result !== 1) {
      logger.warn("[Scheduler] Lost leader lock — another instance took over");
      this.isLeader = false;
      if (this.renewTimer !== null) {
        clearInterval(this.renewTimer);
        this.renewTimer = null;
      }
    }
  }

  private async releaseLeaderLock(): Promise<void> {
    if (!this.isLeader) return;
    // Only delete if we still own it (compare-and-delete)
    const current = await this.redis.get(RedisKeys.schedulerLeaderLock());
    if (current === this.schedulerId) {
      await this.redis.del(RedisKeys.schedulerLeaderLock());
      logger.debug("[Scheduler] Leader lock released");
    }
    this.isLeader = false;
  }

  /**
   * Publicly exposed promotion trigger — bypasses leader-lock for test harnesses.
   * Useful for the E2E verification suite to force-promote delayed tasks without
   * waiting for the full poll interval.
   */
  public async promoteReadyTasks(): Promise<void> {
    const taskIds = await this.popReadyTaskIds(Date.now(), config.SCHEDULER_BATCH_SIZE);
    if (taskIds.length > 0) await this.promoteTasks(taskIds);
  }
}