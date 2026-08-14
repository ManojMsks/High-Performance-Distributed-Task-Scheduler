/**
 * src/infra/redis/redis-keys.ts
 *
 * Centralised Redis key namespace registry and low-level atomic operations.
 *
 * Design decisions:
 *  - All keys share the `scheduler:` prefix for namespace isolation and
 *    easy key-space notifications / scanning in production.
 *  - Locks use Lua compare-and-delete to prevent a worker from releasing a
 *    lock it no longer owns (clock-drift-safe).
 *  - Heartbeat keys are plain STRING keys with a TTL; expiry == worker death.
 *  - Priority queues are Redis Streams (not plain Lists) so we get consumer
 *    groups, message acknowledgement, and replay semantics for free.
 *  - The delayed sorted set uses the task's scheduled Unix timestamp as score
 *    so ZRANGEBYSCORE gives us ready tasks in O(log N + M).
 */

import type Redis from "ioredis";
import { TaskPriority } from "@prisma/client";

// -----------------------------------------------------------------------------
// Constants
// -----------------------------------------------------------------------------

/** Root namespace for all scheduler-owned keys */
const NS = "scheduler" as const;

/**
 * TTL for a distributed task lock, in seconds.
 * Must exceed the maximum expected single-task execution time.
 * Workers renew this via heartbeat; stale locks expire automatically.
 */
export const LOCK_TTL_SECONDS = 60 as const;

/**
 * TTL for a worker heartbeat key, in seconds.
 * If a worker misses this window without renewal the key expires and
 * the recovery scheduler treats its in-flight tasks as stale.
 */
export const WORKER_HEARTBEAT_TTL_SECONDS = 30 as const;

/** Consumer group name shared by all workers reading from priority streams */
export const STREAM_CONSUMER_GROUP = `${NS}:workers` as const;

// -----------------------------------------------------------------------------
// Key Generators
// -----------------------------------------------------------------------------

export const RedisKeys = {
  /**
   * Sorted Set holding taskIds of delayed/scheduled tasks.
   * Score = Unix timestamp (ms) at which the task becomes eligible.
   *
   * Pattern:  scheduler:delayed:zset
   */
  delayedQueue: (): string => `${NS}:delayed:zset`,

  /**
   * Redis Stream keys for priority-tiered task queues.
   * Each stream is consumed by the same STREAM_CONSUMER_GROUP.
   *
   * Patterns:
   *   scheduler:stream:high
   *   scheduler:stream:medium
   *   scheduler:stream:low
   */
  priorityStream: (priority: TaskPriority): string =>
    `${NS}:stream:${priority.toLowerCase()}`,

  /**
   * Worker heartbeat key (STRING).
   * Value: ISO-8601 timestamp of last beat.
   * TTL:   WORKER_HEARTBEAT_TTL_SECONDS
   *
   * Pattern:  scheduler:worker:heartbeat:{workerId}
   */
  workerHeartbeat: (workerId: string): string =>
    `${NS}:worker:heartbeat:${workerId}`,

  /**
   * Distributed task lock key (STRING).
   * Value: workerId that acquired the lock.
   * TTL:   LOCK_TTL_SECONDS (auto-releases on worker crash)
   *
   * Pattern:  scheduler:lock:task:{taskId}
   */
  taskLock: (taskId: string): string => `${NS}:lock:task:${taskId}`,

  /**
   * Dead-letter Stream — tasks that exhausted all retries.
   * Pattern:  scheduler:stream:dead-letter
   */
  deadLetterStream: (): string => `${NS}:stream:dead-letter`,

  /**
   * Per-worker metric hash (tasks processed, errors, etc.)
   * Pattern:  scheduler:worker:metrics:{workerId}
   */
  workerMetrics: (workerId: string): string =>
    `${NS}:worker:metrics:${workerId}`,

  /**
   * Global scheduler lock — ensures a single scheduler promotes delayed tasks.
   * Pattern:  scheduler:lock:scheduler-leader
   */
  schedulerLeaderLock: (): string => `${NS}:lock:scheduler-leader`,
} as const;

// -----------------------------------------------------------------------------
// Lua Scripts (inline — loaded once, called by SHA via EVALSHA in production)
// -----------------------------------------------------------------------------

/**
 * Atomic SET-if-not-exists with TTL for lock acquisition.
 *
 * Equivalent to: SET key value NX PX ttlMs
 * Returns: 1 on success (lock acquired), 0 on failure (already held).
 *
 * We use a Lua script instead of SET NX so the semantics are identical
 * whether called from ioredis or from a Cluster pipeline.
 */
const LOCK_ACQUIRE_SCRIPT = `
local key   = KEYS[1]
local value = ARGV[1]
local ttlMs = tonumber(ARGV[2])
if redis.call("SET", key, value, "NX", "PX", ttlMs) then
  return 1
else
  return 0
end
` as const;

/**
 * Atomic compare-and-delete for lock release.
 *
 * Only deletes the key when its current value matches the expected owner.
 * Prevents Worker A from deleting a lock now held by Worker B after a
 * clock-skew-induced re-acquisition.
 *
 * Returns: 1 if deleted (lock was ours), 0 if not (already expired / stolen).
 */
const LOCK_RELEASE_SCRIPT = `
local key      = KEYS[1]
local expected = ARGV[1]
local current  = redis.call("GET", key)
if current == expected then
  return redis.call("DEL", key)
else
  return 0
end
` as const;

/**
 * Atomic lock renewal (PEXPIRE only if value matches owner).
 * Keeps the lock alive while the task is still in flight.
 *
 * Returns: 1 if renewed, 0 if expired or stolen.
 */
const LOCK_RENEW_SCRIPT = `
local key      = KEYS[1]
local expected = ARGV[1]
local ttlMs    = tonumber(ARGV[2])
local current  = redis.call("GET", key)
if current == expected then
  return redis.call("PEXPIRE", key, ttlMs)
else
  return 0
end
` as const;

// -----------------------------------------------------------------------------
// Public Operations
// -----------------------------------------------------------------------------

/**
 * Attempts to acquire a distributed lock for a task.
 *
 * @param redis    - ioredis client instance
 * @param taskId   - ID of the task being locked
 * @param workerId - ID of the worker attempting to acquire
 * @param ttlSec   - Lock TTL in seconds (defaults to LOCK_TTL_SECONDS)
 * @returns        - true if lock acquired, false if already held
 */
export async function acquireTaskLock(
  redis: Redis,
  taskId: string,
  workerId: string,
  ttlSec: number = LOCK_TTL_SECONDS,
): Promise<boolean> {
  const key = RedisKeys.taskLock(taskId);
  const ttlMs = ttlSec * 1_000;

  const result = await redis.eval(
    LOCK_ACQUIRE_SCRIPT,
    1,       // number of keys
    key,     // KEYS[1]
    workerId, // ARGV[1]
    String(ttlMs), // ARGV[2]
  );

  return result === 1;
}

/**
 * Releases a distributed task lock, but only if this worker still owns it.
 * Uses atomic compare-and-delete to prevent accidental lock theft.
 *
 * @param redis    - ioredis client instance
 * @param taskId   - ID of the task whose lock to release
 * @param workerId - ID of the worker releasing (must match current lock value)
 * @returns        - true if lock was released, false if already expired/stolen
 */
export async function releaseTaskLock(
  redis: Redis,
  taskId: string,
  workerId: string,
): Promise<boolean> {
  const key = RedisKeys.taskLock(taskId);

  const result = await redis.eval(
    LOCK_RELEASE_SCRIPT,
    1,
    key,
    workerId,
  );

  return result === 1;
}

/**
 * Renews a held task lock, extending its TTL.
 * Call this periodically from long-running task handlers to prevent expiry.
 *
 * @param redis    - ioredis client instance
 * @param taskId   - ID of the task whose lock to renew
 * @param workerId - Must match the value stored in the lock key
 * @param ttlSec   - New TTL in seconds (defaults to LOCK_TTL_SECONDS)
 * @returns        - true if renewed, false if lock expired or stolen
 */
export async function renewTaskLock(
  redis: Redis,
  taskId: string,
  workerId: string,
  ttlSec: number = LOCK_TTL_SECONDS,
): Promise<boolean> {
  const key = RedisKeys.taskLock(taskId);
  const ttlMs = ttlSec * 1_000;

  const result = await redis.eval(
    LOCK_RENEW_SCRIPT,
    1,
    key,
    workerId,
    String(ttlMs),
  );

  return result === 1;
}

/**
 * Sends (or renews) a worker heartbeat in Redis.
 *
 * Sets a STRING key with an ISO-8601 timestamp value and a TTL.
 * If the key expires without renewal, downstream recovery logic treats
 * all tasks locked by this worker as orphaned.
 *
 * @param redis    - ioredis client instance
 * @param workerId - Unique worker identifier
 * @param ttlSec   - Key TTL in seconds (defaults to WORKER_HEARTBEAT_TTL_SECONDS)
 */
export async function sendWorkerHeartbeat(
  redis: Redis,
  workerId: string,
  ttlSec: number = WORKER_HEARTBEAT_TTL_SECONDS,
): Promise<void> {
  const key = RedisKeys.workerHeartbeat(workerId);
  const now = new Date().toISOString();

  // SETEX: atomic set + expire in one round-trip
  await redis.setex(key, ttlSec, now);
}

/**
 * Checks whether a worker is currently alive by inspecting its heartbeat key.
 *
 * @param redis    - ioredis client instance
 * @param workerId - Worker to check
 * @returns        - ISO timestamp of last heartbeat, or null if expired/absent
 */
export async function getWorkerHeartbeat(
  redis: Redis,
  workerId: string,
): Promise<string | null> {
  return redis.get(RedisKeys.workerHeartbeat(workerId));
}

/**
 * Returns the set of all stream keys for the given priorities.
 * Useful for XREAD calls that span multiple priority streams.
 */
export function getAllPriorityStreamKeys(): Record<TaskPriority, string> {
  return {
    [TaskPriority.HIGH]:   RedisKeys.priorityStream(TaskPriority.HIGH),
    [TaskPriority.MEDIUM]: RedisKeys.priorityStream(TaskPriority.MEDIUM),
    [TaskPriority.LOW]:    RedisKeys.priorityStream(TaskPriority.LOW),
  };
}
