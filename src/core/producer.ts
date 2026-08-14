/**
 * src/core/producer.ts
 *
 * Task queue producer — creates Task records in Postgres and routes them to
 * the appropriate Redis data structure based on scheduling requirements.
 *
 * Immediate tasks  ? XADD to priority Redis Stream  (scheduler:stream:{priority})
 * Delayed  tasks   ? ZADD to Redis Sorted Set        (scheduler:delayed:zset)
 *                    with Unix-ms timestamp as score
 *
 * Design notes:
 *  - Stream-first ordering for immediate tasks prevents a worker from reading
 *    a QUEUED record that is still PENDING in Postgres due to a race.
 *  - Both the stream XADD and the Postgres status update are intentionally
 *    non-transactional. The recovery service heals any inconsistency left by a
 *    crash between the two writes.
 */

import { TaskPriority, TaskStatus, Prisma } from "@prisma/client";
import { getDb } from "../infra/db";
import { getRedisClient } from "../infra/redis/redis-client";
import { RedisKeys } from "../infra/redis/redis-keys";
import { logger } from "../infra/logger";
import type { EnqueueTaskOptions, StreamTaskMessage } from "../types";

// -----------------------------------------------------------------------------
// Types
// -----------------------------------------------------------------------------

export interface CreateTaskResult {
  taskId: string;
  status: TaskStatus;
  enqueuedImmediately: boolean;
}

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

/**
 * Creates a validated Task record and routes it for execution.
 *
 * @throws if Postgres write fails or Redis is unavailable.
 */
export async function createTask(
  options: EnqueueTaskOptions,
): Promise<CreateTaskResult> {
  const db    = getDb();
  const redis = getRedisClient();

  const priority    = options.priority ?? TaskPriority.MEDIUM;
  const scheduledAt = options.scheduledAt !== undefined
    ? new Date(options.scheduledAt)
    : new Date();
  const maxRetries  = options.maxRetries ?? 3;

  // 1. Persist source-of-truth record in Postgres (always PENDING on creation)
  const task = await db.task.create({
    data: {
      type:        options.type,
      // Cast to Prisma.InputJsonObject — structurally identical to Record<string, unknown>
      // but satisfies the generated client's strict Json field type.
      payload:     options.payload as Prisma.InputJsonObject,
      priority,
      status:      TaskStatus.PENDING,
      scheduledAt,
      maxRetries,
    },
    select: { id: true },
  });

  const isDelayed = scheduledAt.getTime() > Date.now();

  if (isDelayed) {
    // 2a. Delayed path — add to sorted set; score = epoch-ms for ZRANGEBYSCORE
    await redis.zadd(
      RedisKeys.delayedQueue(),
      scheduledAt.getTime(),
      task.id,
    );

    logger.info("[Producer] Task scheduled (delayed)", {
      taskId:      task.id,
      type:        options.type,
      priority,
      scheduledAt: scheduledAt.toISOString(),
    });

    return { taskId: task.id, status: TaskStatus.PENDING, enqueuedImmediately: false };
  }

  // 2b. Immediate path — push to stream first, then upgrade Postgres status
  await enqueueToStream(task.id, options.type, priority);

  await db.task.update({
    where: { id: task.id },
    data:  { status: TaskStatus.QUEUED },
  });

  logger.info("[Producer] Task enqueued (immediate)", {
    taskId:  task.id,
    type:    options.type,
    priority,
  });

  return { taskId: task.id, status: TaskStatus.QUEUED, enqueuedImmediately: true };
}

/**
 * Pushes a task entry into the appropriate priority Redis Stream via XADD.
 *
 * Called by the producer for immediate tasks and by the scheduler when
 * promoting delayed tasks.  Returns the Redis-assigned stream entry ID.
 *
 * @throws if XADD returns null (stream maxlen truncation or server error).
 */
export async function enqueueToStream(
  taskId:   string,
  type:     string,
  priority: TaskPriority,
): Promise<string> {
  const redis     = getRedisClient();
  const streamKey = RedisKeys.priorityStream(priority);

  const message: StreamTaskMessage = {
    taskId,
    type,
    priority,
    enqueuedAt: new Date().toISOString(),
  };

  // XADD with auto-generated monotonic entry ID ("*")
  const entryId = await redis.xadd(
    streamKey,
    "*",
    "taskId",     message.taskId,
    "type",       message.type,
    "priority",   message.priority,
    "enqueuedAt", message.enqueuedAt,
  );

  if (entryId === null) {
    throw new Error(
      `XADD returned null for stream "${streamKey}" (taskId="${taskId}")`,
    );
  }

  logger.debug("[Producer] XADD", { streamKey, entryId, taskId, priority });
  return entryId;
}
