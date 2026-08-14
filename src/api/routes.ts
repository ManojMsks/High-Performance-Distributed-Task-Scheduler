/**
 * src/api/routes.ts
 *
 * REST API route handlers for the task scheduler management plane.
 *
 * Endpoints:
 *   POST  /v1/tasks           � Ingest a new task (immediate or delayed)
 *   GET   /v1/tasks/:id       � Retrieve full task state + execution log
 *   POST  /v1/tasks/:id/cancel � Cancel a PENDING or QUEUED task
 *   GET   /v1/health          � Cluster health (Postgres + Redis + worker liveness)
 *   GET   /v1/metrics         � Queue depths, delayed count, task state breakdown
 */

import { Router, type Request, type Response } from "express";
import { z } from "zod";
import { TaskPriority, TaskStatus, WorkerStatus } from "@prisma/client";
import { getDb } from "../infra/db";
import { getRedisClient } from "../infra/redis/redis-client";
import { RedisKeys } from "../infra/redis/redis-keys";
import { createTask } from "../core/producer";
import { logger } from "../infra/logger";
import type { EnqueueTaskOptions } from "../types";

export const taskRouter = Router();

// -----------------------------------------------------------------------------
// Zod Schemas
// -----------------------------------------------------------------------------

const CreateTaskBody = z.object({
  type:        z.string().min(1).max(255),
  payload:     z.record(z.unknown()).default({}),
  priority:    z.enum(["HIGH", "MEDIUM", "LOW"]).optional(),
  scheduledAt: z.string().datetime({ offset: true }).optional(),
  maxRetries:  z.number().int().min(0).max(20).optional(),
});

// -----------------------------------------------------------------------------
// POST /v1/tasks
// -----------------------------------------------------------------------------

taskRouter.post("/tasks", async (req: Request, res: Response) => {
  const body = CreateTaskBody.parse(req.body);

  // Build EnqueueTaskOptions without spreading undefined values
  // (exactOptionalPropertyTypes requires properties to be absent, not undefined)
  const options: EnqueueTaskOptions = { type: body.type, payload: body.payload };
  if (body.priority    !== undefined) options.priority    = body.priority as TaskPriority;
  if (body.scheduledAt !== undefined) options.scheduledAt = new Date(body.scheduledAt);
  if (body.maxRetries  !== undefined) options.maxRetries  = body.maxRetries;

  const result = await createTask(options);

  logger.info("[API] Task created", { taskId: result.taskId, status: result.status });

  res.status(201).json({
    taskId:              result.taskId,
    status:              result.status,
    enqueuedImmediately: result.enqueuedImmediately,
  });
});

// -----------------------------------------------------------------------------
// GET /v1/tasks/:id
// -----------------------------------------------------------------------------

taskRouter.get("/tasks/:id", async (req: Request, res: Response) => {
  const db   = getDb();
  const id = req.params["id"] as string;

  if (id === undefined || id === "") {
    res.status(400).json({ error: { code: "BAD_REQUEST", message: "Task ID is required" } });
    return;
  }

  const task = await db.task.findUnique({
    where:   { id },
    include: {
      logs: { orderBy: { createdAt: "asc" } },
    },
  });

  if (task === null) {
    res.status(404).json({ error: { code: "NOT_FOUND", message: `Task "${id}" not found` } });
    return;
  }

  res.json(task);
});

// -----------------------------------------------------------------------------
// POST /v1/tasks/:id/cancel
// -----------------------------------------------------------------------------

const CANCELLABLE_STATUSES = new Set<TaskStatus>([
  TaskStatus.PENDING,
  TaskStatus.QUEUED,
  TaskStatus.RETRYING,
]);

taskRouter.post("/tasks/:id/cancel", async (req: Request, res: Response) => {
  const db    = getDb();
  const redis = getRedisClient();
  const id = req.params["id"] as string;

  if (id === undefined || id === "") {
    res.status(400).json({ error: { code: "BAD_REQUEST", message: "Task ID is required" } });
    return;
  }

  const task = await db.task.findUnique({
    where:  { id },
    select: { id: true, status: true },
  });

  if (task === null) {
    res.status(404).json({ error: { code: "NOT_FOUND", message: `Task "${id}" not found` } });
    return;
  }

  if (!CANCELLABLE_STATUSES.has(task.status)) {
    res.status(409).json({
      error: {
        code:    "CANNOT_CANCEL",
        message: `Task in status "${task.status}" cannot be cancelled`,
      },
    });
    return;
  }

  // Remove from delayed ZSET if present (covers PENDING + RETRYING)
  await redis.zrem(RedisKeys.delayedQueue(), id);

  // Update Postgres � workers check status before executing and will ACK + skip
  await db.task.update({
    where: { id },
    data:  { status: TaskStatus.CANCELLED },
  });

  logger.info("[API] Task cancelled", { taskId: id, previousStatus: task.status });

  res.json({ taskId: id, status: TaskStatus.CANCELLED });
});

// -----------------------------------------------------------------------------
// GET /v1/health
// -----------------------------------------------------------------------------

taskRouter.get("/health", async (_req: Request, res: Response) => {
  const db    = getDb();
  const redis = getRedisClient();

  // Ping both stores concurrently; don't let one failure block the other
  const [pgResult, redisResult] = await Promise.allSettled([
    db.$queryRaw`SELECT 1 AS ok`,
    redis.ping(),
  ]);

  const pgOk    = pgResult.status    === "fulfilled";
  const redisOk = redisResult.status === "fulfilled";

  // Count workers that sent a heartbeat in the last 60 s
  const workerCount = pgOk
    ? await db.worker.count({
        where: {
          status:          { not: WorkerStatus.OFFLINE },
          lastHeartbeatAt: { gt: new Date(Date.now() - 60_000) },
        },
      })
    : 0;

  // Queue depth snapshot (number of entries in each stream)
  const [highLen, mediumLen, lowLen, delayedCount] = redisOk
    ? await Promise.all([
        redis.xlen(RedisKeys.priorityStream(TaskPriority.HIGH)),
        redis.xlen(RedisKeys.priorityStream(TaskPriority.MEDIUM)),
        redis.xlen(RedisKeys.priorityStream(TaskPriority.LOW)),
        redis.zcard(RedisKeys.delayedQueue()),
      ])
    : [0, 0, 0, 0];

  const healthy = pgOk && redisOk;

  res.status(healthy ? 200 : 503).json({
    status:      healthy ? "healthy" : "degraded",
    postgres:    pgOk    ? "ok" : "error",
    redis:       redisOk ? "ok" : "error",
    workerCount,
    queueDepths: { high: highLen, medium: mediumLen, low: lowLen },
    delayed:     delayedCount,
    timestamp:   new Date().toISOString(),
  });
});

// -----------------------------------------------------------------------------
// GET /v1/metrics
// -----------------------------------------------------------------------------

taskRouter.get("/metrics", async (_req: Request, res: Response) => {
  const db    = getDb();
  const redis = getRedisClient();

  const [highLen, mediumLen, lowLen, delayedCount, dlqLen, taskCounts] =
    await Promise.all([
      redis.xlen(RedisKeys.priorityStream(TaskPriority.HIGH)),
      redis.xlen(RedisKeys.priorityStream(TaskPriority.MEDIUM)),
      redis.xlen(RedisKeys.priorityStream(TaskPriority.LOW)),
      redis.zcard(RedisKeys.delayedQueue()),
      redis.xlen(RedisKeys.deadLetterStream()),
      db.task.groupBy({ by: ["status"], _count: { id: true } }),
    ]);

  // Convert Prisma groupBy result to a plain object { STATUS: count }
  const byStatus: Partial<Record<TaskStatus, number>> = {};
  for (const row of taskCounts) {
    byStatus[row.status] = row._count.id;
  }

  res.json({
    queues: {
      high:   highLen,
      medium: mediumLen,
      low:    lowLen,
    },
    delayed:   delayedCount,
    deadLetter: dlqLen,
    byStatus,
    timestamp: new Date().toISOString(),
  });
});
