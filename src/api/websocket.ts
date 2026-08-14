/**
 * src/api/websocket.ts
 *
 * Real-Time Metrics Gateway — Socket.IO layer over the HTTP server.
 *
 * Broadcasts two event streams to connected dashboard clients:
 *
 *   "metrics"  (every METRICS_INTERVAL_MS)
 *     Queue depths (HIGH / MEDIUM / LOW / DELAYED / DLQ),
 *     active worker list with heartbeat ages,
 *     task status breakdown,
 *     recent task status transitions.
 *
 *   "logs"  (every LOG_INTERVAL_MS)
 *     New TaskLog rows since the last broadcast — level-tagged and
 *     ordered chronologically.
 *
 * The gateway is read-only; it never mutates state.
 */

import { type Server as HttpServer } from "http";
import { Server as SocketIO }        from "socket.io";
import { TaskPriority }              from "@prisma/client";
import { getDb }                     from "../infra/db";
import { getRedisClient }            from "../infra/redis/redis-client";
import { RedisKeys }                 from "../infra/redis/redis-keys";
import { logger }                    from "../infra/logger";

// -----------------------------------------------------------------------------
// Constants
// -----------------------------------------------------------------------------

const METRICS_INTERVAL_MS = 1_000;
const LOG_INTERVAL_MS     = 1_500;

/** Time window for "active" worker classification (ms). */
const WORKER_LIVENESS_WINDOW_MS = 60_000;

// -----------------------------------------------------------------------------
// Payload types emitted to clients
// -----------------------------------------------------------------------------

interface QueueDepths {
  high:    number;
  medium:  number;
  low:     number;
  delayed: number;
  dlq:     number;
}

interface WorkerInfo {
  id:              string;
  hostname:        string;
  pid:             number;
  status:          string;
  concurrency:     number;
  lastHeartbeatAt: string | null;
  /** Seconds since last heartbeat, or null if never seen. */
  heartbeatAgeSeconds: number | null;
}

interface TaskTransition {
  id:        string;
  type:      string;
  status:    string;
  priority:  string;
  updatedAt: string;
}

interface MetricsPayload {
  queues:          QueueDepths;
  workers:         { count: number; list: WorkerInfo[] };
  tasks:           { byStatus: Record<string, number> };
  recentTransitions: TaskTransition[];
  timestamp:       string;
}

interface LogEntry {
  id:        string;
  taskId:    string;
  workerId:  string | null;
  level:     string;
  event:     string;
  message:   string;
  metadata:  unknown;
  createdAt: string;
}

// -----------------------------------------------------------------------------
// State: track last-seen timestamps so we only emit deltas
// -----------------------------------------------------------------------------

let lastLogTime        = new Date(Date.now() - 5_000); // prime with last 5s of history
let lastTransitionTime = new Date(Date.now() - 5_000);

// -----------------------------------------------------------------------------
// Data collectors
// -----------------------------------------------------------------------------

async function collectMetrics(): Promise<MetricsPayload> {
  const db    = getDb();
  const redis = getRedisClient();
  const now   = Date.now();

  const [high, medium, low, delayed, dlq, rawWorkers, statusRows, transitions] =
    await Promise.all([
      redis.xlen(RedisKeys.priorityStream(TaskPriority.HIGH)),
      redis.xlen(RedisKeys.priorityStream(TaskPriority.MEDIUM)),
      redis.xlen(RedisKeys.priorityStream(TaskPriority.LOW)),
      redis.zcard(RedisKeys.delayedQueue()),
      redis.xlen(RedisKeys.deadLetterStream()),
      db.worker.findMany({
        where:   { lastHeartbeatAt: { gt: new Date(now - WORKER_LIVENESS_WINDOW_MS) } },
        select:  {
          id: true, hostname: true, pid: true,
          status: true, concurrency: true, lastHeartbeatAt: true,
        },
        orderBy: { lastHeartbeatAt: "desc" },
      }),
      db.task.groupBy({ by: ["status"], _count: { id: true } }),
      db.task.findMany({
        where:   { updatedAt: { gt: lastTransitionTime } },
        orderBy: { updatedAt: "asc" },
        take:    20,
        select:  { id: true, type: true, status: true, priority: true, updatedAt: true },
      }),
    ]);

  if (transitions.length > 0) {
    const last = transitions[transitions.length - 1];
    if (last !== undefined) lastTransitionTime = last.updatedAt;
  }

  const workers: WorkerInfo[] = rawWorkers.map((w) => ({
    id:          w.id,
    hostname:    w.hostname,
    pid:         w.pid,
    status:      w.status,
    concurrency: w.concurrency,
    lastHeartbeatAt: w.lastHeartbeatAt?.toISOString() ?? null,
    heartbeatAgeSeconds: w.lastHeartbeatAt !== null
      ? Math.floor((now - w.lastHeartbeatAt.getTime()) / 1_000)
      : null,
  }));

  const byStatus: Record<string, number> = {};
  for (const row of statusRows) byStatus[row.status] = row._count.id;

  return {
    queues:   { high, medium, low, delayed, dlq },
    workers:  { count: workers.length, list: workers },
    tasks:    { byStatus },
    recentTransitions: transitions.map((t) => ({
      id:        t.id,
      type:      t.type,
      status:    t.status,
      priority:  t.priority,
      updatedAt: t.updatedAt.toISOString(),
    })),
    timestamp: new Date().toISOString(),
  };
}

async function collectNewLogs(): Promise<LogEntry[]> {
  const db   = getDb();
  const rows = await db.taskLog.findMany({
    where:   { createdAt: { gt: lastLogTime } },
    orderBy: { createdAt: "asc" },
    take:    50,
    select: {
      id: true, taskId: true, workerId: true,
      level: true, event: true, message: true,
      metadata: true, createdAt: true,
    },
  });

  if (rows.length > 0) {
    const last = rows[rows.length - 1];
    if (last !== undefined) lastLogTime = last.createdAt;
  }

  return rows.map((r) => ({
    id:        r.id,
    taskId:    r.taskId,
    workerId:  r.workerId,
    level:     r.level,
    event:     r.event,
    message:   r.message,
    metadata:  r.metadata,
    createdAt: r.createdAt.toISOString(),
  }));
}

// -----------------------------------------------------------------------------
// Broadcast loops
// -----------------------------------------------------------------------------

function startMetricsBroadcast(io: SocketIO): void {
  setInterval(async () => {
    if (io.engine.clientsCount === 0) return; // skip if no clients

    try {
      const payload = await collectMetrics();
      io.emit("metrics", payload);
    } catch (err) {
      // Don't crash the server for a metrics blip
      logger.warn("[WS] Metrics collection error", { error: (err as Error).message });
    }
  }, METRICS_INTERVAL_MS);
}

function startLogBroadcast(io: SocketIO): void {
  setInterval(async () => {
    if (io.engine.clientsCount === 0) return;

    try {
      const entries = await collectNewLogs();
      if (entries.length > 0) {
        io.emit("logs", entries);
      }
    } catch (err) {
      logger.warn("[WS] Log broadcast error", { error: (err as Error).message });
    }
  }, LOG_INTERVAL_MS);
}

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

/**
 * Attaches Socket.IO to an existing HTTP server and starts broadcasting
 * real-time metrics and log streams to all connected dashboard clients.
 *
 * @returns The Socket.IO server instance (for testing / graceful shutdown).
 */
export function attachWebSocket(httpServer: HttpServer): SocketIO {
  const io = new SocketIO(httpServer, {
    cors:       { origin: "*", methods: ["GET", "POST"] },
    transports: ["websocket", "polling"],
    // Increase ping timeout for slow networks
    pingTimeout:  20_000,
    pingInterval: 10_000,
  });

  io.on("connection", (socket) => {
    const clientIp = socket.handshake.address;
    logger.info("[WS] Dashboard client connected", { id: socket.id, ip: clientIp });

    // Send a full metrics snapshot immediately on connect (no waiting for interval)
    collectMetrics()
      .then((payload) => socket.emit("metrics", payload))
      .catch((err: unknown) => {
        logger.warn("[WS] Initial snapshot error", { error: (err as Error).message });
      });

    socket.on("disconnect", (reason) => {
      logger.info("[WS] Client disconnected", { id: socket.id, reason });
    });
  });

  startMetricsBroadcast(io);
  startLogBroadcast(io);

  logger.info("[WS] Socket.IO gateway attached");
  return io;
}
