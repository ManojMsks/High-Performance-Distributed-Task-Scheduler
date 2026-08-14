/**
 * src/cli/admin.ts
 *
 * Terminal admin utility for the Distributed Task Scheduler.
 * Uses plain process.argv parsing � no additional dependencies.
 *
 * Usage:
 *   npx ts-node src/cli/admin.ts <command> [options]
 *
 * Commands:
 *   dlq:inspect              List all dead-letter queue entries (latest 100)
 *   dlq:replay [--id <id>]   Replay one or all DLQ entries back into the scheduler
 *   status                   Print live cluster status (queues, workers, DB counts)
 *   tasks:list [--status <S>] List recent tasks optionally filtered by status
 */

import "dotenv/config";
import { TaskStatus, TaskPriority } from "@prisma/client";
import { getDb, disconnectDb } from "../infra/db";
import { getRedisClient, closeRedisClient } from "../infra/redis/redis-client";
import { RedisKeys, STREAM_CONSUMER_GROUP } from "../infra/redis/redis-keys";

// -----------------------------------------------------------------------------
// ANSI colour helpers
// -----------------------------------------------------------------------------

const C = {
  reset:  "\x1b[0m",
  bold:   "\x1b[1m",
  cyan:   "\x1b[36m",
  green:  "\x1b[32m",
  yellow: "\x1b[33m",
  red:    "\x1b[31m",
  gray:   "\x1b[90m",
};

const fmt = {
  header: (s: string) => `\n${C.bold}${C.cyan}${s}${C.reset}`,
  ok:     (s: string) => `${C.green}${s}${C.reset}`,
  warn:   (s: string) => `${C.yellow}${s}${C.reset}`,
  error:  (s: string) => `${C.red}${s}${C.reset}`,
  dim:    (s: string) => `${C.gray}${s}${C.reset}`,
  label:  (k: string, v: unknown) => `  ${C.bold}${k}${C.reset}: ${String(v)}`,
};

// -----------------------------------------------------------------------------
// Arg parser
// -----------------------------------------------------------------------------

interface ParsedArgs {
  command:  string;
  flags:    Record<string, string>;
}

function parseArgs(): ParsedArgs {
  const [, , command = "help", ...rest] = process.argv;
  const flags: Record<string, string> = {};

  for (let i = 0; i < rest.length - 1; i++) {
    const key = rest[i];
    const val = rest[i + 1];
    if (key !== undefined && key.startsWith("--") && val !== undefined && !val.startsWith("--")) {
      flags[key.slice(2)] = val;
      i++; // skip value
    }
  }

  return { command, flags };
}

// -----------------------------------------------------------------------------
// Commands
// -----------------------------------------------------------------------------

async function cmdDlqInspect(): Promise<void> {
  const redis = getRedisClient();
  console.log(fmt.header("Dead-Letter Queue Entries"));

  const entries = await redis.xrange(RedisKeys.deadLetterStream(), "-", "+", "COUNT", 100);

  if (entries.length === 0) {
    console.log(fmt.ok("  DLQ is empty � nothing to report."));
    return;
  }

  console.log(fmt.warn(`  ${entries.length} entries\n`));

  for (const [entryId, fields] of entries) {
    const map: Record<string, string> = {};
    for (let i = 0; i + 1 < fields.length; i += 2) {
      const k = fields[i];
      const v = fields[i + 1];
      if (k !== undefined && v !== undefined) map[k] = v;
    }
    console.log(`  ${fmt.dim(entryId)}`);
    console.log(fmt.label("  taskId",        map["taskId"]        ?? "?"));
    console.log(fmt.label("  type",          map["type"]          ?? "?"));
    console.log(fmt.label("  priority",      map["priority"]      ?? "?"));
    console.log(fmt.label("  reason",        map["reason"]        ?? "?"));
    console.log(fmt.label("  deadLetteredAt", map["deadLetteredAt"] ?? "?"));
    if (map["errorMessage"] !== undefined) {
      console.log(fmt.label("  error",        fmt.error(map["errorMessage"])));
    }
    console.log();
  }
}

async function cmdDlqReplay(targetId?: string): Promise<void> {
  const redis = getRedisClient();
  const db    = getDb();

  const entries = await redis.xrange(RedisKeys.deadLetterStream(), "-", "+", "COUNT", 100);
  const toReplay = targetId !== undefined
    ? entries.filter(([id]) => id === targetId)
    : entries;

  if (toReplay.length === 0) {
    console.log(fmt.warn("  No matching DLQ entries found."));
    return;
  }

  console.log(fmt.header(`Replaying ${toReplay.length} DLQ entry/entries`));

  let replayed = 0;
  for (const [entryId, fields] of toReplay) {
    const map: Record<string, string> = {};
    for (let i = 0; i + 1 < fields.length; i += 2) {
      const k = fields[i];
      const v = fields[i + 1];
      if (k !== undefined && v !== undefined) map[k] = v;
    }

    const taskId = map["taskId"];
    if (taskId === undefined) {
      console.log(fmt.error(`  Skipping malformed entry ${entryId} (no taskId)`));
      continue;
    }

    // Reset task to PENDING in Postgres
    const updated = await db.task.updateMany({
      where: { id: taskId, status: TaskStatus.DEAD_LETTER },
      data: {
        status:      TaskStatus.PENDING,
        retryCount:  0,
        lockedById:  null,
        lockedAt:    null,
        completedAt: null,
      },
    });

    if (updated.count === 0) {
      console.log(fmt.warn(`  Task ${taskId} not in DEAD_LETTER state � skipping`));
      continue;
    }

    // Re-add to delayed ZSET for immediate promotion (score = now)
    await redis.zadd(RedisKeys.delayedQueue(), Date.now(), taskId);

    // Remove from DLQ stream (entry has been replayed)
    await redis.xdel(RedisKeys.deadLetterStream(), entryId);

    console.log(fmt.ok(`  Replayed: ${taskId} (entry ${entryId})`));
    replayed++;
  }

  console.log(`\n  ${fmt.ok(`${replayed}/${toReplay.length} task(s) replayed successfully.`)}`);
}

async function cmdStatus(): Promise<void> {
  const db    = getDb();
  const redis = getRedisClient();

  console.log(fmt.header("Cluster Status"));

  // Ping both stores
  const [pgResult, redisResult] = await Promise.allSettled([
    db.$queryRaw`SELECT 1 AS ok`,
    redis.ping(),
  ]);

  console.log(fmt.label("PostgreSQL", pgResult.status    === "fulfilled" ? fmt.ok("ok") : fmt.error("ERROR")));
  console.log(fmt.label("Redis",      redisResult.status === "fulfilled" ? fmt.ok("ok") : fmt.error("ERROR")));

  // Worker count
  const workerCount = await db.worker.count({
    where: { lastHeartbeatAt: { gt: new Date(Date.now() - 60_000) } },
  });
  console.log(fmt.label("Active workers (heartbeat =60 s)", workerCount));

  // Queue depths
  const [high, medium, low, delayed, dlq] = await Promise.all([
    redis.xlen(RedisKeys.priorityStream(TaskPriority.HIGH)),
    redis.xlen(RedisKeys.priorityStream(TaskPriority.MEDIUM)),
    redis.xlen(RedisKeys.priorityStream(TaskPriority.LOW)),
    redis.zcard(RedisKeys.delayedQueue()),
    redis.xlen(RedisKeys.deadLetterStream()),
  ]);

  console.log("\n  Stream Queue Depths:");
  console.log(fmt.label("    HIGH",   high));
  console.log(fmt.label("    MEDIUM", medium));
  console.log(fmt.label("    LOW",    low));
  console.log(fmt.label("    Delayed ZSET", delayed));
  console.log(fmt.label("    Dead-Letter", dlq > 0 ? fmt.warn(String(dlq)) : String(dlq)));

  // Task counts by status
  const taskCounts = await db.task.groupBy({ by: ["status"], _count: { id: true } });
  console.log("\n  Task Status Breakdown:");
  for (const row of taskCounts) {
    const count = row._count.id;
    const label = row.status === TaskStatus.DEAD_LETTER
      ? fmt.error(row.status)
      : row.status === TaskStatus.FAILED || row.status === TaskStatus.RETRYING
        ? fmt.warn(row.status)
        : row.status === TaskStatus.COMPLETED
          ? fmt.ok(row.status)
          : row.status;
    console.log(fmt.label(`    ${label}`, count));
  }

  // Consumer group PEL summary
  console.log("\n  Consumer Group PEL (pending unacked messages):");
  const streamKeys = [
    RedisKeys.priorityStream(TaskPriority.HIGH),
    RedisKeys.priorityStream(TaskPriority.MEDIUM),
    RedisKeys.priorityStream(TaskPriority.LOW),
  ];

  for (const key of streamKeys) {
    try {
      // Summary form (no start/end/count range) returns [totalPending, minId, maxId, perConsumer].
      // The previous version used the range form with COUNT 1, which caps the returned
      // array at 1 entry regardless of how many are actually pending — so it could only
      // ever report "0" or "1", never the real backlog size.
      const summary = await redis.xpending(key, STREAM_CONSUMER_GROUP) as [number, string | null, string | null, unknown];
      const label = key.split(":").pop() ?? key;
      const count = summary[0];
      console.log(fmt.label(`    ${label}`, count > 0 ? fmt.warn(String(count)) : String(count)));
    } catch {
      // stream may not exist yet
    }
  }
}

async function cmdTasksList(filterStatus?: string): Promise<void> {
  const db = getDb();
  console.log(fmt.header(`Recent Tasks${filterStatus !== undefined ? ` (status: ${filterStatus})` : ""}`));

  const tasks = await db.task.findMany({
    ...(filterStatus !== undefined ? { where: { status: filterStatus as TaskStatus } } : {}),
    orderBy: { createdAt: "desc" },
    take:    25,
    select: {
      id:          true,
      type:        true,
      status:      true,
      priority:    true,
      retryCount:  true,
      maxRetries:  true,
      scheduledAt: true,
      createdAt:   true,
    },
  });

  if (tasks.length === 0) {
    console.log("  No tasks found.");
    return;
  }

  for (const t of tasks) {
    const statusStr =
      t.status === TaskStatus.COMPLETED   ? fmt.ok(t.status)   :
      t.status === TaskStatus.DEAD_LETTER ? fmt.error(t.status) :
      t.status === TaskStatus.FAILED      ? fmt.error(t.status) :
      t.status === TaskStatus.RETRYING    ? fmt.warn(t.status)  :
      t.status;

    console.log(`  ${fmt.dim(t.id)}  ${t.type.padEnd(24)} ${statusStr.padEnd(16)} ${t.priority.padEnd(8)} retries:${t.retryCount}/${t.maxRetries}`);
  }
}

function printHelp(): void {
  console.log(fmt.header("Distributed Task Scheduler � Admin CLI"));
  console.log(`
  ${C.bold}Commands:${C.reset}
    dlq:inspect                  List all dead-letter queue entries
    dlq:replay                   Replay all DLQ entries
    dlq:replay --id <streamId>   Replay a specific DLQ entry
    status                       Live cluster status (queues, workers, DB)
    tasks:list                   List 25 most recent tasks
    tasks:list --status <STATUS> Filter by status (PENDING|RUNNING|COMPLETED|...)

  ${C.bold}Examples:${C.reset}
    npx ts-node src/cli/admin.ts status
    npx ts-node src/cli/admin.ts dlq:inspect
    npx ts-node src/cli/admin.ts dlq:replay --id 1700000000000-0
    npx ts-node src/cli/admin.ts tasks:list --status DEAD_LETTER
`);
}

// -----------------------------------------------------------------------------
// Entry point
// -----------------------------------------------------------------------------

async function main(): Promise<void> {
  const { command, flags } = parseArgs();

  try {
    switch (command) {
      case "dlq:inspect":
        await cmdDlqInspect();
        break;

      case "dlq:replay":
        await cmdDlqReplay(flags["id"]);
        break;

      case "status":
        await cmdStatus();
        break;

      case "tasks:list":
        await cmdTasksList(flags["status"]);
        break;

      case "help":
      default:
        printHelp();
    }
  } finally {
    await disconnectDb();
    await closeRedisClient();
  }
}

main().catch((err: unknown) => {
  console.error(fmt.error(`\nFatal: ${(err as Error).message}`));
  process.exit(1);
});
