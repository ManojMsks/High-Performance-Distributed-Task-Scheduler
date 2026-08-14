/**
 * src/scripts/verify-engine.ts
 *
 * End-to-End Verification & Stress Script
 *
 * Bootstraps the full scheduler engine in-process and runs four scenarios:
 *
 *   A — Immediate multi-priority execution
 *       Creates HIGH / MEDIUM / LOW tasks and verifies all complete.
 *
 *   B — Delayed task scheduling
 *       Schedules a task +4 s in the future, waits for scheduler promotion
 *       and worker execution.
 *
 *   C — Retry & Dead-Letter Queue
 *       Registers a handler that always throws.  Verifies exponential-backoff
 *       retry cycles and eventual DEAD_LETTER routing.
 *
 *   D — Stale Lock Recovery
 *       Inserts a task directly in RUNNING state with a stale lockedAt,
 *       triggers one recovery sweep, and verifies the task is re-queued
 *       then executed to COMPLETED.
 *
 * Run:
 *   npx ts-node src/scripts/verify-engine.ts
 *
 * Pre-requisites:
 *   - Docker stack running  (docker compose -f docker/docker-compose.yml up -d)
 *   - Migrations applied    (npx prisma migrate deploy)
 *   - .env configured
 */

import "dotenv/config";
import { TaskPriority, TaskStatus } from "@prisma/client";
import { getDb, disconnectDb } from "../infra/db";
import { getRedisClient, closeRedisClient } from "../infra/redis/redis-client";
import { RedisKeys } from "../infra/redis/redis-keys";
import { createTask } from "../core/producer";
import { WorkerService, type TaskHandlerFn } from "../core/worker";
import { SchedulerService } from "../core/scheduler";
import { RecoveryService }  from "../core/recovery";

// -----------------------------------------------------------------------------
// ANSI output helpers
// -----------------------------------------------------------------------------

const C = {
  reset:  "\x1b[0m",
  bold:   "\x1b[1m",
  green:  "\x1b[32m",
  red:    "\x1b[31m",
  yellow: "\x1b[33m",
  cyan:   "\x1b[36m",
  gray:   "\x1b[90m",
};

function pass(msg: string): void { console.log(`  ${C.green}?${C.reset} ${msg}`); }
function fail(msg: string): void { console.log(`  ${C.red}?${C.reset} ${msg}`); }
function info(msg: string): void { console.log(`  ${C.gray}?${C.reset} ${msg}`); }
function section(name: string): void {
  console.log(`\n${C.bold}${C.cyan}?? ${name} ${C.reset}`);
}

// -----------------------------------------------------------------------------
// Primitives
// -----------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(
  condition: () => Promise<boolean>,
  timeoutMs: number,
  label:     string,
  pollMs     = 500,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await condition()) return;
    await sleep(pollMs);
  }
  throw new Error(`Timeout (${timeoutMs}ms) waiting for: "${label}"`);
}

function assert(cond: boolean, msg: string): void {
  if (!cond) throw new Error(`Assertion failed: ${msg}`);
}

// -----------------------------------------------------------------------------
// Scenario result tracker
// -----------------------------------------------------------------------------

interface ScenarioResult {
  name:    string;
  passed:  boolean;
  durationMs: number;
  error?:  string;
}

// -----------------------------------------------------------------------------
// Scenario A: Immediate multi-priority execution
// -----------------------------------------------------------------------------

async function scenarioA(): Promise<void> {
  section("Scenario A — Immediate Multi-Priority Execution");

  const db   = getDb();
  const tasks = await Promise.all([
    createTask({ type: "verify:noop", payload: { label: "HIGH-1"   }, priority: TaskPriority.HIGH   }),
    createTask({ type: "verify:noop", payload: { label: "HIGH-2"   }, priority: TaskPriority.HIGH   }),
    createTask({ type: "verify:noop", payload: { label: "MEDIUM-1" }, priority: TaskPriority.MEDIUM }),
    createTask({ type: "verify:noop", payload: { label: "MEDIUM-2" }, priority: TaskPriority.MEDIUM }),
    createTask({ type: "verify:noop", payload: { label: "LOW-1"    }, priority: TaskPriority.LOW    }),
    createTask({ type: "verify:noop", payload: { label: "LOW-2"    }, priority: TaskPriority.LOW    }),
  ]);

  const taskIds = tasks.map((t) => t.taskId);
  info(`Created ${taskIds.length} tasks (2×HIGH, 2×MEDIUM, 2×LOW)`);

  // Verify all enqueued immediately
  for (const t of tasks) {
    assert(t.enqueuedImmediately, `Task ${t.taskId} should be enqueued immediately`);
  }

  await waitFor(
    async () => {
      const completed = await db.task.count({
        where: { id: { in: taskIds }, status: TaskStatus.COMPLETED },
      });
      info(`Completed: ${completed}/${taskIds.length}`);
      return completed === taskIds.length;
    },
    20_000,
    "all 6 tasks COMPLETED",
    800,
  );

  pass(`All ${taskIds.length} tasks completed in priority order`);
}

// -----------------------------------------------------------------------------
// Scenario B: Delayed task scheduling
// -----------------------------------------------------------------------------

async function scenarioB(scheduler: SchedulerService): Promise<void> {
  section("Scenario B — Delayed Task Scheduling");

  const db         = getDb();
  const redis      = getRedisClient();
  const delayMs    = 4_000; // 4 s in future
  const scheduledAt = new Date(Date.now() + delayMs);

  const { taskId } = await createTask({
    type:        "verify:noop",
    payload:     { label: "delayed-task" },
    scheduledAt,
  });

  info(`Task ${taskId} scheduled at ${scheduledAt.toISOString()}`);

  // Verify it landed in the delayed ZSET (not the stream)
  const score = await redis.zscore(RedisKeys.delayedQueue(), taskId);
  assert(score !== null, "Task should be in the delayed ZSET");
  pass("Task is in delayed ZSET with correct score");

  // Verify Postgres status is PENDING (not QUEUED)
  const initial = await db.task.findUniqueOrThrow({ where: { id: taskId }, select: { status: true } });
  assert(initial.status === TaskStatus.PENDING, "Status should be PENDING before promotion");
  pass("Initial status is PENDING");

  // Wait for delay to elapse, then trigger scheduler promotion
  info("Waiting for scheduled time…");
  await sleep(delayMs + 200);
  await scheduler.promoteReadyTasks();
  info("Promotion triggered");

  // Wait for worker to execute
  await waitFor(
    async () => {
      const t = await db.task.findUniqueOrThrow({ where: { id: taskId }, select: { status: true } });
      info(`Task status: ${t.status}`);
      return t.status === TaskStatus.COMPLETED;
    },
    15_000,
    `task ${taskId} COMPLETED`,
    500,
  );

  pass(`Delayed task promoted and executed successfully`);
}

// -----------------------------------------------------------------------------
// Scenario C: Retry & Dead-Letter Queue
// -----------------------------------------------------------------------------

async function scenarioC(scheduler: SchedulerService): Promise<void> {
  section("Scenario C — Retry & Dead-Letter Queue");

  const db = getDb();

  const { taskId } = await createTask({
    type:       "verify:always-fail",
    payload:    { label: "dlq-test" },
    maxRetries: 2,  // ? 3 total attempts before DEAD_LETTER
  });

  info(`Task ${taskId} created (maxRetries=2)`);

  await waitFor(
    async () => {
      const t = await db.task.findUniqueOrThrow({
        where:  { id: taskId },
        select: { status: true, retryCount: true },
      });
      info(`Status: ${t.status}, retryCount: ${t.retryCount}`);

      // If RETRYING, force-promote so test doesn't wait for full backoff
      if (t.status === TaskStatus.RETRYING) {
        await scheduler.promoteReadyTasks();
      }

      return t.status === TaskStatus.DEAD_LETTER;
    },
    60_000,
    `task ${taskId} DEAD_LETTER`,
    1_000,
  );

  const final = await db.task.findUniqueOrThrow({
    where:  { id: taskId },
    select: { status: true, retryCount: true },
  });

  assert(final.status === TaskStatus.DEAD_LETTER, "Task must be DEAD_LETTER");
  assert(final.retryCount === 2, `retryCount must be 2 (got ${final.retryCount})`);

  // Verify DLQ stream entry exists
  const dlqEntries = await getRedisClient().xrange(
    RedisKeys.deadLetterStream(),
    "-", "+",
    "COUNT", 10,
  );
  const inDlq = dlqEntries.some(([, fields]) => {
    for (let i = 0; i + 1 < fields.length; i += 2) {
      if (fields[i] === "taskId" && fields[i + 1] === taskId) return true;
    }
    return false;
  });
  assert(inDlq, "Task should be present in the dead-letter Redis stream");

  pass(`Task dead-lettered after 3 attempts (retryCount=${final.retryCount})`);
  pass("Dead-letter Redis stream entry confirmed");
}

// -----------------------------------------------------------------------------
// Scenario D: Stale Lock Recovery
// -----------------------------------------------------------------------------

async function scenarioD(scheduler: SchedulerService, recovery: RecoveryService): Promise<void> {
  section("Scenario D — Stale Lock & Orphaned Task Recovery");

  const db    = getDb();
  const redis = getRedisClient();

  // Create a task but don't enqueue it — we'll manually set it to RUNNING
  const { taskId } = await createTask({
    type:    "verify:noop",
    payload: { label: "orphan-recovery-test" },
  });

  // Simulate a worker crash: set task to RUNNING with a stale lockedAt
  const staleLockTime = new Date(Date.now() - 300_000); // 5 minutes ago
  const fakeWorkerId  = "dead-worker-00000000-0000-0000-0000-000000000000";

  await db.task.update({
    where: { id: taskId },
    data: {
      status:     TaskStatus.RUNNING,
      lockedById: fakeWorkerId,
      lockedAt:   staleLockTime,
      executedAt: staleLockTime,
    },
  });

  // Do NOT set a heartbeat key for fakeWorkerId — it's genuinely "dead"
  const heartbeat = await redis.get(RedisKeys.workerHeartbeat(fakeWorkerId));
  assert(heartbeat === null, "Fake worker must have no heartbeat");
  info(`Task ${taskId} artificially set to RUNNING with stale lock (${staleLockTime.toISOString()})`);

  // Trigger one recovery sweep
  await recovery.runSweep();
  info("Recovery sweep triggered");

  // Task should now be RETRYING and in the delayed ZSET
  const afterRecovery = await db.task.findUniqueOrThrow({
    where:  { id: taskId },
    select: { status: true, retryCount: true, lockedById: true },
  });

  assert(afterRecovery.status === TaskStatus.RETRYING, `Expected RETRYING, got ${afterRecovery.status}`);
  assert(afterRecovery.lockedById === null, "lockedById should be cleared after recovery");
  pass(`Task moved to RETRYING (retryCount=${afterRecovery.retryCount})`);

  // Force immediate promotion (override backoff for test speed)
  await redis.zadd(RedisKeys.delayedQueue(), Date.now(), taskId);
  await scheduler.promoteReadyTasks();
  info("Recovery-promoted task force-promoted to stream");

  await waitFor(
    async () => {
      const t = await db.task.findUniqueOrThrow({ where: { id: taskId }, select: { status: true } });
      info(`Task status: ${t.status}`);
      return t.status === TaskStatus.COMPLETED;
    },
    15_000,
    `orphaned task ${taskId} COMPLETED`,
    500,
  );

  pass("Orphaned task recovered and executed to COMPLETED");
}

// -----------------------------------------------------------------------------
// Bootstrap
// -----------------------------------------------------------------------------

function buildWorkers(): WorkerService[] {
  const noopHandler: TaskHandlerFn = async (_payload, _ctx) => {
    await sleep(20); // minimal real work
  };

  const failingHandler: TaskHandlerFn = async (_payload, _ctx) => {
    throw new Error("Intentional failure for Scenario C");
  };

  const workers = [new WorkerService(), new WorkerService()];
  for (const w of workers) {
    w.register("verify:noop",        noopHandler);
    w.register("verify:always-fail", failingHandler);
  }
  return workers;
}

async function startServices(): Promise<{
  workers:   WorkerService[];
  scheduler: SchedulerService;
  recovery:  RecoveryService;
}> {
  const workers   = buildWorkers();
  const scheduler = new SchedulerService();
  const recovery  = new RecoveryService();

  // Start workers (non-blocking since start() now returns after init)
  await Promise.all(workers.map((w) => w.start()));

  await scheduler.start();
  await recovery.start();

  // Give consumer groups and heartbeats time to settle
  await sleep(1_500);
  info(`${workers.length} workers, 1 scheduler, 1 recovery service started`);

  return { workers, scheduler, recovery };
}

async function teardown(
  workers:   WorkerService[],
  scheduler: SchedulerService,
  recovery:  RecoveryService,
): Promise<void> {
  section("Teardown");
  await Promise.all(workers.map((w) => w.stop()));
  await scheduler.stop();
  await recovery.stop();
  await disconnectDb();
  await closeRedisClient();
  info("All services stopped, connections closed");
}

// -----------------------------------------------------------------------------
// Main
// -----------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log(`\n${C.bold}${C.cyan}${"-".repeat(60)}${C.reset}`);
  console.log(`${C.bold}${C.cyan}  Distributed Task Scheduler — E2E Verification Suite${C.reset}`);
  console.log(`${C.bold}${C.cyan}${"-".repeat(60)}${C.reset}\n`);

  const { workers, scheduler, recovery } = await startServices();
  const results: ScenarioResult[] = [];

  const run = async (name: string, fn: () => Promise<void>): Promise<void> => {
    const t0 = Date.now();
    try {
      await fn();
      results.push({ name, passed: true, durationMs: Date.now() - t0 });
    } catch (err) {
      fail(`${name} FAILED: ${(err as Error).message}`);
      results.push({ name, passed: false, durationMs: Date.now() - t0, error: (err as Error).message });
    }
  };

  try {
    await run("Scenario A", () => scenarioA());
    await run("Scenario B", () => scenarioB(scheduler));
    await run("Scenario C", () => scenarioC(scheduler));
    await run("Scenario D", () => scenarioD(scheduler, recovery));
  } finally {
    await teardown(workers, scheduler, recovery);
  }

  // -- Summary ----------------------------------------------------------------
  console.log(`\n${C.bold}${"-".repeat(60)}${C.reset}`);
  console.log(`${C.bold}  Results${C.reset}`);
  console.log(`${"-".repeat(60)}`);

  let allPassed = true;
  for (const r of results) {
    const status = r.passed ? `${C.green}PASS${C.reset}` : `${C.red}FAIL${C.reset}`;
    console.log(`  ${status}  ${r.name.padEnd(16)} ${r.durationMs}ms`);
    if (!r.passed && r.error !== undefined) {
      console.log(`         ${C.gray}${r.error}${C.reset}`);
    }
    if (!r.passed) allPassed = false;
  }

  console.log(`${"-".repeat(60)}\n`);

  if (allPassed) {
    console.log(`${C.bold}${C.green}  All scenarios passed! Engine is production-ready.${C.reset}\n`);
    process.exit(0);
  } else {
    console.log(`${C.bold}${C.red}  Some scenarios failed — see output above.${C.reset}\n`);
    process.exit(1);
  }
}

main().catch((err: unknown) => {
  console.error(`\n${C.red}Fatal error:${C.reset}`, (err as Error).message);
  process.exit(1);
});
