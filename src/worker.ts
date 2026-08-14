/**
 * src/worker.ts — Worker process entry point.
 * Run with: npm run worker
 *
 * Register your task handlers here before calling start().
 */
import "dotenv/config";
import { WorkerService }     from "./core/worker";
import { registerAllHandlers } from "./handlers";
import { logger }            from "./infra/logger";
import { disconnectDb }      from "./infra/db";
import { closeRedisClient }  from "./infra/redis/redis-client";

const worker = new WorkerService();

// -- Register task handlers ---------------------------------------------------
// Wired to the real handler implementations in src/handlers/index.ts
// (email:send, image:resize, report:generate). Previously this file defined
// its own bare-bones inline stubs and never registered "report:generate" at
// all â€” any task of that type would fail with "No handler registered" and
// go straight to retry/DLQ. registerAllHandlers() keeps the two in sync.
registerAllHandlers(worker);

// -- Lifecycle ----------------------------------------------------------------

async function shutdown(signal: string): Promise<void> {
  logger.info(`[Worker Process] ${signal} — shutting down`);
  await worker.stop();
  await disconnectDb();
  await closeRedisClient();
  process.exit(0);
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT",  () => void shutdown("SIGINT"));

worker.start().catch((err: unknown) => {
  logger.error("[Worker Process] Fatal error", { error: err });
  process.exit(1);
});
