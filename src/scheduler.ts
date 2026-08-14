/**
 * src/scheduler.ts — Scheduler process entry point.
 * Run with: npm run scheduler
 */
import "dotenv/config";
import { SchedulerService } from "./core/scheduler";
import { RecoveryService }  from "./core/recovery";
import { logger }           from "./infra/logger";
import { disconnectDb }     from "./infra/db";
import { closeRedisClient } from "./infra/redis/redis-client";

const scheduler = new SchedulerService();
const recovery  = new RecoveryService();

async function main(): Promise<void> {
  await Promise.all([scheduler.start(), recovery.start()]);
}

async function shutdown(signal: string): Promise<void> {
  logger.info(`[Scheduler Process] ${signal} — shutting down`);
  await Promise.all([scheduler.stop(), recovery.stop()]);
  await disconnectDb();
  await closeRedisClient();
  process.exit(0);
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT",  () => void shutdown("SIGINT"));

main().catch((err: unknown) => {
  logger.error("[Scheduler Process] Fatal error", { error: err });
  process.exit(1);
});
