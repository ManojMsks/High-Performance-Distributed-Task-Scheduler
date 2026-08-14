/**
 * src/scheduler.ts - Scheduler process entry point.
 * Run with: npm run scheduler
 *
 * NOTE: This process owns delayed-task promotion ONLY. Stale-lock / orphaned
 * task recovery is a separate concern owned by src/recovery-service.ts
 * (npm run recovery, or the `recovery` container in docker-compose). This
 * file previously started its own RecoveryService in addition to that
 * dedicated process, so running both side by side (as docker-compose does)
 * meant two independent recovery sweepers running concurrently against the
 * same Postgres/Redis state on every RECOVERY_POLL_INTERVAL_MS tick -
 * harmless (the recovery logic is idempotent) but pure wasted load and a
 * confusing footgun if the recovery timing/logic ever changes in one place
 * and not the other.
 */
import "dotenv/config";
import { SchedulerService } from "./core/scheduler";
import { logger }           from "./infra/logger";
import { disconnectDb }     from "./infra/db";
import { closeRedisClient } from "./infra/redis/redis-client";

const scheduler = new SchedulerService();

async function main(): Promise<void> {
  await scheduler.start();
}

async function shutdown(signal: string): Promise<void> {
  logger.info(`[Scheduler Process] ${signal} - shutting down`);
  await scheduler.stop();
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
