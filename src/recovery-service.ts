/**
 * src/recovery-service.ts
 * Standalone Recovery Service process entry point.
 * Run with: npm run recovery  (or node dist/recovery-service.js in Docker)
 */
import "dotenv/config";
import { RecoveryService } from "./core/recovery";
import { logger }          from "./infra/logger";
import { disconnectDb }    from "./infra/db";
import { closeRedisClient } from "./infra/redis/redis-client";

const recovery = new RecoveryService();

async function shutdown(signal: string): Promise<void> {
  logger.info(`[Recovery Service] ${signal} — shutting down`);
  await recovery.stop();
  await disconnectDb();
  await closeRedisClient();
  process.exit(0);
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT",  () => void shutdown("SIGINT"));

recovery.start().catch((err: unknown) => {
  logger.error("[Recovery Service] Fatal error", { error: err });
  process.exit(1);
});
