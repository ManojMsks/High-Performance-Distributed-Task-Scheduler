/**
 * src/worker.ts — Worker process entry point.
 * Run with: npm run worker
 *
 * Register your task handlers here before calling start().
 */
import "dotenv/config";
import { WorkerService }    from "./core/worker";
import { logger }           from "./infra/logger";
import { disconnectDb }     from "./infra/db";
import { closeRedisClient } from "./infra/redis/redis-client";

const worker = new WorkerService();

// -- Register task handlers ---------------------------------------------------

worker.register("email:send", async (payload, ctx) => {
  logger.info("[Handler] email:send", { taskId: ctx.taskId, to: payload["to"] });
  // TODO: integrate email provider (SendGrid, SES, etc.)
  await new Promise<void>((resolve) => setTimeout(resolve, 100));
});

worker.register("image:resize", async (payload, ctx) => {
  logger.info("[Handler] image:resize", { taskId: ctx.taskId, url: payload["url"] });
  // TODO: integrate image processing (sharp, ffmpeg, etc.)
  await new Promise<void>((resolve) => setTimeout(resolve, 200));
});

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
