/**
 * src/index.ts
 * API Server + WebSocket Gateway process entry point.
 * Run:  npm run dev   (development)
 *       npm run start (production)
 */
import "dotenv/config";
import { createServer }                            from "http";
import { createApp }                               from "./api/server";
import { attachWebSocket }                         from "./api/websocket";
import { config }                                  from "./config";
import { logger }                                  from "./infra/logger";
import { getDb, disconnectDb }                     from "./infra/db";
import { getRedisClient, closeRedisClient }        from "./infra/redis/redis-client";

async function bootstrap(): Promise<void> {
  // Eagerly validate connections before accepting traffic
  getDb();
  getRedisClient();

  const app        = createApp();
  const httpServer = createServer(app);

  // Attach real-time Socket.IO gateway
  attachWebSocket(httpServer);

  httpServer.listen(config.PORT, () => {
    logger.info(`[Server] Listening on :${config.PORT} (${config.NODE_ENV})`);
    logger.info(`[Server] Dashboard ? http://localhost:${config.PORT}/`);
    logger.info(`[Server] REST API  ? http://localhost:${config.PORT}/v1/tasks`);
  });
}

async function shutdown(signal: string): Promise<void> {
  logger.info(`[Server] ${signal} received — shutting down gracefully`);
  await disconnectDb();
  await closeRedisClient();
  process.exit(0);
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT",  () => void shutdown("SIGINT"));

bootstrap().catch((err: unknown) => {
  logger.error("[Server] Fatal startup error", { error: err });
  process.exit(1);
});
