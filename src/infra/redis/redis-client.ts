/**
 * src/infra/redis/redis-client.ts
 *
 * Singleton ioredis client with:
 *  - Retry strategy with exponential back-off and jitter
 *  - Connection event logging
 *  - Graceful shutdown hook
 */

import Redis, { type RedisOptions } from "ioredis";
import { logger } from "../logger";

const MAX_RETRY_ATTEMPTS = 20;
const BASE_RETRY_DELAY_MS = 100;
const MAX_RETRY_DELAY_MS = 10_000;

function buildRetryStrategy(
  attempt: number,
): number | null {
  if (attempt > MAX_RETRY_ATTEMPTS) {
    logger.error(
      `[Redis] Exceeded ${MAX_RETRY_ATTEMPTS} reconnect attempts. Giving up.`,
    );
    return null; // stops retrying; ioredis emits "end" event
  }

  // Exponential back-off with full jitter
  const exp = Math.min(BASE_RETRY_DELAY_MS * 2 ** attempt, MAX_RETRY_DELAY_MS);
  const jitter = Math.floor(Math.random() * exp * 0.2);
  const delay = exp + jitter;

  logger.warn(`[Redis] Reconnecting in ${delay}ms (attempt ${attempt})`);
  return delay;
}

const redisOptions: RedisOptions = {
  host: process.env["REDIS_HOST"] ?? "127.0.0.1",
  port: Number(process.env["REDIS_PORT"] ?? 6379),
  password: process.env["REDIS_PASSWORD"] ?? undefined,
  db: Number(process.env["REDIS_DB"] ?? 0),
  retryStrategy: buildRetryStrategy,
  enableReadyCheck: true,
  maxRetriesPerRequest: 3,
  lazyConnect: false,
  // Keep-alive so TCP connections don't silently drop behind NAT
  keepAlive: 10_000,
  connectTimeout: 10_000,
  commandTimeout: 5_000,
};

let _client: Redis | null = null;
let _blockingClient: Redis | null = null;

/**
 * Returns the shared ioredis client, creating it on first call.
 * Registers event handlers once per process lifecycle.
 */
export function getRedisClient(): Redis {
  if (_client !== null) return _client;

  _client = new Redis(redisOptions);

  _client.on("connect", () => logger.info("[Redis] TCP connection established"));
  _client.on("ready", () => logger.info("[Redis] Client ready"));
  _client.on("error", (err: Error) =>
    logger.error("[Redis] Error", { error: err.message }),
  );
  _client.on("close", () => logger.warn("[Redis] Connection closed"));
  _client.on("reconnecting", () => logger.warn("[Redis] Reconnecting�"));
  _client.on("end", () =>
    logger.error("[Redis] Connection ended permanently"),
  );

  return _client;
}

/**
 * Returns the shared blocking ioredis client (for xreadgroup etc.),
 * creating it on first call.
 */
export function getBlockingRedisClient(): Redis {
  if (_blockingClient !== null) return _blockingClient;

  _blockingClient = new Redis({
    ...redisOptions,
    commandTimeout: 0,
    maxRetriesPerRequest: null,
  });

  _blockingClient.on("connect", () => logger.info("[Blocking Redis] TCP connection established"));
  _blockingClient.on("ready", () => logger.info("[Blocking Redis] Client ready"));
  _blockingClient.on("error", (err: Error) =>
    logger.error("[Blocking Redis] Error", { error: err.message }),
  );
  _blockingClient.on("close", () => logger.warn("[Blocking Redis] Connection closed"));
  _blockingClient.on("reconnecting", () => logger.warn("[Blocking Redis] Reconnecting…"));
  _blockingClient.on("end", () =>
    logger.error("[Blocking Redis] Connection ended permanently"),
  );

  return _blockingClient;
}

/**
 * Gracefully closes the Redis connection.
 * Call during process shutdown (SIGTERM / SIGINT handlers).
 */
export async function closeRedisClient(): Promise<void> {
  const promises = [];
  if (_client !== null) promises.push(_client.quit());
  if (_blockingClient !== null) promises.push(_blockingClient.quit());
  
  await Promise.all(promises);
  _client = null;
  _blockingClient = null;
  
  logger.info("[Redis] Connections closed gracefully");
}
