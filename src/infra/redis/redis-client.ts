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
 * Gracefully closes the Redis connection.
 * Call during process shutdown (SIGTERM / SIGINT handlers).
 */
export async function closeRedisClient(): Promise<void> {
  if (_client === null) return;
  await _client.quit();
  _client = null;
  logger.info("[Redis] Connection closed gracefully");
}
