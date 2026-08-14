/**
 * src/config/index.ts
 *
 * Centralised, validated runtime configuration.
 * All env vars are parsed here — fail fast on startup if anything is missing.
 */

import "dotenv/config";
import { z } from "zod";

const envSchema = z.object({
  NODE_ENV: z
    .enum(["development", "production", "test"])
    .default("development"),
  LOG_LEVEL: z
    .enum(["error", "warn", "info", "http", "verbose", "debug", "silly"])
    .default("debug"),
  LOG_DIR: z.string().optional(),
  PORT: z.coerce.number().int().positive().default(3000),

  // Database
  DATABASE_URL: z.string().url(),

  // Redis
  REDIS_HOST: z.string().default("127.0.0.1"),
  REDIS_PORT: z.coerce.number().int().positive().default(6379),
  REDIS_PASSWORD: z.string().optional(),
  REDIS_DB: z.coerce.number().int().nonnegative().default(0),

  // Scheduler
  SCHEDULER_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(1_000),
  SCHEDULER_BATCH_SIZE: z.coerce.number().int().positive().default(100),
  RECOVERY_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(30_000),
  STALE_LOCK_THRESHOLD_SECONDS: z.coerce.number().int().positive().default(90),

  // Worker
  WORKER_CONCURRENCY: z.coerce.number().int().positive().default(5),
  WORKER_BLOCK_TIMEOUT_MS: z.coerce.number().int().nonnegative().default(5_000),
  WORKER_HEARTBEAT_INTERVAL_MS: z.coerce.number().int().positive().default(10_000),
});

function parseConfig() {
  const result = envSchema.safeParse(process.env);
  if (!result.success) {
    console.error(
      "[Config] Invalid environment variables:\n",
      result.error.format(),
    );
    process.exit(1);
  }
  return result.data;
}

export const config = parseConfig();
export type Config = typeof config;
