/**
 * src/infra/db.ts
 *
 * Prisma Client singleton with connection pooling.
 *
 * Prisma v5 narrows $on() event types from the literal log config passed to
 * the constructor. A runtime-computed log array loses those literals and
 * collapses to `never`. We therefore create two typed clients — one for
 * development (all events) and one for production (warn + error only) — so
 * TypeScript can infer the correct overloads for $on().
 */

import { PrismaClient, Prisma } from "@prisma/client";
import { logger } from "./logger";

// --- Dev client (query + info + warn + error emitted as events) -------------

type DevClient = PrismaClient<{
  log: [
    { level: "query"; emit: "event" },
    { level: "info";  emit: "event" },
    { level: "warn";  emit: "event" },
    { level: "error"; emit: "event" },
  ];
}>;

function createDevClient(): DevClient {
  const client = new PrismaClient({
    log: [
      { level: "query", emit: "event" },
      { level: "info",  emit: "event" },
      { level: "warn",  emit: "event" },
      { level: "error", emit: "event" },
    ],
    errorFormat: "pretty",
  }) as DevClient;

  client.$on("warn",  (e: Prisma.LogEvent) => logger.warn("[Prisma]",  { message: e.message }));
  client.$on("error", (e: Prisma.LogEvent) => logger.error("[Prisma]", { message: e.message }));
  client.$on("query", (e: Prisma.QueryEvent) =>
    logger.debug("[Prisma Query]", {
      query:    e.query,
      params:   e.params,
      duration: `${e.duration}ms`,
    }),
  );

  return client;
}

// --- Prod client (warn + error only) ----------------------------------------

type ProdClient = PrismaClient<{
  log: [
    { level: "warn";  emit: "event" },
    { level: "error"; emit: "event" },
  ];
}>;

function createProdClient(): ProdClient {
  const client = new PrismaClient({
    log: [
      { level: "warn",  emit: "event" },
      { level: "error", emit: "event" },
    ],
    errorFormat: "minimal",
  }) as ProdClient;

  client.$on("warn",  (e: Prisma.LogEvent) => logger.warn("[Prisma]",  { message: e.message }));
  client.$on("error", (e: Prisma.LogEvent) => logger.error("[Prisma]", { message: e.message }));

  return client;
}

// --- Singleton ---------------------------------------------------------------

let _prisma: PrismaClient | null = null;

export function getDb(): PrismaClient {
  if (_prisma !== null) return _prisma;

  _prisma =
    process.env["NODE_ENV"] === "production"
      ? createProdClient()
      : createDevClient();

  return _prisma;
}

export async function disconnectDb(): Promise<void> {
  if (_prisma === null) return;
  await _prisma.$disconnect();
  _prisma = null;
  logger.info("[Prisma] Disconnected");
}
