/**
 * src/handlers/index.ts
 *
 * Concrete, production-ready sample task handlers.
 *
 * Each handler is a pure function matching the TaskHandlerFn signature.
 * registerAllHandlers() wires them all to a WorkerService instance.
 *
 * Design:
 *  - Configurable mock delay + failure rate via env vars or payload overrides
 *  - Progress logging via ctx.renewLock() on long-running handlers
 *  - Strongly typed payload extraction with safe defaults
 */

import { type TaskHandlerFn, type WorkerService } from "../core/worker";
import { logger } from "../infra/logger";

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function safeString(value: unknown, fallback: string): string {
  return typeof value === "string" && value.length > 0 ? value : fallback;
}

function safeNumber(value: unknown, fallback: number): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

// -----------------------------------------------------------------------------
// email:send
// -----------------------------------------------------------------------------

/**
 * Simulates transactional email delivery.
 *
 * Expected payload keys:
 *   to          {string}   — recipient address (default: user@example.com)
 *   subject     {string}   — email subject
 *   body        {string}   — plain-text body
 *   simulateFailureRate {number} 0–1 — probability of failure (default 0)
 *   delayMs     {number}   — simulated send latency (default 80)
 */
export const emailSendHandler: TaskHandlerFn = async (payload, ctx) => {
  const to          = safeString(payload["to"],      "user@example.com");
  const subject     = safeString(payload["subject"], "Notification");
  const delayMs     = safeNumber(payload["delayMs"], 80);
  const failureRate = safeNumber(payload["simulateFailureRate"], 0);

  logger.info("[Handler:email:send] Sending email", {
    taskId:  ctx.taskId,
    attempt: ctx.attemptNumber,
    to,
    subject,
  });

  // Simulate network latency
  await sleep(delayMs);

  // Simulated failure injection
  if (Math.random() < failureRate) {
    throw new Error(`[email:send] Simulated SMTP delivery failure to "${to}"`);
  }

  logger.info("[Handler:email:send] Email delivered", { taskId: ctx.taskId, to });
};

// -----------------------------------------------------------------------------
// image:resize
// -----------------------------------------------------------------------------

/**
 * Simulates an image resizing pipeline (e.g., using sharp or ffmpeg).
 *
 * Expected payload keys:
 *   url        {string}  — source image URL
 *   width      {number}  — target width in px (default 800)
 *   height     {number}  — target height in px (default 600)
 *   quality    {number}  — compression quality 1–100 (default 85)
 *   delayMs    {number}  — simulated processing time per mpx (default 2)
 */
export const imageResizeHandler: TaskHandlerFn = async (payload, ctx) => {
  const url     = safeString(payload["url"],    "https://example.com/image.jpg");
  const width   = safeNumber(payload["width"],  800);
  const height  = safeNumber(payload["height"], 600);
  const quality = safeNumber(payload["quality"], 85);
  const msPerMpx = safeNumber(payload["delayMs"], 2);

  const megapixels = (width * height) / 1_000_000;
  const processingMs = Math.round(megapixels * msPerMpx * 100); // scale for realism

  logger.info("[Handler:image:resize] Starting", {
    taskId:  ctx.taskId,
    attempt: ctx.attemptNumber,
    url,
    width,
    height,
    quality,
    estimatedMs: processingMs,
  });

  // Simulate processing time, renewing lock every second for large images
  const chunkMs   = 1_000;
  let remaining   = processingMs;

  while (remaining > 0) {
    const step = Math.min(remaining, chunkMs);
    await sleep(step);
    remaining -= step;

    if (remaining > 0) {
      await ctx.renewLock();
      logger.debug("[Handler:image:resize] Processing…", {
        taskId:    ctx.taskId,
        remainingMs: remaining,
      });
    }
  }

  logger.info("[Handler:image:resize] Complete", {
    taskId: ctx.taskId,
    output: `resized_${width}x${height}_q${quality}.jpg`,
  });
};

// -----------------------------------------------------------------------------
// report:generate
// -----------------------------------------------------------------------------

/**
 * Simulates a heavy background report generation job.
 * Logs progress at each page and renews the lock to prevent stale-lock recovery
 * from treating it as orphaned.
 *
 * Expected payload keys:
 *   reportType  {string} — e.g. "monthly-sales", "user-activity" (default "summary")
 *   pages       {number} — number of report pages (default 5)
 *   msPerPage   {number} — simulated compute time per page (default 200)
 */
export const reportGenerateHandler: TaskHandlerFn = async (payload, ctx) => {
  const reportType = safeString(payload["reportType"], "summary");
  const pages      = Math.max(1, safeNumber(payload["pages"], 5));
  const msPerPage  = safeNumber(payload["msPerPage"], 200);

  logger.info("[Handler:report:generate] Starting report", {
    taskId:     ctx.taskId,
    attempt:    ctx.attemptNumber,
    reportType,
    pages,
  });

  for (let page = 1; page <= pages; page++) {
    await sleep(msPerPage);

    // Renew distributed lock after each page (keeps long reports alive)
    await ctx.renewLock();

    logger.info("[Handler:report:generate] Page rendered", {
      taskId:     ctx.taskId,
      reportType,
      page,
      totalPages: pages,
      progress:   `${Math.round((page / pages) * 100)}%`,
    });
  }

  logger.info("[Handler:report:generate] Report complete", {
    taskId: ctx.taskId,
    reportType,
    pages,
    outputPath: `/reports/${reportType}-${Date.now()}.pdf`,
  });
};

// -----------------------------------------------------------------------------
// Registration helper
// -----------------------------------------------------------------------------

/**
 * Registers all production handlers on the provided WorkerService.
 * Call this once before `worker.start()`.
 */
export function registerAllHandlers(worker: WorkerService): void {
  worker.register("email:send",       emailSendHandler);
  worker.register("image:resize",     imageResizeHandler);
  worker.register("report:generate",  reportGenerateHandler);
}
