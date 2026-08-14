/**
 * src/api/server.ts
 *
 * Express application factory.
 * Separated from the process entry-point (src/index.ts) so it can be
 * imported by test harnesses without side-effects.
 */

import "express-async-errors";
import path from "path";
import express, {
  type Request,
  type Response,
  type NextFunction,
} from "express";
import { ZodError } from "zod";
import { taskRouter } from "./routes";
import { logger } from "../infra/logger";

// -----------------------------------------------------------------------------
// Standard API error shape
// -----------------------------------------------------------------------------

interface ApiError {
  code:     string;
  message:  string;
  issues?:  unknown;
}

function sendError(res: Response, status: number, body: ApiError): void {
  res.status(status).json({ error: body });
}

// -----------------------------------------------------------------------------
// App factory
// -----------------------------------------------------------------------------

export function createApp(): express.Application {
  const app = express();

  app.use(express.json({ limit: "1mb" }));
  app.use(express.urlencoded({ extended: false }));

  // -- Static dashboard --------------------------------------------------------
  // MUST be registered before the /v1 router and the 404 catch-all below,
  // otherwise GET / (and any other unmatched path) hits the catch-all first
  // and the SPA never gets served. express.static also handles GET / -> index.html
  // automatically, plus JS/CSS/asset requests.
  app.use(express.static(path.join(process.cwd(), "public")));

  // -- Routes ----------------------------------------------------------------
  app.use("/v1", taskRouter);

  // -- 404 handler -----------------------------------------------------------
  app.use((_req: Request, res: Response) => {
    sendError(res, 404, { code: "NOT_FOUND", message: "Route not found" });
  });

  // -- Global error handler --------------------------------------------------
  // Express identifies error handlers by 4-parameter signature — _next must be declared.
  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    if (err instanceof ZodError) {
      sendError(res, 400, {
        code:    "VALIDATION_ERROR",
        message: "Request validation failed",
        issues:  err.issues,
      });
      return;
    }

    const message = err instanceof Error ? err.message : "Internal server error";
    logger.error("[API] Unhandled error", { error: message });
    sendError(res, 500, { code: "INTERNAL_ERROR", message });
  });

  return app;
}
