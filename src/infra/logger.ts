/**
 * src/infra/logger.ts
 *
 * Structured Winston logger.
 * - Console output (pretty in dev, JSON in production)
 * - Rotating file transport for persistent audit trails
 */

import path from "path";
import winston from "winston";
import DailyRotateFile from "winston-daily-rotate-file";

const { combine, timestamp, json, colorize, printf, errors } = winston.format;

const isProduction = process.env["NODE_ENV"] === "production";

// -- Pretty format for local development -------------------------------------
const devFormat = combine(
  colorize({ all: true }),
  timestamp({ format: "HH:mm:ss.SSS" }),
  errors({ stack: true }),
  printf(({ level, message, timestamp: ts, ...meta }) => {
    const metaStr =
      Object.keys(meta).length > 0 ? `  ${JSON.stringify(meta)}` : "";
    return `[${ts as string}] ${level}: ${message as string}${metaStr}`;
  }),
);

// -- Structured JSON format for production / log aggregation -----------------
const prodFormat = combine(
  timestamp(),
  errors({ stack: true }),
  json(),
);

const transports: winston.transport[] = [
  new winston.transports.Console({
    format: isProduction ? prodFormat : devFormat,
    handleExceptions: true,
  }),
];

// Add rotating file transport if LOG_DIR is configured
const logDir = process.env["LOG_DIR"];
if (logDir !== undefined && logDir !== "") {
  transports.push(
    new DailyRotateFile({
      dirname: path.resolve(logDir),
      filename: "scheduler-%DATE%.log",
      datePattern: "YYYY-MM-DD",
      maxFiles: "14d",
      maxSize: "50m",
      zippedArchive: true,
      format: prodFormat,
    }),
  );
}

export const logger = winston.createLogger({
  level: process.env["LOG_LEVEL"] ?? (isProduction ? "info" : "debug"),
  format: prodFormat,
  transports,
  exitOnError: false,
});
