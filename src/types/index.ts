/**
 * src/types/index.ts
 *
 * Shared TypeScript types and branded types used across the codebase.
 * Re-exports Prisma enums so consumers import from one place.
 */

export {
  TaskStatus,
  TaskPriority,
  WorkerStatus,
  LogLevel,
} from "@prisma/client";

// -----------------------------------------------------------------------------
// Branded scalar types
// Prevents mix-ups between task IDs and worker IDs at compile time.
// -----------------------------------------------------------------------------

declare const _brand: unique symbol;
type Brand<T, B extends string> = T & { readonly [_brand]: B };

export type TaskId   = Brand<string, "TaskId">;
export type WorkerId = Brand<string, "WorkerId">;

export function asTaskId(id: string): TaskId {
  return id as TaskId;
}

export function asWorkerId(id: string): WorkerId {
  return id as WorkerId;
}

// -----------------------------------------------------------------------------
// Task Enqueue DTO
// -----------------------------------------------------------------------------

import { TaskPriority } from "@prisma/client";

export interface EnqueueTaskOptions {
  /** Handler type identifier, e.g. "send-email" */
  type: string;
  /** Arbitrary JSON payload passed to the handler */
  payload: Record<string, unknown>;
  /** Defaults to MEDIUM */
  priority?: TaskPriority;
  /** ISO-8601 string or Date. Defaults to now (immediate). */
  scheduledAt?: Date | string;
  /** Maximum retry attempts before dead-lettering. Defaults to 3. */
  maxRetries?: number;
}

// -----------------------------------------------------------------------------
// Redis Stream Message Shape
// -----------------------------------------------------------------------------

/** Fields written to a Redis Stream entry when a task is enqueued */
export interface StreamTaskMessage {
  taskId: string;
  type: string;
  priority: string;
  enqueuedAt: string; // ISO-8601
}

// -----------------------------------------------------------------------------
// Result types
// -----------------------------------------------------------------------------

export type Result<T, E extends Error = Error> =
  | { success: true; value: T }
  | { success: false; error: E };

export function ok<T>(value: T): Result<T> {
  return { success: true, value };
}

export function err<E extends Error>(error: E): Result<never, E> {
  return { success: false, error };
}
