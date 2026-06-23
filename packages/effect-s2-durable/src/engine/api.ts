import { Context, type Effect, type Schema } from "effect"
import type { DurableExecutionError } from "../errors.ts"
import type { Handler } from "../authoring/types.ts"
import type { ResultReaderApi } from "./result-reader.ts"

/**
 * The outcome of starting a workflow. A workflow `run` is admitted at most once
 * per workflow id: the first start is `"started"`; any later start is
 * `"alreadyStarted"`.
 */
export type WorkflowStartStatus = "started" | "alreadyStarted"

export type DurableQuery = <A, I>(
  handler: Handler<unknown, unknown, never, never>,
  object: string,
  key: string,
  input: unknown,
  schema: Schema.Codec<A, I, never, never>,
) => Effect.Effect<A, DurableExecutionError>

export type ExternalResolution = <A, I>(
  executionId: string,
  name: string,
  schema: Schema.Codec<A, I, never, never>,
  value: A,
) => Effect.Effect<void, DurableExecutionError>

/** The public durable engine surface that authoring APIs and primitives target. */
export interface DurableEngineApi {
  readonly submit: <I, O, E, R>(
    handler: Handler<I, O, E, R>,
    executionId: string,
    input: I,
  ) => Effect.Effect<void, DurableExecutionError, R>
  readonly attach: ResultReaderApi["attach"]
  readonly poll: ResultReaderApi["poll"]
  readonly query: DurableQuery
  readonly resolveAwakeable: ExternalResolution
  readonly resolveDurablePromise: ExternalResolution
  readonly workflowStart: <I, O, E, R>(
    handler: Handler<I, O, E, R>,
    runCallId: string,
    input: I,
  ) => Effect.Effect<WorkflowStartStatus, DurableExecutionError, R>
}

export class DurableEngine extends Context.Service<DurableEngine, DurableEngineApi>()(
  "effect-s2-durable/engine/api/DurableEngine",
) {}
