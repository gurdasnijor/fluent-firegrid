import { Context, type Effect, type Schema } from "effect"
import type { DurableExecutionError } from "../errors.ts"
import type { Handler } from "../authoring/types.ts"
import type { HandlerPrimitivesApi } from "./handler-primitives.ts"
import type { ResolutionRouterApi } from "./resolution-router.ts"
import type { ResultReaderApi } from "./result-reader.ts"

/** The address of a durable object call target (`call`/`send` between executions). */
export interface CallTarget {
  readonly object: string
  readonly key: string
  readonly method: string
}

/**
 * The outcome of starting a workflow. A workflow `run` is admitted at most once
 * per workflow id: the first start is `"started"`; any later start is
 * `"alreadyStarted"`.
 */
export type WorkflowStartStatus = "started" | "alreadyStarted"

/** The public durable engine surface that authoring APIs and primitives target. */
export interface DurableEngineApi {
  readonly assertTopLevel: Effect.Effect<void, DurableExecutionError>
  readonly submit: <I, O, E, R>(
    handler: Handler<I, O, E, R>,
    executionId: string,
    input: I,
  ) => Effect.Effect<void, DurableExecutionError, R>
  readonly attach: ResultReaderApi["attach"]
  readonly poll: ResultReaderApi["poll"]
  readonly runStep: HandlerPrimitivesApi["runStep"]
  readonly handlerRequest: HandlerPrimitivesApi["handlerRequest"]
  readonly sleepStep: HandlerPrimitivesApi["sleepStep"]
  readonly stateGet: HandlerPrimitivesApi["stateGet"]
  readonly stateSet: HandlerPrimitivesApi["stateSet"]
  readonly stateDelete: HandlerPrimitivesApi["stateDelete"]
  readonly awaitDeferred: HandlerPrimitivesApi["awaitDeferred"]
  readonly resolveLocal: HandlerPrimitivesApi["resolveLocal"]
  readonly resolveExternal: ResolutionRouterApi["resolveExternal"]
  readonly resolvePromise: HandlerPrimitivesApi["resolvePromise"]
  readonly nextAwakeableId: HandlerPrimitivesApi["nextAwakeableId"]
  readonly sharedCall: <A, I>(
    handler: Handler<unknown, unknown, never, never>,
    object: string,
    key: string,
    input: unknown,
    schema: Schema.Codec<A, I, never, never>,
  ) => Effect.Effect<A, DurableExecutionError>
  readonly callStep: <A, I, B, J>(
    target: CallTarget,
    input: unknown,
    inputSchema: Schema.Codec<B, J, never, never>,
    schema: Schema.Codec<A, I, never, never>,
  ) => Effect.Effect<A, DurableExecutionError>
  readonly sendStep: <B, J>(
    target: CallTarget,
    input: unknown,
    inputSchema: Schema.Codec<B, J, never, never>,
  ) => Effect.Effect<string, DurableExecutionError>
  readonly workflowStart: <I, O, E, R>(
    handler: Handler<I, O, E, R>,
    runCallId: string,
    input: I,
  ) => Effect.Effect<WorkflowStartStatus, DurableExecutionError, R>
}

export class DurableEngine extends Context.Service<DurableEngine, DurableEngineApi>()(
  "effect-s2-durable/engine/api/DurableEngine",
) {}
