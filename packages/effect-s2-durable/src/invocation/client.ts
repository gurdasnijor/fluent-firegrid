import { Effect, Option, type Schema } from "effect"
import { DurableEngine, type DurableEngineApi, type WorkflowStartStatus } from "../engine/api.ts"
import { ActiveInvocation } from "../engine/context.ts"
import { type DurableExecutionError, durableError } from "../errors.ts"
import { compileExclusive, compileOne, compileShared, type CompiledMethod } from "../authoring/compiler.ts"
import {
  type InvokeOptions,
  objectIdentity,
  type ObjectIdentity,
  planInvocationId,
  workflowRunIdFor,
} from "./plan.ts"
import type {
  AnyDef,
  HandlerFn,
  HandlerInput,
  HandlerOutput,
  Handlers,
  ObjectDefinition,
  ServiceDefinition,
  WorkflowDefinition,
} from "../authoring/definition.ts"

// a `void`-input method (e.g. `*get()`) takes no input argument; everything else
// takes its declared input. `options` is always optional and trailing.
type InvokeArgs<I> = [I] extends [void] ? [input?: undefined, options?: InvokeOptions]
  : [input: I, options?: InvokeOptions]

/** Call surface: `client(def).method(input)` submits + attaches, returning the result. */
export type ServiceClient<H extends Handlers> = {
  readonly [K in keyof H]: (
    ...args: InvokeArgs<HandlerInput<H[K]>>
  ) => Effect.Effect<HandlerOutput<H[K]>, DurableExecutionError, DurableEngine>
}

/** Fire-and-forget surface: `sendClient(def).method(input)` submits, returning the execution id. */
export type SendClient<H extends Handlers> = {
  readonly [K in keyof H]: (
    ...args: InvokeArgs<HandlerInput<H[K]>>
  ) => Effect.Effect<string, DurableExecutionError, DurableEngine>
}

/** Read-only call surface for an object's SHARED handlers: `sharedClient(obj, key).method(input)`. */
export type SharedClient<S extends Handlers> = {
  readonly [K in keyof S]: (
    ...args: InvokeArgs<HandlerInput<S[K]>>
  ) => Effect.Effect<HandlerOutput<S[K]>, DurableExecutionError, DurableEngine>
}

/** Mint the execution id, submit, and hand back the id + engine for the caller to finish. */
const beginInvoke = (
  compiled: CompiledMethod,
  method: string,
  input: unknown,
  options: InvokeOptions | undefined,
  object: ObjectIdentity | undefined,
): Effect.Effect<{ readonly id: string; readonly rt: DurableEngineApi }, DurableExecutionError, DurableEngine> =>
  Effect.gen(function*() {
    // Footgun guard (Restate keeps these surfaces physically separate): `client`/
    // `sendClient` are the TOP-LEVEL / ingress path. Inside a handler they would
    // mint a fresh random id on every replay (not deterministic) — so reject that
    // and direct callers to the in-handler `objectClient`/`objectSendClient`,
    // whose child ids are replay-stable.
    if (Option.isSome(yield* ActiveInvocation)) {
      return yield* durableError("submit")(
        new Error(
          "client(...)/sendClient(...) is the top-level invocation path and is not replay-safe inside a handler; use objectClient(def, key)/objectSendClient(def, key) for in-handler durable calls",
        ),
      )
    }
    const id = yield* planInvocationId(method, options, object)
    const rt = yield* DurableEngine
    yield* rt.submit(compiled.handler, id, input)
    return { id, rt }
  })

/** Build a per-method proxy over a definition; `finish` decides what each call returns. */
const makeProxy = <T>(
  def: AnyDef,
  finish: (
    compiled: CompiledMethod,
    ctx: { readonly id: string; readonly rt: DurableEngineApi },
  ) => Effect.Effect<T, DurableExecutionError, DurableEngine>,
  object: ObjectIdentity | undefined,
): Record<string, (input: unknown, options?: InvokeOptions) => Effect.Effect<T, DurableExecutionError, DurableEngine>> =>
  Object.fromEntries(
    Object.entries(compileExclusive(def)).map(([method, compiled]) =>
      [
        method,
        (input: unknown, options?: InvokeOptions) =>
          beginInvoke(compiled, method, input, options, object).pipe(Effect.flatMap((ctx) => finish(compiled, ctx))),
      ] as const,
    ),
  )

/**
 * A typed call client: `client(service).method(input)` for a stateless service, or
 * `client(object, key).method(input)` for a keyed virtual object. Submits + attaches,
 * returning the decoded result.
 */
export function client<Name extends string, H extends Handlers>(def: ServiceDefinition<Name, H>): ServiceClient<H>
export function client<Name extends string, H extends Handlers>(def: ObjectDefinition<Name, H>, key: string): ServiceClient<H>
export function client<Name extends string, H extends Handlers>(
  def: ServiceDefinition<Name, H> | ObjectDefinition<Name, H>,
  key?: string,
): ServiceClient<H> {
  // Intentional dynamic proxy cast: each Effect<unknown> is recovered structurally by ServiceClient<H>.
  return makeProxy(def, (compiled, { id, rt }) => rt.attach(id, compiled.output), objectIdentity(def, key)) as unknown as ServiceClient<H>
}

/**
 * A typed fire-and-forget client: `sendClient(service).method(input)` /
 * `sendClient(object, key).method(input)`. Submits, returning the execution id.
 */
export function sendClient<Name extends string, H extends Handlers>(def: ServiceDefinition<Name, H>): SendClient<H>
export function sendClient<Name extends string, H extends Handlers>(def: ObjectDefinition<Name, H>, key: string): SendClient<H>
export function sendClient<Name extends string, H extends Handlers>(
  def: ServiceDefinition<Name, H> | ObjectDefinition<Name, H>,
  key?: string,
): SendClient<H> {
  // Intentional dynamic proxy cast; see client.
  return makeProxy(def, (_compiled, { id }) => Effect.succeed(id), objectIdentity(def, key)) as unknown as SendClient<H>
}

type RuntimeCodec = Schema.Codec<unknown, unknown, never, never>
type ObjectCallTarget = { readonly object: string; readonly key: string; readonly method: string }

const makeObjectStepProxy = <T>(
  def: ObjectDefinition<string, Handlers>,
  key: string,
  invoke: (
    rt: DurableEngineApi,
    target: ObjectCallTarget,
    input: unknown,
    inputCodec: RuntimeCodec,
    outputCodec: RuntimeCodec,
  ) => Effect.Effect<T, DurableExecutionError, DurableEngine>,
): Record<string, (input: unknown) => Effect.Effect<T, DurableExecutionError, DurableEngine>> =>
  Object.entries(compileExclusive(def)).reduce<Record<string, (input: unknown) => Effect.Effect<T, DurableExecutionError, DurableEngine>>>(
    (proxy, [method, compiled]) => ({
      ...proxy,
      [method]: (input: unknown) =>
        Effect.flatMap(DurableEngine, (rt) =>
          invoke(
            rt,
            { object: def.name, key, method },
            input,
            compiled.input,
            compiled.output,
          )),
    }),
    {},
  )

/**
 * The typed **in-handler** call surface to another object: `objectClient(Def, key).method(input)`.
 * Issues a durable child object call and awaits its decoded result. Identity is derived
 * from the object DEFINITION (name + method + output schema), never raw strings; the
 * child id is replay-stable, so a parent replay re-reads the result instead of re-issuing.
 */
export const objectClient = <Name extends string, H extends Handlers>(
  def: ObjectDefinition<Name, H>,
  key: string,
): ServiceClient<H> =>
  // Intentional dynamic proxy cast: each Effect<unknown> is recovered structurally by ServiceClient<H>.
  makeObjectStepProxy(def, key, (rt, target, input, inputCodec, outputCodec) =>
    rt.callStep(target, input, inputCodec, outputCodec)) as unknown as ServiceClient<H>

/**
 * The typed **in-handler** one-way send surface: `objectSendClient(Def, key).method(input)`
 * issues the child call without awaiting, returning its id (`restate`'s `.send()`).
 */
export const objectSendClient = <Name extends string, H extends Handlers>(
  def: ObjectDefinition<Name, H>,
  key: string,
): SendClient<H> =>
  // Intentional dynamic proxy cast; see objectClient.
  makeObjectStepProxy(def, key, (rt, target, input, inputCodec) =>
    rt.sendStep(target, input, inputCodec)) as unknown as SendClient<H>

/**
 * A typed **shared** (read-only) call surface for a virtual object or workflow:
 * `sharedClient(obj, key).method(input)` / `sharedClient(wf, id).query(input)`. Each
 * call runs ephemerally over a folded snapshot — no admission, no exclusive lock — so
 * reads never block (nor are blocked by) the drainer. Shared handlers cannot write
 * state or use durable primitives.
 */
export function sharedClient<Name extends string, H extends Handlers, S extends Handlers>(
  def: ObjectDefinition<Name, H, S>,
  key: string,
): SharedClient<S>
export function sharedClient<Name extends string, R extends HandlerFn, S extends Handlers>(
  def: WorkflowDefinition<Name, R, S>,
  id: string,
): SharedClient<S>
export function sharedClient(
  def: ObjectDefinition<string, Handlers> | WorkflowDefinition<string, HandlerFn, Handlers>,
  key: string,
): Record<string, unknown> {
   
  return Object.fromEntries(
    Object.entries(compileShared(def)).map(([method, compiled]) =>
      [
        method,
        (input: unknown) =>
          Effect.flatMap(DurableEngine, (rt) =>
            rt.sharedCall(
              compiled.handler,
              def.name,
              key,
              input,
              compiled.output,
            )),
      ] as const,
    ),
  )
}

// ── workflow lifecycle ─────────────────────────────────────────────────────

/**
 * The DETERMINISTIC owner call id of a workflow's `run` for a given workflow id — the
 * anchor for run-once admission, `workflowAttach`, and ingress `resolveSignal`. Keyed by
 * `{ object: name, key: id, method: "run", nonce: id }`, so every start of the same id
 * collides on one id (admission dedups → at most one run).
 */
export const workflowRunId = <Name extends string, R extends HandlerFn, S extends Handlers>(
  def: WorkflowDefinition<Name, R, S>,
  id: string,
): Effect.Effect<string, DurableExecutionError> =>
  workflowRunIdFor(def, id)

/**
 * Start a workflow's `run` for `id` (run-once). Returns `"started"` on the first start
 * and `"alreadyStarted"` on any later start — never a second run. Await its result with
 * `workflowAttach(def, id)`.
 */
export const workflowSubmit = <Name extends string, R extends HandlerFn, S extends Handlers>(
  def: WorkflowDefinition<Name, R, S>,
  id: string,
  ...args: InvokeArgs<HandlerInput<R>>
): Effect.Effect<WorkflowStartStatus, DurableExecutionError, DurableEngine> =>
  Effect.gen(function*() {
    const runCallId = yield* workflowRunId(def, id)
    const rt = yield* DurableEngine
    const compiledRun = compileOne(def, "run")
    if (compiledRun === undefined) {
      return yield* durableError("workflowSubmit")(new Error(`workflow ${def.name} has no run handler`))
    }
    return yield* rt.workflowStart(compiledRun.handler, runCallId, args[0])
  })

/** Attach to a running/completed workflow `run` and return its decoded output. */
export const workflowAttach = <Name extends string, R extends HandlerFn, S extends Handlers>(
  def: WorkflowDefinition<Name, R, S>,
  id: string,
): Effect.Effect<HandlerOutput<R>, DurableExecutionError, DurableEngine> =>
  Effect.gen(function*() {
    const runCallId = yield* workflowRunId(def, id)
    const rt = yield* DurableEngine
     
    return (yield* rt.attach(runCallId, def.runCodecs.output)) as HandlerOutput<R>
  })
