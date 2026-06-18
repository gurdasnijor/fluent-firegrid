/* eslint-disable @typescript-eslint/no-explicit-any -- handler input/output are existential at the definition boundary; `any` is required for inference across the handler record (mirrors restate-sdk-gen's define/free). Concrete types are recovered on the client surface via HandlerInput/HandlerOutput. */
import { Effect, Random, type Layer, Schema } from "effect"
import type { S2Client } from "effect-s2"
import { encodeObjectCallId, OBJECT_ID_PREFIX } from "./actor/core.ts"
import { type DurableExecutionError, durableError } from "./errors.ts"
import { handler } from "./handler.ts"
import { handlerRequest } from "./primitives.ts"
import { DurableExecutionRuntime } from "./Runtime.ts"
import type { DurableExecutionRuntimeApi, ObjectHandlerSeed, RegisteredHandler, WorkflowStartStatus } from "./Runtime.ts"
import type { Handler } from "./types.ts"

/**
 * The ergonomic authoring surface (restate-sdk-gen shape) over the engine
 * primitives. A handler is a **generator method** — `*greet(input) { … }` — the
 * input is the argument (no `handlerRequest`, no `Effect.gen` wrapper), and
 * `yield* run(...)` etc. stay typed because an Effect is `yield*`-able (its
 * iterator returns the success). `service({ name, handlers })` groups them;
 * `client(def)` is the typed call surface that hides `submit`/`attach`/exec-id.
 */

/** A handler body: a generator method receiving the decoded input. */
type HandlerFn = (input: any) => Generator<any, any, any>
export type Handlers = Record<string, HandlerFn>

// extract the declared argument / generator-return types of a handler method
export type HandlerInput<H> = Parameters<H extends (...args: any[]) => any ? H : never> extends [infer I, ...any[]] ? I
  : void
export type HandlerOutput<H> = H extends (...args: any[]) => Generator<any, infer O, any> ? O : never

/** Optional durable I/O schemas for a handler (default: opaque JSON via `Schema.Unknown`). */
export interface HandlerSchemas<I = any, O = any> {
  readonly input?: Schema.Codec<I, any, never, never>
  readonly output?: Schema.Codec<O, any, never, never>
}

type SchemasFor<H extends Handlers> = {
  readonly [K in keyof H]?: HandlerSchemas<HandlerInput<H[K]>, HandlerOutput<H[K]>>
}

interface Compiled {
  // self-contained: a registered service handler has no unmet `R` beyond the
  // runtime (provided by `submit`) and surfaces no typed error to the caller.
  readonly handler: Handler<unknown, unknown, never, never>
}

/** A registerable service definition (stateless — each call a fresh execution). */
export interface ServiceDefinition<Name extends string, H extends Handlers> {
  readonly name: Name
  readonly kind: "service"
  /** Retained for input/output type inference; not a call surface. */
  readonly handlers: H
  readonly compiled: Record<string, Compiled>
}

export interface ServiceConfig<Name extends string, H extends Handlers> {
  readonly name: Name
  readonly handlers: H
  readonly schemas?: SchemasFor<H>
}

const compileHandlers = (
  name: string,
  handlers: Handlers,
  schemas: Record<string, HandlerSchemas> | undefined,
): Record<string, Compiled> =>
  Object.fromEntries(
    Object.entries(handlers).map(([method, fn]) => {
      const input = schemas?.[method]?.input ?? Schema.Unknown
      const output = schemas?.[method]?.output ?? Schema.Unknown
      // fetch the decoded input (internal), then run the generator body with it —
      // the user writes neither `handlerRequest` nor an `Effect.gen` wrapper.
      const body = Effect.fnUntraced(fn)
      const program = handlerRequest(input).pipe(Effect.flatMap((decoded) => body(decoded)))
      const compiledHandler = handler(`${name}/${method}`, { input, output })(program) as Handler<
        unknown,
        unknown,
        never,
        never
      >
      return [method, { handler: compiledHandler }] as const
    }),
  )

/** Define a stateless durable service — each call is a fresh execution. */
export const service = <const Name extends string, H extends Handlers>(
  config: ServiceConfig<Name, H>,
): ServiceDefinition<Name, H> => ({
  name: config.name,
  kind: "service",
  handlers: config.handlers,
  compiled: compileHandlers(config.name, config.handlers, config.schemas as Record<string, HandlerSchemas> | undefined),
})

/** A keyed virtual-object definition — durable state + exclusive methods (+ shared read-only methods), per key. */
export interface ObjectDefinition<Name extends string, H extends Handlers, S extends Handlers = Record<never, never>> {
  readonly name: Name
  readonly kind: "object"
  readonly handlers: H
  readonly shared: S
  readonly compiled: Record<string, Compiled>
  readonly compiledShared: Record<string, Compiled>
}

/**
 * Define a durable **virtual object** — a keyed, stateful entity. Each `(name, key)`
 * has its own persistent `state(Table)` store that survives across calls; `handlers`
 * run **exclusively** (single-writer, serialized via `client(obj, key)`), while
 * optional `shared` handlers run **concurrently, read-only** over a snapshot (via
 * `sharedClient(obj, key)`) and never block, nor are blocked by, the exclusive drainer.
 */
export const object = <const Name extends string, H extends Handlers, S extends Handlers = Record<never, never>>(
  config: ServiceConfig<Name, H> & { readonly shared?: S; readonly sharedSchemas?: SchemasFor<S> },
): ObjectDefinition<Name, H, S> => ({
  name: config.name,
  kind: "object",
  handlers: config.handlers,
  shared: (config.shared ?? {}) as S,
  compiled: compileHandlers(config.name, config.handlers, config.schemas as Record<string, HandlerSchemas> | undefined),
  compiledShared: compileHandlers(
    config.name,
    config.shared ?? {},
    config.sharedSchemas as Record<string, HandlerSchemas> | undefined,
  ),
})

/**
 * A durable **workflow** definition — an object specialization (not a third runtime).
 * Its `run` is a single exclusive handler admitted **at most once** per workflow id;
 * optional `handlers` are shared, read-only **query** handlers over the run's owner
 * projection. Waits/timers/durable steps/state all reuse the same object primitives,
 * scoped to the workflow id. The reserved method name for the entrypoint is `run`.
 */
export interface WorkflowDefinition<Name extends string, R extends HandlerFn, S extends Handlers> {
  readonly name: Name
  readonly kind: "workflow"
  readonly run: R
  readonly shared: S
  /** The single exclusive entrypoint, keyed by the reserved method name `run`. */
  readonly compiled: Record<string, Compiled>
  /** The compiled `run` entrypoint (the sole member of `compiled`), typed for direct use. */
  readonly compiledRun: Compiled
  readonly compiledShared: Record<string, Compiled>
}

/** The reserved method name of a workflow's exclusive entrypoint. */
const WORKFLOW_RUN = "run"

export interface WorkflowConfig<Name extends string, R extends HandlerFn, S extends Handlers> {
  readonly name: Name
  /** The exclusive, run-once entrypoint. */
  readonly run: R
  /**
   * Optional shared, read-only query/signal handlers over the workflow's owner projection.
   * The method name `run` is RESERVED for the entrypoint — typed out here AND rejected at
   * definition time (see `workflow()`), so `sharedClient(wf, id).run` can never be ambiguous.
   */
  readonly handlers?: S & { readonly run?: never }
  /** Durable I/O schema for the `run` entrypoint (default: opaque JSON). */
  readonly runSchema?: HandlerSchemas<HandlerInput<R>, HandlerOutput<R>>
  /** Durable I/O schemas for the shared query handlers. */
  readonly sharedSchemas?: SchemasFor<S>
}

/**
 * Define a durable **workflow** — `run` executes once per workflow id (a second start
 * returns `"alreadyStarted"`, never a second run), while `handlers` are shared queries.
 * Drive it with `workflowSubmit` / `workflowAttach` (+ `resolveSignal` for ingress
 * signals on `workflowRunId(def, id)`); read it with `sharedClient(def, id)`.
 */
export const workflow = <const Name extends string, R extends HandlerFn, S extends Handlers = Record<never, never>>(
  config: WorkflowConfig<Name, R, S>,
): WorkflowDefinition<Name, R, S> => {
  // `run` is the reserved exclusive entrypoint — a shared handler of the same name would
  // shadow it on `sharedClient(wf, id).run`. Typed out above; guard at runtime too.
  if (config.handlers !== undefined && Object.prototype.hasOwnProperty.call(config.handlers, WORKFLOW_RUN)) {
    throw new Error(`workflow ${JSON.stringify(config.name)}: a shared handler may not be named ${JSON.stringify(WORKFLOW_RUN)} (reserved for the run-once entrypoint)`)
  }
  const compiledRun = compileHandlers(
    config.name,
    { [WORKFLOW_RUN]: config.run },
    config.runSchema === undefined ? undefined : { [WORKFLOW_RUN]: config.runSchema },
  )
  return {
    name: config.name,
    kind: "workflow",
    run: config.run,
    shared: (config.handlers ?? {}) as S,
    compiled: compiledRun,
    compiledRun: compiledRun[WORKFLOW_RUN] as Compiled,
    compiledShared: compileHandlers(
      config.name,
      config.handlers ?? {},
      config.sharedSchemas as Record<string, HandlerSchemas> | undefined,
    ),
  }
}

// ── clients ──────────────────────────────────────────────────────────────

export interface InvokeOptions {
  /** Pin the execution id (idempotent invocation). Default: a fresh id per call. */
  readonly idempotencyKey?: string
}

// a `void`-input method (e.g. `*get()`) takes no input argument; everything else
// takes its declared input. `options` is always optional and trailing.
type InvokeArgs<I> = [I] extends [void] ? [input?: undefined, options?: InvokeOptions]
  : [input: I, options?: InvokeOptions]

/** Call surface: `client(def).method(input)` submits + attaches, returning the result. */
export type ServiceClient<H extends Handlers> = {
  readonly [K in keyof H]: (
    ...args: InvokeArgs<HandlerInput<H[K]>>
  ) => Effect.Effect<HandlerOutput<H[K]>, DurableExecutionError, DurableExecutionRuntime>
}

/** Fire-and-forget surface: `sendClient(def).method(input)` submits, returning the execution id. */
export type SendClient<H extends Handlers> = {
  readonly [K in keyof H]: (
    ...args: InvokeArgs<HandlerInput<H[K]>>
  ) => Effect.Effect<string, DurableExecutionError, DurableExecutionRuntime>
}

/** Read-only call surface for an object's SHARED handlers: `sharedClient(obj, key).method(input)`. */
export type SharedClient<S extends Handlers> = {
  readonly [K in keyof S]: (
    ...args: InvokeArgs<HandlerInput<S[K]>>
  ) => Effect.Effect<HandlerOutput<S[K]>, DurableExecutionError, DurableExecutionRuntime>
}

/** A virtual object's identity for a call: its definition name + the typed key. */
interface ObjectIdentity {
  readonly name: string
  readonly key: string
}

const freshNonce = Effect.map(
  Effect.all([Random.nextInt, Random.nextInt, Random.nextInt]),
  (parts) => parts.map((part) => Math.abs(part).toString(36)).join("-"),
)
const nonceFor = (options: InvokeOptions | undefined) =>
  options?.idempotencyKey === undefined ? freshNonce : Effect.succeed(options.idempotencyKey)

// Service ids are an opaque nonce; object ids are a schema-owned call id that
// carries `{ object, key, method, nonce }` so `attach`/`poll` self-route to the
// owner stream (no legacy `name:key` delimiter identity).
const mintId = (
  method: string,
  options: InvokeOptions | undefined,
  object: ObjectIdentity | undefined,
): Effect.Effect<string, DurableExecutionError> =>
  object === undefined
    // a service id IS its idempotencyKey/nonce — reserve the object namespace so a
    // service id can never be misrouted to an owner stream.
    ? nonceFor(options).pipe(
      Effect.flatMap((id) =>
        id.startsWith(OBJECT_ID_PREFIX)
          ? Effect.fail(
            durableError("submit")(
              new Error(`idempotencyKey must not start with the reserved prefix ${JSON.stringify(OBJECT_ID_PREFIX)}`),
            ),
          )
          : Effect.succeed(id),
      ),
    )
    : nonceFor(options).pipe(
      Effect.flatMap((nonce) =>
        encodeObjectCallId({ object: object.name, key: object.key, method, nonce }).pipe(
          Effect.mapError(durableError("object.callId")),
        ),
      ),
    )

/** Mint the execution id, submit, and hand back the id + runtime for the caller to finish. */
const beginInvoke = (
  compiled: Compiled,
  method: string,
  input: unknown,
  options: InvokeOptions | undefined,
  object: ObjectIdentity | undefined,
): Effect.Effect<{ readonly id: string; readonly rt: DurableExecutionRuntimeApi }, DurableExecutionError, DurableExecutionRuntime> =>
  Effect.gen(function*() {
    const id = yield* mintId(method, options, object)
    const rt = yield* DurableExecutionRuntime
    yield* rt.submit(compiled.handler, id, input)
    return { id, rt }
  })

/** Build a per-method proxy over a definition; `finish` decides what each call returns. */
const makeProxy = <T>(
  def: { readonly compiled: Record<string, Compiled> },
  finish: (
    compiled: Compiled,
    ctx: { readonly id: string; readonly rt: DurableExecutionRuntimeApi },
  ) => Effect.Effect<T, DurableExecutionError, DurableExecutionRuntime>,
  object: ObjectIdentity | undefined,
): Record<string, (input: unknown, options?: InvokeOptions) => Effect.Effect<T, DurableExecutionError, DurableExecutionRuntime>> =>
  Object.fromEntries(
    Object.entries(def.compiled).map(([method, compiled]) =>
      [
        method,
        (input: unknown, options?: InvokeOptions) =>
          beginInvoke(compiled, method, input, options, object).pipe(Effect.flatMap((ctx) => finish(compiled, ctx))),
      ] as const,
    ),
  )

/** The object identity for a call, or `undefined` for a stateless service. */
const objectIdentity = (
  def: { readonly name: string },
  key: string | undefined,
): ObjectIdentity | undefined => key === undefined ? undefined : { name: def.name, key }

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
  return makeProxy(def, (compiled, { id, rt }) =>
    rt.attach(id, compiled.handler.output as Schema.Codec<unknown, unknown, never, never>), objectIdentity(def, key)) as unknown as ServiceClient<H>
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
    rt: DurableExecutionRuntimeApi,
    target: ObjectCallTarget,
    input: unknown,
    inputCodec: RuntimeCodec,
    outputCodec: RuntimeCodec,
  ) => Effect.Effect<T, DurableExecutionError, DurableExecutionRuntime>,
): Record<string, (input: unknown) => Effect.Effect<T, DurableExecutionError, DurableExecutionRuntime>> =>
  Object.entries(def.compiled).reduce<Record<string, (input: unknown) => Effect.Effect<T, DurableExecutionError, DurableExecutionRuntime>>>(
    (proxy, [method, compiled]) => ({
      ...proxy,
      [method]: (input: unknown) =>
        Effect.flatMap(DurableExecutionRuntime, (rt) =>
          invoke(
            rt,
            { object: def.name, key, method },
            input,
            compiled.handler.input as RuntimeCodec,
            compiled.handler.output as RuntimeCodec,
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
  def: { readonly name: string; readonly compiledShared: Record<string, Compiled> },
  key: string,
): Record<string, unknown> {
   
  return Object.fromEntries(
    Object.entries(def.compiledShared).map(([method, compiled]) =>
      [
        method,
        (input: unknown) =>
          Effect.flatMap(DurableExecutionRuntime, (rt) =>
            rt.sharedCall(
              compiled.handler,
              def.name,
              key,
              input,
              compiled.handler.output as Schema.Codec<unknown, unknown, never, never>,
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
  encodeObjectCallId({ object: def.name, key: id, method: WORKFLOW_RUN, nonce: id }).pipe(
    Effect.mapError(durableError("workflow.runId")),
  )

/**
 * Start a workflow's `run` for `id` (run-once). Returns `"started"` on the first start
 * and `"alreadyStarted"` on any later start — never a second run. Await its result with
 * `workflowAttach(def, id)`.
 */
export const workflowSubmit = <Name extends string, R extends HandlerFn, S extends Handlers>(
  def: WorkflowDefinition<Name, R, S>,
  id: string,
  ...args: InvokeArgs<HandlerInput<R>>
): Effect.Effect<WorkflowStartStatus, DurableExecutionError, DurableExecutionRuntime> =>
  Effect.gen(function*() {
    const runCallId = yield* workflowRunId(def, id)
    const rt = yield* DurableExecutionRuntime
    return yield* rt.workflowStart(def.compiledRun.handler, runCallId, args[0])
  })

/** Attach to a running/completed workflow `run` and return its decoded output. */
export const workflowAttach = <Name extends string, R extends HandlerFn, S extends Handlers>(
  def: WorkflowDefinition<Name, R, S>,
  id: string,
): Effect.Effect<HandlerOutput<R>, DurableExecutionError, DurableExecutionRuntime> =>
  Effect.gen(function*() {
    const runCallId = yield* workflowRunId(def, id)
    const rt = yield* DurableExecutionRuntime
     
    return (yield* rt.attach(
      runCallId,
      def.compiledRun.handler.output as Schema.Codec<unknown, unknown, never, never>,
    )) as HandlerOutput<R>
  })

/**
 * The engine layer **seeded with these definitions' handlers** so boot recovery can
 * re-drive their running/suspended work after a process restart. Use this instead of
 * the bare `DurableExecutionRuntime.layer()` whenever work can outlive the process
 * (it parks on `sleep`/`signal`/`awakeable`, or is a pending object/workflow call).
 * Service handlers seed the by-name registry; object/workflow methods seed owner-stream
 * recovery (a workflow's `run` re-drives its pending head exactly like an object call).
 */
export const serviceLayer = (
  ...defs: ReadonlyArray<
    | ServiceDefinition<string, Handlers>
    | ObjectDefinition<string, Handlers>
    | WorkflowDefinition<string, HandlerFn, Handlers>
  >
): Layer.Layer<DurableExecutionRuntime, DurableExecutionError, S2Client> => {
  const services = defs.filter((def): def is ServiceDefinition<string, Handlers> => def.kind === "service")
  // objects and workflows share owner-stream recovery: seed each `compiled` method as
  // `${name}/${method}`. (A workflow's only exclusive method is `run`.)
  const ownerLogged = defs.filter(
    (def): def is ObjectDefinition<string, Handlers> | WorkflowDefinition<string, HandlerFn, Handlers> =>
      def.kind === "object" || def.kind === "workflow",
  )
  return DurableExecutionRuntime.layer(
    services.flatMap((def): ReadonlyArray<RegisteredHandler> => Object.values(def.compiled).map((c) => c.handler)),
    ownerLogged.flatMap((def): ReadonlyArray<ObjectHandlerSeed> =>
      Object.entries(def.compiled).map(([method, c]) => ({ object: def.name, method, handler: c.handler })),
    ),
  )
}
