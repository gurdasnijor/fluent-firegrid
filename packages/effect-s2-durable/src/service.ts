/* eslint-disable @typescript-eslint/no-explicit-any -- handler input/output are existential at the definition boundary; `any` is required for inference across the handler record (mirrors restate-sdk-gen's define/free). Concrete types are recovered on the client surface via HandlerInput/HandlerOutput. */
import { Effect, Schema } from "effect"
import type { DurableExecutionError } from "./errors.ts"
import { handler } from "./handler.ts"
import { handlerRequest } from "./primitives.ts"
import { DurableExecutionRuntime } from "./Runtime.ts"
import type { DurableExecutionRuntimeApi } from "./Runtime.ts"
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

// ── clients ──────────────────────────────────────────────────────────────

export interface InvokeOptions {
  /** Pin the execution id (idempotent invocation). Default: a fresh id per call. */
  readonly idempotencyKey?: string
}

/** Call surface: `client(def).method(input)` submits + attaches, returning the result. */
export type ServiceClient<H extends Handlers> = {
  readonly [K in keyof H]: (
    input: HandlerInput<H[K]>,
    options?: InvokeOptions,
  ) => Effect.Effect<HandlerOutput<H[K]>, DurableExecutionError, DurableExecutionRuntime>
}

/** Fire-and-forget surface: `sendClient(def).method(input)` submits, returning the execution id. */
export type SendClient<H extends Handlers> = {
  readonly [K in keyof H]: (
    input: HandlerInput<H[K]>,
    options?: InvokeOptions,
  ) => Effect.Effect<string, DurableExecutionError, DurableExecutionRuntime>
}

const freshId = Effect.sync(() => globalThis.crypto.randomUUID())
const idFor = (options: InvokeOptions | undefined) => options?.idempotencyKey === undefined ? freshId : Effect.succeed(options.idempotencyKey)

/** Mint the execution id, submit, and hand back the id + runtime for the caller to finish. */
const beginInvoke = (
  compiled: Compiled,
  input: unknown,
  options: InvokeOptions | undefined,
): Effect.Effect<{ readonly id: string; readonly rt: DurableExecutionRuntimeApi }, DurableExecutionError, DurableExecutionRuntime> =>
  Effect.gen(function*() {
    const id = yield* idFor(options)
    const rt = yield* DurableExecutionRuntime
    yield* rt.submit(compiled.handler, id, input)
    return { id, rt }
  })

/** Build a per-method proxy over a definition; `finish` decides what each call returns. */
const makeProxy = <Name extends string, H extends Handlers, T>(
  def: ServiceDefinition<Name, H>,
  finish: (
    compiled: Compiled,
    ctx: { readonly id: string; readonly rt: DurableExecutionRuntimeApi },
  ) => Effect.Effect<T, DurableExecutionError, DurableExecutionRuntime>,
): Record<string, (input: unknown, options?: InvokeOptions) => Effect.Effect<T, DurableExecutionError, DurableExecutionRuntime>> =>
  Object.fromEntries(
    Object.entries(def.compiled).map(([method, compiled]) =>
      [
        method,
        (input: unknown, options?: InvokeOptions) =>
          beginInvoke(compiled, input, options).pipe(Effect.flatMap((ctx) => finish(compiled, ctx))),
      ] as const,
    ),
  )

/** A typed call client: `client(def).method(input)` submits + attaches, returning the result. */
export const client = <Name extends string, H extends Handlers>(def: ServiceDefinition<Name, H>): ServiceClient<H> =>
  // eslint-disable-next-line local/no-launder-cast -- dynamic proxy; each Effect<unknown> is the typed Effect<HandlerOutput> recovered structurally by ServiceClient<H>
  makeProxy(def, (compiled, { id, rt }) => rt.attach(compiled.handler, id)) as unknown as ServiceClient<H>

/** A typed fire-and-forget client: `sendClient(def).method(input)` submits, returning the execution id. */
export const sendClient = <Name extends string, H extends Handlers>(def: ServiceDefinition<Name, H>): SendClient<H> =>
  // eslint-disable-next-line local/no-launder-cast -- dynamic proxy (see client)
  makeProxy(def, (_compiled, { id }) => Effect.succeed(id)) as unknown as SendClient<H>
