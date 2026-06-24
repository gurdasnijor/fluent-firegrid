import * as Schema from "effect/Schema"

/**
 * The ergonomic authoring surface (restate-sdk-gen shape) over the engine
 * primitives. A handler is a **generator method** — `*greet(input) { … }` — the
 * input is the argument (no `handlerRequest`, no `Effect.gen` wrapper), and
 * `yield* run(...)` etc. stay typed because an Effect is `yield*`-able (its
 * iterator returns the success). `service({ name, handlers })` groups them;
 * `client(def)` is the typed call surface that hides `submit`/`attach`/exec-id.
 */

/** A handler body: a generator method receiving the decoded input. */
export type HandlerFn = (input: any) => Generator<any, any, any>
export type Handlers = Record<string, HandlerFn>

// extract the declared argument / generator-return types of a handler method
export type HandlerInput<H> = Parameters<H extends (...args: Array<any>) => any ? H : never> extends
  [infer I, ...any[]] ? I
  : void
export type HandlerOutput<H> = H extends (...args: Array<any>) => Generator<any, infer O, any> ? O : never

/** Optional durable I/O schemas for a handler (default: opaque JSON via `Schema.Unknown`). */
export interface HandlerSchemas<I = any, O = any> {
  readonly input?: Schema.Codec<I, any, never, never>
  readonly output?: Schema.Codec<O, any, never, never>
}

type SchemasFor<H extends Handlers> = {
  readonly [K in keyof H]?: HandlerSchemas<HandlerInput<H[K]>, HandlerOutput<H[K]>>
}

export interface MethodCodecs {
  readonly input: Schema.Codec<unknown, unknown, never, never>
  readonly output: Schema.Codec<unknown, unknown, never, never>
}

const methodCodecs = (
  handlers: Handlers,
  schemas: Record<string, HandlerSchemas> | undefined
): Record<string, MethodCodecs> =>
  Object.fromEntries(
    Object.keys(handlers).map((method) => [
      method,
      {
        input: (schemas?.[method]?.input ?? Schema.Unknown) as Schema.Codec<unknown, unknown, never, never>,
        output: (schemas?.[method]?.output ?? Schema.Unknown) as Schema.Codec<unknown, unknown, never, never>
      }
    ])
  )

/** A registerable service definition (stateless — each call a fresh execution). */
export interface ServiceDefinition<Name extends string, H extends Handlers> {
  readonly name: Name
  readonly kind: "service"
  /** Retained for input/output type inference; not a call surface. */
  readonly handlers: H
  readonly codecs: Record<string, MethodCodecs>
}

export interface ServiceConfig<Name extends string, H extends Handlers> {
  readonly name: Name
  readonly handlers: H
  readonly schemas?: SchemasFor<H>
}

/** Define a stateless durable service — each call is a fresh execution. */
export const service = <const Name extends string, H extends Handlers>(
  config: ServiceConfig<Name, H>
): ServiceDefinition<Name, H> => ({
  name: config.name,
  kind: "service",
  handlers: config.handlers,
  codecs: methodCodecs(config.handlers, config.schemas as Record<string, HandlerSchemas> | undefined)
})

/** A keyed virtual-object definition — durable state + exclusive methods (+ shared read-only methods), per key. */
export interface ObjectDefinition<Name extends string, H extends Handlers, S extends Handlers = Record<never, never>> {
  readonly name: Name
  readonly kind: "object"
  readonly handlers: H
  readonly shared: S
  readonly codecs: Record<string, MethodCodecs>
  readonly sharedCodecs: Record<string, MethodCodecs>
}

/**
 * Define a durable **virtual object** — a keyed, stateful entity. Each `(name, key)`
 * has its own persistent `state(Table)` store that survives across calls; `handlers`
 * run **exclusively** (single-writer, serialized via `client(obj, key)`), while
 * optional `shared` handlers run **concurrently, read-only** over a snapshot (via
 * `sharedClient(obj, key)`) and never block, nor are blocked by, the exclusive drainer.
 */
export const object = <const Name extends string, H extends Handlers, S extends Handlers = Record<never, never>>(
  config: ServiceConfig<Name, H> & { readonly shared?: S; readonly sharedSchemas?: SchemasFor<S> }
): ObjectDefinition<Name, H, S> => ({
  name: config.name,
  kind: "object",
  handlers: config.handlers,
  shared: (config.shared ?? {}) as S,
  codecs: methodCodecs(config.handlers, config.schemas as Record<string, HandlerSchemas> | undefined),
  sharedCodecs: methodCodecs(
    config.shared ?? {},
    config.sharedSchemas as Record<string, HandlerSchemas> | undefined
  )
})

/**
 * A durable **workflow** definition — an object specialization (not a third engine).
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
  /** The single exclusive entrypoint codecs, keyed by the reserved method name `run`. */
  readonly codecs: Record<string, MethodCodecs>
  /** The `run` entrypoint codecs (the sole member of `codecs`), typed for direct use. */
  readonly runCodecs: MethodCodecs
  readonly sharedCodecs: Record<string, MethodCodecs>
}

/** The reserved method name of a workflow's exclusive entrypoint. */
export const WORKFLOW_RUN = "run"

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
  config: WorkflowConfig<Name, R, S>
): WorkflowDefinition<Name, R, S> => {
  // `run` is the reserved exclusive entrypoint — a shared handler of the same name would
  // shadow it on `sharedClient(wf, id).run`. Typed out above; guard dynamically too.
  if (config.handlers !== undefined && Object.prototype.hasOwnProperty.call(config.handlers, WORKFLOW_RUN)) {
    throw new Error(
      `workflow ${JSON.stringify(config.name)}: a shared handler may not be named ${
        JSON.stringify(WORKFLOW_RUN)
      } (reserved for the run-once entrypoint)`
    )
  }
  const runCodecs = methodCodecs(
    { [WORKFLOW_RUN]: config.run },
    config.runSchema === undefined ? undefined : { [WORKFLOW_RUN]: config.runSchema }
  )
  return {
    name: config.name,
    kind: "workflow",
    run: config.run,
    shared: (config.handlers ?? {}) as S,
    codecs: runCodecs,
    runCodecs: runCodecs[WORKFLOW_RUN] as MethodCodecs,
    sharedCodecs: methodCodecs(
      config.handlers ?? {},
      config.sharedSchemas as Record<string, HandlerSchemas> | undefined
    )
  }
}

/** Any registerable service/object definition — the erased union the ingress dispatches over. */
export type AnyDef =
  | ServiceDefinition<string, Handlers>
  | ObjectDefinition<string, Handlers>
  | WorkflowDefinition<string, HandlerFn, Handlers>
