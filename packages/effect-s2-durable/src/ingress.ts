/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unnecessary-type-assertion -- the ingress dispatches over heterogeneous definitions by name/method at runtime; this is an existential proxy boundary (the same reason service.ts disables no-explicit-any), and runtime correctness is covered by the S2-backed ingress test. */
import { Effect, Layer, Schema } from "effect"
import { HttpClient, HttpClientRequest, HttpRouter } from "effect/unstable/http"
import { HttpApi, HttpApiBuilder, HttpApiClient, HttpApiEndpoint, HttpApiGroup } from "effect/unstable/httpapi"
import { client, sendClient } from "./service.ts"
import type { Handlers, HandlerInput, HandlerOutput, InvokeOptions, ObjectDefinition, ServiceDefinition } from "./service.ts"
import { DurableExecutionRuntime } from "./Runtime.ts"

/**
 * The HTTP **ingress** for durable definitions — the network front door an
 * out-of-process caller uses to invoke a service/object/workflow handler, the
 * analog of Restate's `restate-sdk-clients` (`connect(url)`) talking to a served
 * deployment. Uses only the ABSTRACT Effect HTTP stack (`effect/unstable/http*` +
 * `httpapi`); the Node bindings (`NodeHttpServer`/`NodeHttpClient`) are supplied
 * at the edge, so the engine stays platform-free.
 *
 * Restate-style ergonomics on the client:
 * - **typed errors**: invocation failures come back as a `DurableFailure` in the
 *   Effect error channel, not opaque 500 defects;
 * - **idempotency keys**: forwarded so a retried call de-dups on the same id;
 * - **attach handles**: `sendClient(def).method(input)` returns an
 *   `InvocationHandle` whose `.attach` awaits the eventual result.
 *
 * Footgun-free split (as in Restate): the ingress client is obtained ONLY from
 * `connect(url)`; the in-handler durable-call path is `objectClient(def, key)`.
 */

type AnyCodec = Schema.Codec<any, any, never, never>
type HandlerCodecs = { readonly input: AnyCodec; readonly output: AnyCodec }

/** A typed invocation failure surfaced to the ingress client (not a 500 defect). */
export class DurableFailure extends Schema.ErrorClass<DurableFailure>("DurableFailure")({
  message: Schema.String,
}) {}

// ── the wire API (one generic group; per-def typing is layered on the client) ──

const InvokePayload = Schema.Struct({
  name: Schema.String,
  key: Schema.optionalKey(Schema.String),
  method: Schema.String,
  input: Schema.Unknown,
  idempotencyKey: Schema.optionalKey(Schema.String),
})

const AttachPayload = Schema.Struct({
  name: Schema.String,
  key: Schema.optionalKey(Schema.String),
  method: Schema.String,
  invocationId: Schema.String,
})

const CallSuccess = Schema.Struct({ output: Schema.Unknown })
const SendSuccess = Schema.Struct({ invocationId: Schema.String })

export const DurableApi = HttpApi.make("durable").add(
  HttpApiGroup.make("invocations")
    .add(HttpApiEndpoint.post("call", "/call", { payload: InvokePayload, success: CallSuccess, error: DurableFailure }))
    .add(HttpApiEndpoint.post("send", "/send", { payload: InvokePayload, success: SendSuccess, error: DurableFailure }))
    .add(HttpApiEndpoint.post("attach", "/attach", { payload: AttachPayload, success: CallSuccess, error: DurableFailure })),
)

// ── definitions registry + helpers ──────────────────────────────────────────

type AnyDef = ServiceDefinition<string, any> | ObjectDefinition<string, any>

interface InvokePayloadType {
  readonly name: string
  readonly key?: string
  readonly method: string
  readonly input?: unknown
  readonly idempotencyKey?: string
  readonly invocationId?: string
}

const resolve = (registry: Map<string, AnyDef>, payload: InvokePayloadType) => {
  const def = registry.get(payload.name)
  if (def === undefined) return undefined
  const compiled = def.compiled[payload.method]
  if (compiled === undefined) return undefined
  return { def, codec: compiled.handler as unknown as HandlerCodecs }
}

const optionsFor = (payload: InvokePayloadType): InvokeOptions | undefined =>
  payload.idempotencyKey === undefined ? undefined : { idempotencyKey: payload.idempotencyKey }

const messageOf = (cause: unknown): string => {
  if (typeof cause === "object" && cause !== null && "message" in cause) {
    const message = (cause as { readonly message: unknown }).message
    if (typeof message === "string") return message
  }
  return String(cause)
}

// Map any engine/codec failure into the typed DurableFailure channel (defects stay defects).
const isDurableFailure = Schema.is(DurableFailure)
const asFailure = (cause: unknown): DurableFailure =>
  isDurableFailure(cause) ? cause : new DurableFailure({ message: messageOf(cause) })

const notFound = (payload: InvokePayloadType) =>
  new DurableFailure({ message: `no handler ${payload.name}/${payload.method}` })

// ── server handlers ─────────────────────────────────────────────────────────
// The ingress sits OUTSIDE any handler (no ActiveInvocation), so `client`/
// `sendClient` are the correct surfaces; decode the wire input via the def's
// codec, drive the engine, re-encode the output.

// Resolve the def + decode the wire input (the shared head of call/send).
const prepare = (registry: Map<string, AnyDef>, payload: InvokePayloadType) =>
  Effect.gen(function*() {
    const resolved = resolve(registry, payload)
    if (resolved === undefined) return yield* notFound(payload)
    const input = yield* Schema.decodeUnknownEffect(resolved.codec.input)(payload.input)
    return { resolved, input }
  })

const callProxy = (def: AnyDef, key: string | undefined): Record<string, any> =>
  (def.kind === "object" ? client(def as ObjectDefinition<string, any>, key ?? "") : client(def as ServiceDefinition<string, any>)) as Record<string, any>

const sendProxy = (def: AnyDef, key: string | undefined): Record<string, any> =>
  (def.kind === "object" ? sendClient(def as ObjectDefinition<string, any>, key ?? "") : sendClient(def as ServiceDefinition<string, any>)) as Record<string, any>

const runCall = (registry: Map<string, AnyDef>, payload: InvokePayloadType) =>
  prepare(registry, payload).pipe(
    Effect.flatMap(({ input, resolved }) =>
      (callProxy(resolved.def, payload.key)[payload.method](input, optionsFor(payload)) as Effect.Effect<unknown, unknown, DurableExecutionRuntime>)
        .pipe(Effect.flatMap((output) => Schema.encodeEffect(resolved.codec.output)(output)), Effect.map((output) => ({ output })))),
    Effect.mapError(asFailure),
  )

const runSend = (registry: Map<string, AnyDef>, payload: InvokePayloadType) =>
  prepare(registry, payload).pipe(
    Effect.flatMap(({ input, resolved }) =>
      (sendProxy(resolved.def, payload.key)[payload.method](input, optionsFor(payload)) as Effect.Effect<string, unknown, DurableExecutionRuntime>)
        .pipe(Effect.map((invocationId) => ({ invocationId })))),
    Effect.mapError(asFailure),
  )

const runAttach = (registry: Map<string, AnyDef>, payload: InvokePayloadType) =>
  Effect.gen(function*() {
    const resolved = resolve(registry, payload)
    if (resolved === undefined) return yield* notFound(payload)
    const rt = yield* DurableExecutionRuntime
    const output = yield* rt.attach(payload.invocationId!, resolved.codec.output)
    return { output: yield* Schema.encodeEffect(resolved.codec.output)(output) }
  }).pipe(Effect.mapError(asFailure))

/**
 * The ingress server layer for a set of definitions. Requires `HttpServer`
 * (supplied at the edge by `NodeHttpServer.layer`) and `DurableExecutionRuntime`
 * (the engine, from `serviceLayer(...)` over the same definitions).
 */
export const durableIngress = (defs: ReadonlyArray<AnyDef>) => {
  const registry = new Map(defs.map((def) => [def.name, def] as const))
  const InvocationsLive = HttpApiBuilder.group(DurableApi, "invocations", (handlers) =>
    handlers
      .handle("call", ({ payload }) => runCall(registry, payload))
      .handle("send", ({ payload }) => runSend(registry, payload))
      .handle("attach", ({ payload }) => runAttach(registry, payload)))
  return HttpRouter.serve(HttpApiBuilder.layer(DurableApi).pipe(Layer.provide(InvocationsLive)))
}

// ── ingress client (obtained ONLY from connect — the footgun-free outer path) ──

/** A handle to a sent invocation: its id + an Effect that awaits the result. */
export interface InvocationHandle<O> {
  readonly invocationId: string
  readonly attach: Effect.Effect<O, DurableFailure>
}

/** Typed call surface for a service/object over the ingress (mirrors `ServiceClient`). */
export type IngressClient<H extends Handlers> = {
  readonly [K in keyof H]: (input: HandlerInput<H[K]>, options?: InvokeOptions) => Effect.Effect<HandlerOutput<H[K]>, DurableFailure>
}
/** Fire-and-forget surface: each call returns an awaitable `InvocationHandle`. */
export type IngressSendClient<H extends Handlers> = {
  readonly [K in keyof H]: (
    input: HandlerInput<H[K]>,
    options?: InvokeOptions,
  ) => Effect.Effect<InvocationHandle<HandlerOutput<H[K]>>, DurableFailure>
}

export interface DurableIngressClient {
  client<Name extends string, H extends Handlers>(def: ServiceDefinition<Name, H>): IngressClient<H>
  client<Name extends string, H extends Handlers>(def: ObjectDefinition<Name, H>, key: string): IngressClient<H>
  sendClient<Name extends string, H extends Handlers>(def: ServiceDefinition<Name, H>): IngressSendClient<H>
  sendClient<Name extends string, H extends Handlers>(def: ObjectDefinition<Name, H>, key: string): IngressSendClient<H>
}

const codecsOf = (def: AnyDef, method: string): HandlerCodecs => def.compiled[method]!.handler as unknown as HandlerCodecs

const payloadFor = (def: AnyDef, key: string | undefined, method: string, encoded: unknown, options?: InvokeOptions) => ({
  name: def.name,
  method,
  input: encoded,
  ...(key === undefined ? {} : { key }),
  ...(options?.idempotencyKey === undefined ? {} : { idempotencyKey: options.idempotencyKey }),
})

/**
 * Connect to a durable ingress at `baseUrl`. Returns typed client/sendClient
 * surfaces keyed by the SAME definition the server serves; input/output are
 * encoded/decoded via the definition's codecs. Requires an `HttpClient`
 * (supplied at the edge by `NodeHttpClient.layer`).
 */
export const connect = (
  baseUrl: string,
): Effect.Effect<DurableIngressClient, never, HttpClient.HttpClient> =>
  Effect.gen(function*() {
    const api = yield* HttpApiClient.make(DurableApi, {
      transformClient: (httpClient) => HttpClient.mapRequest(httpClient, HttpClientRequest.prependUrl(baseUrl)),
    }).pipe(Effect.orDie)

    const call = (def: AnyDef, key: string | undefined) => (method: string) => (input: unknown, options?: InvokeOptions) =>
      Effect.gen(function*() {
        const codec = codecsOf(def, method)
        const encoded = yield* Schema.encodeEffect(codec.input)(input).pipe(Effect.orDie)
        const result = yield* api.invocations.call({ payload: payloadFor(def, key, method, encoded, options) }).pipe(Effect.mapError(asFailure))
        return yield* Schema.decodeUnknownEffect(codec.output)((result as { output: unknown }).output).pipe(Effect.orDie)
      })

    const attachTo = (def: AnyDef, key: string | undefined, method: string, invocationId: string) =>
      Effect.gen(function*() {
        const codec = codecsOf(def, method)
        const result = yield* api.invocations.attach({
          payload: { name: def.name, method, invocationId, ...(key === undefined ? {} : { key }) },
        }).pipe(Effect.mapError(asFailure))
        return yield* Schema.decodeUnknownEffect(codec.output)((result as { output: unknown }).output).pipe(Effect.orDie)
      })

    const send = (def: AnyDef, key: string | undefined) => (method: string) => (input: unknown, options?: InvokeOptions) =>
      Effect.gen(function*() {
        const codec = codecsOf(def, method)
        const encoded = yield* Schema.encodeEffect(codec.input)(input).pipe(Effect.orDie)
        const result = yield* api.invocations.send({ payload: payloadFor(def, key, method, encoded, options) }).pipe(Effect.mapError(asFailure))
        const invocationId = (result as { invocationId: string }).invocationId
        return { invocationId, attach: attachTo(def, key, method, invocationId) }
      })

    const proxy = (build: (method: string) => (input: unknown, options?: InvokeOptions) => Effect.Effect<unknown, DurableFailure>) =>
      new Proxy({}, { get: (_t, method: string) => build(method) }) as any

    return {
      client: (def: AnyDef, key?: string) => proxy(call(def, key)),
      sendClient: (def: AnyDef, key?: string) => proxy(send(def, key)),
    } as DurableIngressClient
  })
