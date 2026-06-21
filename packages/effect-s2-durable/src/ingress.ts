/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unnecessary-type-assertion -- the ingress dispatches over heterogeneous definitions by name/method at runtime; this is an existential proxy boundary (the same reason service.ts disables no-explicit-any), and runtime correctness is covered by the S2-backed ingress test. */
import { Effect, Layer, Option, Schema } from "effect"
import { HttpClient, HttpClientRequest, HttpRouter } from "effect/unstable/http"
import { HttpApi, HttpApiBuilder, HttpApiClient, HttpApiEndpoint, HttpApiGroup } from "effect/unstable/httpapi"
import { encodeObjectCallId } from "./actor/core.ts"
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
 *   `InvocationHandle` whose `.attach` awaits the eventual result;
 * - **non-blocking output**: `.output` (or `ingress.output(def[, key]).method(...)`)
 *   reads the completed result without blocking — `Option.none()` while still
 *   running (the Effect-idiomatic analog of Restate's `/output` HTTP 470);
 * - **attach/output by idempotency key**: a caller that didn't make the original
 *   send can re-attach or poll using only `(def[, key], method, { idempotencyKey })`
 *   — the server reconstructs the engine id the original `idempotencyKey` minted.
 *
 * Footgun-free split (as in Restate): the ingress client is obtained ONLY from
 * `connect(url)`; the in-handler durable-call path is `objectClient(def, key)`.
 *
 * NOTE: cancel / kill / purge / status are deliberately NOT here — in Restate
 * those are admin-plane operations (a separate port), not ingress; keep them off
 * this surface.
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

// Locate an existing invocation by EITHER its server-minted id OR the
// idempotency key the original send used (the server reconstructs the id).
const LocatePayload = Schema.Struct({
  name: Schema.String,
  key: Schema.optionalKey(Schema.String),
  method: Schema.String,
  invocationId: Schema.optionalKey(Schema.String),
  idempotencyKey: Schema.optionalKey(Schema.String),
})

const CallSuccess = Schema.Struct({ output: Schema.Unknown })
const SendSuccess = Schema.Struct({ invocationId: Schema.String })
// Non-blocking read: `ready` carries the decoded output, `notReady` means the
// invocation is still running (the typed analog of Restate's `/output` 470).
const OutputResult = Schema.Union([
  Schema.Struct({ status: Schema.Literal("ready"), output: Schema.Unknown }),
  Schema.Struct({ status: Schema.Literal("notReady") }),
])

export const DurableApi = HttpApi.make("durable").add(
  HttpApiGroup.make("invocations")
    .add(HttpApiEndpoint.post("call", "/call", { payload: InvokePayload, success: CallSuccess, error: DurableFailure }))
    .add(HttpApiEndpoint.post("send", "/send", { payload: InvokePayload, success: SendSuccess, error: DurableFailure }))
    .add(HttpApiEndpoint.post("attach", "/attach", { payload: LocatePayload, success: CallSuccess, error: DurableFailure }))
    .add(HttpApiEndpoint.post("output", "/output", { payload: LocatePayload, success: OutputResult, error: DurableFailure })),
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

// Resolve the engine execution id from the locate payload: an explicit
// invocationId wins; otherwise reconstruct the id the original idempotencyKey
// minted (a service id IS its idempotencyKey; an object id encodes
// `{object, key, method, nonce: idempotencyKey}` — matching service.ts `mintId`).
const locateId = (def: AnyDef, payload: InvokePayloadType): Effect.Effect<string, DurableFailure> => {
  if (payload.invocationId !== undefined) return Effect.succeed(payload.invocationId)
  if (payload.idempotencyKey === undefined) {
    return Effect.fail(new DurableFailure({ message: "attach/output requires invocationId or idempotencyKey" }))
  }
  if (def.kind === "object") {
    return encodeObjectCallId({ object: payload.name, key: payload.key ?? "", method: payload.method, nonce: payload.idempotencyKey })
      .pipe(Effect.mapError(asFailure))
  }
  return Effect.succeed(payload.idempotencyKey)
}

// Resolve the def + the engine id + the runtime (the shared head of attach/output).
const locate = (registry: Map<string, AnyDef>, payload: InvokePayloadType) =>
  Effect.gen(function*() {
    const resolved = resolve(registry, payload)
    if (resolved === undefined) return yield* notFound(payload)
    const id = yield* locateId(resolved.def, payload)
    const rt = yield* DurableExecutionRuntime
    return { resolved, id, rt }
  })

const runAttach = (registry: Map<string, AnyDef>, payload: InvokePayloadType) =>
  Effect.gen(function*() {
    const { id, resolved, rt } = yield* locate(registry, payload)
    const output = yield* rt.attach(id, resolved.codec.output)
    return { output: yield* Schema.encodeEffect(resolved.codec.output)(output) }
  }).pipe(Effect.mapError(asFailure))

const runOutput = (registry: Map<string, AnyDef>, payload: InvokePayloadType) =>
  Effect.gen(function*() {
    const { id, resolved, rt } = yield* locate(registry, payload)
    const polled = yield* rt.poll(id, resolved.codec.output)
    return Option.isNone(polled)
      ? { status: "notReady" as const }
      : { status: "ready" as const, output: yield* Schema.encodeEffect(resolved.codec.output)(polled.value) }
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
      .handle("attach", ({ payload }) => runAttach(registry, payload))
      .handle("output", ({ payload }) => runOutput(registry, payload)))
  return HttpRouter.serve(HttpApiBuilder.layer(DurableApi).pipe(Layer.provide(InvocationsLive)))
}

// ── ingress client (obtained ONLY from connect — the footgun-free outer path) ──

/** Locate an in-flight invocation by its server-minted id or its idempotency key. */
export type Locator = { readonly invocationId: string } | { readonly idempotencyKey: string }

/** A handle to a sent invocation: its id, a blocking `attach`, and a non-blocking `output`. */
export interface InvocationHandle<O> {
  readonly invocationId: string
  /** Block until the invocation finishes; decode its result (or fail with `DurableFailure`). */
  readonly attach: Effect.Effect<O, DurableFailure>
  /** Non-blocking read: `Option.none()` while still running, `Option.some(result)` once done. */
  readonly output: Effect.Effect<Option.Option<O>, DurableFailure>
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
/** Re-attach (blocking) to an existing invocation by `Locator` — for a caller without the handle. */
export type IngressAttachClient<H extends Handlers> = {
  readonly [K in keyof H]: (locator: Locator) => Effect.Effect<HandlerOutput<H[K]>, DurableFailure>
}
/** Non-blocking output read of an existing invocation by `Locator`. */
export type IngressOutputClient<H extends Handlers> = {
  readonly [K in keyof H]: (locator: Locator) => Effect.Effect<Option.Option<HandlerOutput<H[K]>>, DurableFailure>
}

export interface DurableIngressClient {
  client<Name extends string, H extends Handlers>(def: ServiceDefinition<Name, H>): IngressClient<H>
  client<Name extends string, H extends Handlers>(def: ObjectDefinition<Name, H>, key: string): IngressClient<H>
  sendClient<Name extends string, H extends Handlers>(def: ServiceDefinition<Name, H>): IngressSendClient<H>
  sendClient<Name extends string, H extends Handlers>(def: ObjectDefinition<Name, H>, key: string): IngressSendClient<H>
  attach<Name extends string, H extends Handlers>(def: ServiceDefinition<Name, H>): IngressAttachClient<H>
  attach<Name extends string, H extends Handlers>(def: ObjectDefinition<Name, H>, key: string): IngressAttachClient<H>
  output<Name extends string, H extends Handlers>(def: ServiceDefinition<Name, H>): IngressOutputClient<H>
  output<Name extends string, H extends Handlers>(def: ObjectDefinition<Name, H>, key: string): IngressOutputClient<H>
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

    const locatePayload = (def: AnyDef, key: string | undefined, method: string, locator: Locator) => ({
      name: def.name,
      method,
      ...(key === undefined ? {} : { key }),
      ...("invocationId" in locator ? { invocationId: locator.invocationId } : { idempotencyKey: locator.idempotencyKey }),
    })

    const attachBy = (def: AnyDef, key: string | undefined, method: string, locator: Locator) =>
      Effect.gen(function*() {
        const codec = codecsOf(def, method)
        const result = yield* api.invocations.attach({ payload: locatePayload(def, key, method, locator) }).pipe(Effect.mapError(asFailure))
        return yield* Schema.decodeUnknownEffect(codec.output)((result as { output: unknown }).output).pipe(Effect.orDie)
      })

    const outputBy = (def: AnyDef, key: string | undefined, method: string, locator: Locator) =>
      Effect.gen(function*() {
        const codec = codecsOf(def, method)
        const result = yield* api.invocations.output({ payload: locatePayload(def, key, method, locator) }).pipe(Effect.mapError(asFailure))
        const polled = result as { status: "ready"; output: unknown } | { status: "notReady" }
        if (polled.status === "notReady") return Option.none()
        return Option.some(yield* Schema.decodeUnknownEffect(codec.output)(polled.output).pipe(Effect.orDie))
      })

    const send = (def: AnyDef, key: string | undefined) => (method: string) => (input: unknown, options?: InvokeOptions) =>
      Effect.gen(function*() {
        const codec = codecsOf(def, method)
        const encoded = yield* Schema.encodeEffect(codec.input)(input).pipe(Effect.orDie)
        const result = yield* api.invocations.send({ payload: payloadFor(def, key, method, encoded, options) }).pipe(Effect.mapError(asFailure))
        const invocationId = (result as { invocationId: string }).invocationId
        const locator: Locator = { invocationId }
        return { invocationId, attach: attachBy(def, key, method, locator), output: outputBy(def, key, method, locator) }
      })

    const proxy = (build: (method: string) => (...args: ReadonlyArray<any>) => Effect.Effect<unknown, DurableFailure>) =>
      new Proxy({}, { get: (_t, method: string) => build(method) }) as any

    return {
      client: (def: AnyDef, key?: string) => proxy(call(def, key)),
      sendClient: (def: AnyDef, key?: string) => proxy(send(def, key)),
      attach: (def: AnyDef, key?: string) => proxy((method: string) => (locator: Locator) => attachBy(def, key, method, locator)),
      output: (def: AnyDef, key?: string) => proxy((method: string) => (locator: Locator) => outputBy(def, key, method, locator)),
    } as DurableIngressClient
  })
