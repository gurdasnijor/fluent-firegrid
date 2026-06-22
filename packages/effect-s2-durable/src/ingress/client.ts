/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unnecessary-type-assertion -- the client materializes per-method surfaces over heterogeneous definitions at runtime (the same reason service.ts disables no-explicit-any); the typed surface is recovered from each definition's codecs and proven by the S2-backed ingress test. */
import { Effect, Option, Schema } from "effect"
import { HttpClient, HttpClientRequest } from "effect/unstable/http"
import { HttpApiClient } from "effect/unstable/httpapi"
import type { Handlers, HandlerInput, HandlerOutput, InvokeOptions, ObjectDefinition, ServiceDefinition } from "../service.ts"
import { type AnyDef, asFailure, DurableApi, type DurableFailure, type HandlerCodecs } from "./contract.ts"

// The ingress client depends only on the wire contract + the definition *types*
// (and the def's runtime codecs) — never on the engine runtime — so it can be
// consumed out of process via the `effect-s2-durable/client` subpath.
export { DurableFailure } from "./contract.ts"

type IngressMethod = (...args: ReadonlyArray<any>) => Effect.Effect<unknown, DurableFailure, unknown>

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
  serviceClient<Name extends string, H extends Handlers>(def: ServiceDefinition<Name, H>): IngressClient<H>
  objectClient<Name extends string, H extends Handlers>(def: ObjectDefinition<Name, H>, key: string): IngressClient<H>
  serviceSendClient<Name extends string, H extends Handlers>(def: ServiceDefinition<Name, H>): IngressSendClient<H>
  objectSendClient<Name extends string, H extends Handlers>(def: ObjectDefinition<Name, H>, key: string): IngressSendClient<H>
  serviceAttachClient<Name extends string, H extends Handlers>(def: ServiceDefinition<Name, H>): IngressAttachClient<H>
  objectAttachClient<Name extends string, H extends Handlers>(def: ObjectDefinition<Name, H>, key: string): IngressAttachClient<H>
  serviceOutputClient<Name extends string, H extends Handlers>(def: ServiceDefinition<Name, H>): IngressOutputClient<H>
  objectOutputClient<Name extends string, H extends Handlers>(def: ObjectDefinition<Name, H>, key: string): IngressOutputClient<H>
}

const codecsOf = (def: AnyDef, method: string): HandlerCodecs => def.compiled[method]!.handler as unknown as HandlerCodecs

const payloadFor = (encoded: unknown, options?: InvokeOptions) => ({
  input: encoded,
  ...(options?.idempotencyKey === undefined ? {} : { idempotencyKey: options.idempotencyKey }),
})

export interface ConnectOptions {
  readonly url: string
}

/**
 * Connect to a durable ingress at `url`. Returns typed service/object client
 * surfaces keyed by the SAME definition the server serves; input/output are
 * encoded/decoded via the definition's codecs. Requires an `HttpClient`
 * (supplied at the edge by `NodeHttpClient.layer`).
 */
export const connect = (
  options: ConnectOptions,
): Effect.Effect<DurableIngressClient, never, HttpClient.HttpClient> =>
  Effect.gen(function*() {
    const api = yield* HttpApiClient.make(DurableApi, {
      transformClient: (httpClient) => HttpClient.mapRequest(httpClient, HttpClientRequest.prependUrl(options.url)),
    }).pipe(Effect.orDie)

    // Pick the object-vs-service endpoint pair and build its params from the def.
    // (The wire is generic over (name, key?, method); the typed surface below is
    // recovered per-method from the def's codecs — see contract.ts.)
    const invokeEndpoint = (
      endpoints: { object: (req: any) => Effect.Effect<any, any>; service: (req: any) => Effect.Effect<any, any> },
      def: AnyDef,
      key: string | undefined,
      method: string,
      payload: unknown,
    ): Effect.Effect<unknown, DurableFailure> =>
      (def.kind === "object"
        ? endpoints.object({ params: { name: def.name, key: key as string, method }, payload })
        : endpoints.service({ params: { name: def.name, method }, payload })).pipe(Effect.mapError(asFailure))

    const call = (def: AnyDef, key: string | undefined) => (method: string) => (input: unknown, invokeOptions?: InvokeOptions) =>
      Effect.gen(function*() {
        const codec = codecsOf(def, method)
        const encoded = yield* Schema.encodeEffect(codec.input)(input).pipe(Effect.orDie)
        const result = yield* invokeEndpoint({ object: api.invocations.objectCall!, service: api.invocations.serviceCall! }, def, key, method, payloadFor(encoded, invokeOptions))
        return yield* Schema.decodeUnknownEffect(codec.output)((result as { output: unknown }).output).pipe(Effect.orDie)
      })

    const locatePayload = (locator: Locator) =>
      "invocationId" in locator ? { invocationId: locator.invocationId } : { idempotencyKey: locator.idempotencyKey }

    const attachBy = (def: AnyDef, key: string | undefined, method: string, locator: Locator) =>
      Effect.gen(function*() {
        const codec = codecsOf(def, method)
        const result = yield* invokeEndpoint({ object: api.invocations.objectAttach!, service: api.invocations.serviceAttach! }, def, key, method, locatePayload(locator))
        return yield* Schema.decodeUnknownEffect(codec.output)((result as { output: unknown }).output).pipe(Effect.orDie)
      })

    const outputBy = (def: AnyDef, key: string | undefined, method: string, locator: Locator) =>
      Effect.gen(function*() {
        const codec = codecsOf(def, method)
        const result = yield* invokeEndpoint({ object: api.invocations.objectOutput!, service: api.invocations.serviceOutput! }, def, key, method, locatePayload(locator))
        const polled = result as { status: "ready"; output: unknown } | { status: "notReady" }
        if (polled.status === "notReady") return Option.none()
        return Option.some(yield* Schema.decodeUnknownEffect(codec.output)(polled.output).pipe(Effect.orDie))
      })

    const send = (def: AnyDef, key: string | undefined) => (method: string) => (input: unknown, invokeOptions?: InvokeOptions) =>
      Effect.gen(function*() {
        const codec = codecsOf(def, method)
        const encoded = yield* Schema.encodeEffect(codec.input)(input).pipe(Effect.orDie)
        const result = yield* invokeEndpoint({ object: api.invocations.objectSend!, service: api.invocations.serviceSend! }, def, key, method, payloadFor(encoded, invokeOptions))
        const invocationId = (result as { invocationId: string }).invocationId
        const locator: Locator = { invocationId }
        return { invocationId, attach: attachBy(def, key, method, locator), output: outputBy(def, key, method, locator) }
      })

    const materializeClient = (def: AnyDef, build: (method: string) => IngressMethod): Record<string, IngressMethod> =>
      Object.fromEntries(Object.keys(def.compiled).map((method) => [method, build(method)]))

    return {
      serviceClient: (def: AnyDef) => materializeClient(def, call(def, undefined)),
      objectClient: (def: AnyDef, key: string) => materializeClient(def, call(def, key)),
      serviceSendClient: (def: AnyDef) => materializeClient(def, send(def, undefined)),
      objectSendClient: (def: AnyDef, key: string) => materializeClient(def, send(def, key)),
      serviceAttachClient: (def: AnyDef) => materializeClient(def, (method: string) => (locator: Locator) => attachBy(def, undefined, method, locator)),
      objectAttachClient: (def: AnyDef, key: string) => materializeClient(def, (method: string) => (locator: Locator) => attachBy(def, key, method, locator)),
      serviceOutputClient: (def: AnyDef) => materializeClient(def, (method: string) => (locator: Locator) => outputBy(def, undefined, method, locator)),
      objectOutputClient: (def: AnyDef, key: string) => materializeClient(def, (method: string) => (locator: Locator) => outputBy(def, key, method, locator)),
    } as DurableIngressClient
  })
