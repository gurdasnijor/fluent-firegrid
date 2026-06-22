/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unnecessary-type-assertion -- the server dispatches over heterogeneous definitions by name/method at runtime (the same reason service.ts disables no-explicit-any), and runtime correctness is covered by the S2-backed ingress test. */
import { Effect, Layer, Option, Schema } from "effect"
import { HttpRouter } from "effect/unstable/http"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { encodeObjectCallId } from "../actor/core.ts"
import { client, sendClient } from "../service.ts"
import type { InvokeOptions, ObjectDefinition, ServiceDefinition } from "../service.ts"
import { DurableExecutionRuntime } from "../Runtime.ts"
import { type AnyDef, asFailure, DurableApi, DurableFailure, type HandlerCodecs } from "./contract.ts"

// ── definitions registry + helpers ──────────────────────────────────────────

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

const validateTarget = (def: AnyDef, payload: InvokePayloadType): Effect.Effect<void, DurableFailure> => {
  if (def.kind === "object" && payload.key === undefined) {
    return Effect.fail(new DurableFailure({ message: `object ${payload.name}/${payload.method} requires a key` }))
  }
  if (def.kind === "service" && payload.key !== undefined) {
    return Effect.fail(new DurableFailure({ message: `service ${payload.name}/${payload.method} must not include a key` }))
  }
  return Effect.void
}

const optionsFor = (payload: InvokePayloadType): InvokeOptions | undefined =>
  payload.idempotencyKey === undefined ? undefined : { idempotencyKey: payload.idempotencyKey }

const notFound = (payload: InvokePayloadType) =>
  new DurableFailure({ message: `no handler ${payload.name}/${payload.method}` })

// ── server handlers ─────────────────────────────────────────────────────────
// The ingress sits OUTSIDE any handler (no ActiveInvocation), so `client`/
// `sendClient` are the correct surfaces; decode the wire input via the def's
// codec, drive the engine, re-encode the output.

// Resolve the def for a payload and enforce the service/object key contract
// (the shared head of every server handler).
const resolveValidated = (registry: Map<string, AnyDef>, payload: InvokePayloadType) =>
  Effect.gen(function*() {
    const resolved = resolve(registry, payload)
    if (resolved === undefined) return yield* notFound(payload)
    yield* validateTarget(resolved.def, payload)
    return resolved
  })

// Resolve the def + decode the wire input (the shared head of call/send).
const prepare = (registry: Map<string, AnyDef>, payload: InvokePayloadType) =>
  Effect.gen(function*() {
    const resolved = yield* resolveValidated(registry, payload)
    const input = yield* Schema.decodeUnknownEffect(resolved.codec.input)(payload.input)
    return { resolved, input }
  })

const callProxy = (def: AnyDef, key: string | undefined): Record<string, any> =>
  (def.kind === "object" ? client(def as ObjectDefinition<string, any>, key as string) : client(def as ServiceDefinition<string, any>)) as Record<string, any>

const sendProxy = (def: AnyDef, key: string | undefined): Record<string, any> =>
  (def.kind === "object" ? sendClient(def as ObjectDefinition<string, any>, key as string) : sendClient(def as ServiceDefinition<string, any>)) as Record<string, any>

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
    return encodeObjectCallId({ object: payload.name, key: payload.key as string, method: payload.method, nonce: payload.idempotencyKey })
      .pipe(Effect.mapError(asFailure))
  }
  return Effect.succeed(payload.idempotencyKey)
}

// Resolve the def + the engine id + the runtime (the shared head of attach/output).
const locate = (registry: Map<string, AnyDef>, payload: InvokePayloadType) =>
  Effect.gen(function*() {
    const resolved = yield* resolveValidated(registry, payload)
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
  const servicePath = (path: { readonly name: string; readonly method: string }, payload: { readonly input?: unknown; readonly idempotencyKey?: string }): InvokePayloadType => ({
    name: path.name,
    method: path.method,
    input: payload.input,
    ...(payload.idempotencyKey === undefined ? {} : { idempotencyKey: payload.idempotencyKey }),
  })
  const objectPath = (
    path: { readonly name: string; readonly key: string; readonly method: string },
    payload: { readonly input?: unknown; readonly idempotencyKey?: string },
  ): InvokePayloadType => ({
    name: path.name,
    key: path.key,
    method: path.method,
    input: payload.input,
    ...(payload.idempotencyKey === undefined ? {} : { idempotencyKey: payload.idempotencyKey }),
  })
  const serviceLocatePath = (
    path: { readonly name: string; readonly method: string },
    payload: { readonly invocationId?: string; readonly idempotencyKey?: string },
  ): InvokePayloadType => ({ name: path.name, method: path.method, ...payload })
  const objectLocatePath = (
    path: { readonly name: string; readonly key: string; readonly method: string },
    payload: { readonly invocationId?: string; readonly idempotencyKey?: string },
  ): InvokePayloadType => ({ name: path.name, key: path.key, method: path.method, ...payload })
  const InvocationsLive = HttpApiBuilder.group(DurableApi, "invocations", (handlers) =>
    handlers
      .handle("serviceCall", ({ params, payload }) => runCall(registry, servicePath(params, payload)))
      .handle("objectCall", ({ params, payload }) => runCall(registry, objectPath(params, payload)))
      .handle("serviceSend", ({ params, payload }) => runSend(registry, servicePath(params, payload)))
      .handle("objectSend", ({ params, payload }) => runSend(registry, objectPath(params, payload)))
      .handle("serviceAttach", ({ params, payload }) => runAttach(registry, serviceLocatePath(params, payload)))
      .handle("objectAttach", ({ params, payload }) => runAttach(registry, objectLocatePath(params, payload)))
      .handle("serviceOutput", ({ params, payload }) => runOutput(registry, serviceLocatePath(params, payload)))
      .handle("objectOutput", ({ params, payload }) => runOutput(registry, objectLocatePath(params, payload))))
  return HttpRouter.serve(HttpApiBuilder.layer(DurableApi).pipe(Layer.provide(InvocationsLive)))
}
