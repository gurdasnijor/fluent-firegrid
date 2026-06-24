import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as HttpRouter from "effect/unstable/http/HttpRouter"
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder"
import { compileOne } from "../catalog/compiler.ts"
import { DurableEngine } from "../engine/api.ts"
import { type InvokeOptions, locateInvocationId, objectIdentity, planInvocationId } from "../invocation/plan.ts"
import { type AnyDef, asFailure, DurableApi, DurableFailure } from "./contract.ts"

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
  const codecs = def.codecs[payload.method]
  const compiled = compileOne(def, payload.method)
  if (compiled === undefined) return undefined
  if (codecs === undefined) return undefined
  return { def, codecs, compiled }
}

const validateTarget = (def: AnyDef, payload: InvokePayloadType): Effect.Effect<void, DurableFailure> => {
  if (def.kind === "object" && payload.key === undefined) {
    return Effect.fail(new DurableFailure({ message: `object ${payload.name}/${payload.method} requires a key` }))
  }
  if (def.kind === "service" && payload.key !== undefined) {
    return Effect.fail(
      new DurableFailure({ message: `service ${payload.name}/${payload.method} must not include a key` })
    )
  }
  return Effect.void
}

const optionsFor = (payload: InvokePayloadType): InvokeOptions | undefined =>
  payload.idempotencyKey === undefined ? undefined : { idempotencyKey: payload.idempotencyKey }

const notFound = (payload: InvokePayloadType) =>
  new DurableFailure({ message: `no handler ${payload.name}/${payload.method}` })

// ── server handlers ─────────────────────────────────────────────────────────
// The ingress sits outside handler replay. It plans an invocation id, submits
// directly to the engine, then attaches/polls via the same typed codecs.

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
    const input = yield* Schema.decodeUnknownEffect(resolved.codecs.input)(payload.input)
    return { resolved, input }
  })

const runCall = (registry: Map<string, AnyDef>, payload: InvokePayloadType) =>
  Effect.gen(function*() {
    const { input, resolved } = yield* prepare(registry, payload)
    const rt = yield* DurableEngine
    const id = yield* planInvocationId(payload.method, optionsFor(payload), objectIdentity(resolved.def, payload.key))
    yield* rt.submit(resolved.compiled.handler, id, input)
    const decoded = yield* rt.attach(id, resolved.codecs.output)
    return { output: yield* Schema.encodeEffect(resolved.codecs.output)(decoded) }
  }).pipe(Effect.mapError(asFailure))

const runSend = (registry: Map<string, AnyDef>, payload: InvokePayloadType) =>
  Effect.gen(function*() {
    const { input, resolved } = yield* prepare(registry, payload)
    const rt = yield* DurableEngine
    const invocationId = yield* planInvocationId(
      payload.method,
      optionsFor(payload),
      objectIdentity(resolved.def, payload.key)
    )
    yield* rt.submit(resolved.compiled.handler, invocationId, input)
    return { invocationId }
  }).pipe(Effect.mapError(asFailure))

// Resolve the engine execution id from the locate payload: an explicit
// invocationId wins; otherwise reconstruct the id the original idempotencyKey minted.
const locateId = (def: AnyDef, payload: InvokePayloadType): Effect.Effect<string, DurableFailure> => {
  if (payload.invocationId !== undefined) return Effect.succeed(payload.invocationId)
  if (payload.idempotencyKey === undefined) {
    return Effect.fail(new DurableFailure({ message: "attach/output requires invocationId or idempotencyKey" }))
  }
  return locateInvocationId(payload.method, payload.idempotencyKey, objectIdentity(def, payload.key)).pipe(
    Effect.mapError(asFailure)
  )
}

// Resolve the def + the engine id + the engine (the shared head of attach/output).
const locate = (registry: Map<string, AnyDef>, payload: InvokePayloadType) =>
  Effect.gen(function*() {
    const resolved = yield* resolveValidated(registry, payload)
    const id = yield* locateId(resolved.def, payload)
    const rt = yield* DurableEngine
    return { resolved, id, rt }
  })

const runAttach = (registry: Map<string, AnyDef>, payload: InvokePayloadType) =>
  Effect.gen(function*() {
    const { id, resolved, rt } = yield* locate(registry, payload)
    const output = yield* rt.attach(id, resolved.codecs.output)
    return { output: yield* Schema.encodeEffect(resolved.codecs.output)(output) }
  }).pipe(Effect.mapError(asFailure))

const runOutput = (registry: Map<string, AnyDef>, payload: InvokePayloadType) =>
  Effect.gen(function*() {
    const { id, resolved, rt } = yield* locate(registry, payload)
    const polled = yield* rt.poll(id, resolved.codecs.output)
    return Option.isNone(polled)
      ? { status: "notReady" as const }
      : { status: "ready" as const, output: yield* Schema.encodeEffect(resolved.codecs.output)(polled.value) }
  }).pipe(Effect.mapError(asFailure))

/**
 * The ingress server layer for a set of definitions. Requires `HttpServer`
 * (supplied at the edge by `NodeHttpServer.layer`) and `DurableEngine`
 * (the engine, from `serviceLayer(...)` over the same definitions).
 */
export const durableIngress = (defs: ReadonlyArray<AnyDef>) => {
  const registry = new Map(defs.map((def) => [def.name, def] as const))
  const servicePath = (
    path: { readonly name: string; readonly method: string },
    payload: { readonly input?: unknown; readonly idempotencyKey?: string }
  ): InvokePayloadType => ({
    name: path.name,
    method: path.method,
    input: payload.input,
    ...(payload.idempotencyKey === undefined ? {} : { idempotencyKey: payload.idempotencyKey })
  })
  const objectPath = (
    path: { readonly name: string; readonly key: string; readonly method: string },
    payload: { readonly input?: unknown; readonly idempotencyKey?: string }
  ): InvokePayloadType => ({
    name: path.name,
    key: path.key,
    method: path.method,
    input: payload.input,
    ...(payload.idempotencyKey === undefined ? {} : { idempotencyKey: payload.idempotencyKey })
  })
  const serviceLocatePath = (
    path: { readonly name: string; readonly method: string },
    payload: { readonly invocationId?: string; readonly idempotencyKey?: string }
  ): InvokePayloadType => ({ name: path.name, method: path.method, ...payload })
  const objectLocatePath = (
    path: { readonly name: string; readonly key: string; readonly method: string },
    payload: { readonly invocationId?: string; readonly idempotencyKey?: string }
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
  return DurableApi.pipe(HttpApiBuilder.layer, Layer.provide(InvocationsLive), HttpRouter.serve)
}
