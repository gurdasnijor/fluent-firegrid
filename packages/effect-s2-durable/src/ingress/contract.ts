/* eslint-disable @typescript-eslint/no-explicit-any -- the ingress dispatches over heterogeneous definitions by name/method at runtime (the same reason service.ts disables no-explicit-any); the wire codecs are existential at this boundary. */
import { Schema } from "effect"
import { HttpApi, HttpApiEndpoint, HttpApiGroup } from "effect/unstable/httpapi"
import type { ObjectDefinition, ServiceDefinition } from "../service.ts"

/**
 * The HTTP **ingress** wire contract for durable definitions — the shared
 * foundation of the out-of-process client (`./client.ts`, `connect`) and the
 * server (`./server.ts`, `durableIngress`). The analog of Restate's ingress: a
 * caller invokes a service/object handler over HTTP rather than the embedded
 * engine.
 *
 * This module is engine-free — only `effect` Schema + `httpapi` and the
 * definition *types*. The routes are generic (service name, optional key, and
 * method are path segments); the per-method typing is recovered on the client
 * from each definition's codecs. `cancel`/`kill`/`status` are deliberately
 * absent (admin-plane in Restate, not ingress).
 */

export type AnyCodec = Schema.Codec<any, any, never, never>
export type HandlerCodecs = { readonly input: AnyCodec; readonly output: AnyCodec }
export type AnyDef = ServiceDefinition<string, any> | ObjectDefinition<string, any>

/** A typed invocation failure surfaced to the ingress client (not a 500 defect). */
export class DurableFailure extends Schema.ErrorClass<DurableFailure>("DurableFailure")({
  message: Schema.String,
}) {}

// ── the wire API (Restate-shaped routes; per-def typing is layered on the client) ──

const InvokePayload = Schema.Struct({
  input: Schema.Unknown,
  idempotencyKey: Schema.optionalKey(Schema.String),
})

const ServiceParams = {
  name: Schema.String,
  method: Schema.String,
}

const ObjectParams = {
  name: Schema.String,
  key: Schema.String,
  method: Schema.String,
}

// Locate an existing invocation by EITHER its server-minted id OR the
// idempotency key the original send used (the server reconstructs the id).
const LocatePayload = Schema.Struct({
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
    .add(HttpApiEndpoint.post("serviceCall", "/durable/call/:name/:method", {
      params: ServiceParams,
      payload: InvokePayload,
      success: CallSuccess,
      error: DurableFailure,
    }))
    .add(HttpApiEndpoint.post("objectCall", "/durable/call/:name/:key/:method", {
      params: ObjectParams,
      payload: InvokePayload,
      success: CallSuccess,
      error: DurableFailure,
    }))
    .add(HttpApiEndpoint.post("serviceSend", "/durable/send/:name/:method", {
      params: ServiceParams,
      payload: InvokePayload,
      success: SendSuccess,
      error: DurableFailure,
    }))
    .add(HttpApiEndpoint.post("objectSend", "/durable/send/:name/:key/:method", {
      params: ObjectParams,
      payload: InvokePayload,
      success: SendSuccess,
      error: DurableFailure,
    }))
    .add(HttpApiEndpoint.post("serviceAttach", "/durable/attach/:name/:method", {
      params: ServiceParams,
      payload: LocatePayload,
      success: CallSuccess,
      error: DurableFailure,
    }))
    .add(HttpApiEndpoint.post("objectAttach", "/durable/attach/:name/:key/:method", {
      params: ObjectParams,
      payload: LocatePayload,
      success: CallSuccess,
      error: DurableFailure,
    }))
    .add(HttpApiEndpoint.post("serviceOutput", "/durable/output/:name/:method", {
      params: ServiceParams,
      payload: LocatePayload,
      success: OutputResult,
      error: DurableFailure,
    }))
    .add(HttpApiEndpoint.post("objectOutput", "/durable/output/:name/:key/:method", {
      params: ObjectParams,
      payload: LocatePayload,
      success: OutputResult,
      error: DurableFailure,
    })),
)

const messageOf = (cause: unknown): string => {
  if (typeof cause === "object" && cause !== null && "message" in cause) {
    const message = (cause as { readonly message: unknown }).message
    if (typeof message === "string") return message
  }
  return String(cause)
}

// Map any engine/codec failure into the typed DurableFailure channel (defects stay defects).
const isDurableFailure = Schema.is(DurableFailure)
export const asFailure = (cause: unknown): DurableFailure =>
  isDurableFailure(cause) ? cause : new DurableFailure({ message: messageOf(cause) })
