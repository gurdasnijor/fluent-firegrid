import {
  type CallRequest,
  type InvocationBinding,
  run,
  schemas,
  type SendReference,
  type SendRequest,
  service
} from "@firegrid/fluent-firegrid"
import { Effect, Schema } from "effect"
import { afterEach, describe, expect, it } from "vitest"

import {
  createFluentS2NodeRuntime,
  createFluentWebhookHandler,
  type FluentNodeHttpServer,
  listenFluentHttp
} from "../src/index.ts"
import { createFluentHttpHandler } from "@firegrid/fluent-firegrid-http"

let currentServer: FluentNodeHttpServer | undefined

afterEach(async () => {
  await currentServer?.close()
  currentServer = undefined
})

describe("fluent-firegrid-node", () => {
  it("serves health, readiness, call, and send routes over Node HTTP", async () => {
    const incident = service({
      name: "incident",
      handlers: {
        *triage(input: string) {
          return yield* run(() => `triaged:${input}`, { name: "triage" })
        }
      },
      descriptors: {
        triage: schemas({
          input: Schema.String,
          output: Schema.String
        })
      }
    })
    const calls = new Array<CallRequest>()
    const binding: InvocationBinding<never> = {
      call: <Output>(request: CallRequest) =>
        Effect.sync(() => {
          calls.push(request)
          return `called:${String(request.input)}` as Output
        }),
      send: <Output>(request: SendRequest) =>
        Effect.succeed(
          {
            invocationId: `send:${request.name}:${request.handler}`
          } satisfies SendReference<Output>
        )
    }
    currentServer = await listenFluentHttp({
      handler: createFluentHttpHandler({ binding, definitions: [incident] }),
      ready: () => true
    })

    const health = await fetch(`${currentServer.url}/health`)
    const ready = await fetch(`${currentServer.url}/ready`)
    const call = await fetch(`${currentServer.url}/call/service/incident/triage?runId=run-1`, {
      body: JSON.stringify("INC-1"),
      headers: { "content-type": "application/json" },
      method: "POST"
    })
    const send = await fetch(`${currentServer.url}/send/service/incident/triage`, {
      body: JSON.stringify("INC-2"),
      headers: { "content-type": "application/json" },
      method: "POST"
    })

    expect(health.status).toBe(200)
    expect(await health.json()).toEqual({ ok: true })
    expect(ready.status).toBe(200)
    expect(await ready.json()).toEqual({ ok: true })
    expect(call.status).toBe(200)
    expect(await call.json()).toEqual({ output: "called:INC-1" })
    expect(send.status).toBe(202)
    expect(await send.json()).toEqual({ invocationId: "send:incident:triage" })
    expect(calls[0]).toMatchObject({
      handler: "triage",
      input: "INC-1",
      kind: "service",
      name: "incident",
      runId: "run-1"
    })
  })

  it("mounts webhook routes with verification and idempotency keys", async () => {
    const stripeWebhook = service({
      name: "stripe-webhook",
      handlers: {
        *onEvent(input: { readonly id: string }) {
          return yield* run(() => ({ accepted: input.id.length > 0 }), { name: "accept-event" })
        }
      },
      descriptors: {
        onEvent: schemas({
          input: Schema.Struct({ id: Schema.String }),
          output: Schema.Struct({ accepted: Schema.Boolean })
        })
      }
    })
    const calls = new Array<CallRequest>()
    const binding: InvocationBinding<never> = {
      call: <Output>(request: CallRequest) =>
        Effect.sync(() => {
          calls.push(request)
          return { accepted: true } as Output
        }),
      send: <Output>() => Effect.succeed({ invocationId: "not-used" } satisfies SendReference<Output>)
    }
    currentServer = await listenFluentHttp({
      handler: createFluentWebhookHandler(
        createFluentHttpHandler({ binding, definitions: [stripeWebhook] }),
        {
          "/webhooks/stripe": {
            definition: stripeWebhook,
            handler: "onEvent",
            idempotencyKey: (request) => request.headers.get("stripe-event-id") ?? undefined,
            verify: (_request, body) => new TextDecoder().decode(body).includes("evt_1")
          }
        }
      )
    })

    const accepted = await fetch(`${currentServer.url}/webhooks/stripe`, {
      body: JSON.stringify({ id: "evt_1" }),
      headers: {
        "content-type": "application/json",
        "stripe-event-id": "stripe:evt_1"
      },
      method: "POST"
    })
    const rejected = await fetch(`${currentServer.url}/webhooks/stripe`, {
      body: JSON.stringify({ id: "evt_2" }),
      headers: {
        "content-type": "application/json",
        "stripe-event-id": "stripe:evt_2"
      },
      method: "POST"
    })

    expect(accepted.status).toBe(200)
    expect(await accepted.json()).toEqual({ output: { accepted: true } })
    expect(rejected.status).toBe(401)
    expect(await rejected.json()).toEqual({ error: "webhook_verification_failed" })
    expect(calls).toHaveLength(1)
    expect(calls[0]).toMatchObject({
      handler: "onEvent",
      input: { id: "evt_1" },
      kind: "service",
      name: "stripe-webhook",
      runId: "stripe:evt_1"
    })
  })

  it("constructs the S2-backed fluent runtime without starting an HTTP listener", () => {
    const incident = service({
      name: "incident",
      handlers: {
        *triage(input: string) {
          return yield* run(() => input, { name: "triage" })
        }
      }
    })

    const runtime = createFluentS2NodeRuntime({
      definitions: [incident],
      namespace: "node-test",
      s2Endpoint: "http://127.0.0.1:1"
    })

    expect(typeof runtime.handler).toBe("function")
    expect(typeof runtime.binding.call).toBe("function")
    expect(typeof runtime.host.runLoop).toBe("function")
  })
})
