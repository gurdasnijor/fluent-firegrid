// @effect-diagnostics effect/nodeBuiltinImport:off effect/multipleEffectProvide:off -- an edge integration test: node:http creates the server NodeHttpServer.layer adapts, node:child_process probes for the `s2` binary, and the chained provides are the intentional edge wiring (HttpClient → ingress → engine → server → S2).
import { execSync } from "node:child_process"
import { createServer } from "node:http"
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem"
import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient"
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer"
import * as NodePath from "@effect/platform-node/NodePath"
import { Effect, Layer, Option, Schema } from "effect"
import { HttpServer } from "effect/unstable/http"
import { connect, durableIngress, object, run, service, serviceLayer, state } from "../src/index.ts"
import { primaryKey, Table } from "effect-s2-stream-db"
import { describe, expect, it } from "vitest"
import { S2LiteLive } from "./s2lite.ts"

// Proves the durable HTTP ingress end to end: a real Node HTTP server serves the
// durable definitions; an out-of-process-style client connects over HTTP and
// drives a service AND a keyed object, each invocation journaled to (s2 lite) S2.

const Calculator = service({
  name: "ingress-calculator",
  handlers: {
    *double(input: number) {
      return yield* run("double", Effect.succeed(input * 2), { output: Schema.Number })
    },
  },
  schemas: { double: { input: Schema.Number, output: Schema.Number } },
})

class CounterRow extends Table<CounterRow>("ingress-counter")({
  id: Schema.String.pipe(primaryKey),
  value: Schema.Number,
}) {}

const Counter = object({
  name: "ingress-counter-object",
  handlers: {
    *add(amount: number) {
      const current = yield* state(CounterRow).get("v")
      const next = (current._tag === "Some" ? current.value.value : 0) + amount
      yield* state(CounterRow).set({ id: "v", value: next })
      return next
    },
  },
  schemas: { add: { input: Schema.Number, output: Schema.Number } },
})

const hasS2 = (): boolean => {
  try {
    execSync("command -v s2", { stdio: "ignore" })
    return true
  } catch {
    return false
  }
}

const EngineLive = serviceLayer(Calculator, Counter)

describe.skipIf(!hasS2())("durable ingress over HTTP (S2 + node http)", () => {
  it("connect({ url }) drives a service and a keyed object over real HTTP", async () => {
    const program = Effect.gen(function*() {
      const server = yield* HttpServer.HttpServer
      const port = server.address._tag === "TcpAddress" ? server.address.port : 0
      const ingress = yield* connect({ url: `http://127.0.0.1:${port}` })
      // request-response over HTTP, for a service and a keyed object
      const doubled = yield* ingress.serviceClient(Calculator).double(21)
      const added = yield* ingress.objectClient(Counter, "cart").add(5)
      // fire-and-forget: send returns an awaitable handle (Restate's Send → attach)
      const handle = yield* ingress.serviceSendClient(Calculator).double(10)
      const attached = yield* handle.attach
      // non-blocking output: once attached, the handle's output is ready
      const polled = yield* handle.output
      return { doubled, added, invocationIdPresent: handle.invocationId.length > 0, attached, polled: Option.getOrNull(polled) }
    })

    const result = await program.pipe(
      Effect.provide(NodeHttpClient.layerUndici),
      Effect.provide(durableIngress([Calculator, Counter])),
      Effect.provide(EngineLive),
      Effect.provide(NodeHttpServer.layer(() => createServer(), { port: 0 })),
      Effect.provide(Layer.mergeAll(NodeFileSystem.layer, NodePath.layer)),
      Effect.provide(S2LiteLive),
      Effect.scoped,
      Effect.runPromise,
    )

    expect(result).toEqual({ doubled: 42, added: 5, invocationIdPresent: true, attached: 20, polled: 20 })
  }, 60_000)

  it("attaches and polls an existing invocation by idempotency key (no original handle)", async () => {
    const program = Effect.gen(function*() {
      const server = yield* HttpServer.HttpServer
      const port = server.address._tag === "TcpAddress" ? server.address.port : 0
      const ingress = yield* connect({ url: `http://127.0.0.1:${port}` })
      // a "first caller" sends a keyed-object invocation pinned to an idempotency key
      const idempotencyKey = "ingress-idem-1"
      yield* ingress.objectSendClient(Counter, "wishlist").add(7, { idempotencyKey })
      // a SECOND caller — holding only (def, key, method, idempotencyKey), not the
      // server-minted id — re-attaches to the same invocation and reads its result
      const attached = yield* ingress.objectAttachClient(Counter, "wishlist").add({ idempotencyKey })
      const polled = yield* ingress.objectOutputClient(Counter, "wishlist").add({ idempotencyKey })
      return { attached, polled: Option.getOrNull(polled) }
    })

    const result = await program.pipe(
      Effect.provide(NodeHttpClient.layerUndici),
      Effect.provide(durableIngress([Calculator, Counter])),
      Effect.provide(EngineLive),
      Effect.provide(NodeHttpServer.layer(() => createServer(), { port: 0 })),
      Effect.provide(Layer.mergeAll(NodeFileSystem.layer, NodePath.layer)),
      Effect.provide(S2LiteLive),
      Effect.scoped,
      Effect.runPromise,
    )

    expect(result).toEqual({ attached: 7, polled: 7 })
  }, 60_000)
})
