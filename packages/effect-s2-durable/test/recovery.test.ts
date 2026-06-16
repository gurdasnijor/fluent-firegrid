import { expect, layer } from "@effect/vitest"
import { Duration, Effect, Schema } from "effect"
import { attach, awakeable, resolveAwakeable, resolveSignal, sendClient, service, serviceLayer, signal } from "../src/index.ts"
import { S2LiteLive } from "./s2lite.ts"

const Approval = Schema.Struct({ approved: Schema.Boolean })

// Recovery lives in its own file (separate vitest worker = isolated `process.env`,
// since `s2lite` configures the SDK via env). Crucially there is NO long-lived
// ambient engine here: each test stands a fresh engine over the shared s2 server
// and tears it down, so a "restart" is just a second engine over the same streams
// — and only one engine (one roster StreamDb) is ever live at a time.
layer(S2LiteLive, { excludeTestServices: true, timeout: Duration.seconds(40) })(
  "effect-s2-durable boot recovery over s2 lite",
  (it) => {
    it.effect("recovers a signal-parked execution across an engine restart", () =>
      Effect.gen(function*() {
        const svc = service({
          name: "recover-signal",
          handlers: {
            *approve(_req: { x: string }) {
              return (yield* signal("approval", Approval)).approved
            },
          },
        })
        // `serviceLayer` seeds the registry so a fresh engine can re-drive by name.
        const engine = serviceLayer(svc)

        // process 1: submit, let it park on the signal, then tear the engine down.
        // The S2 streams (genesis + roster row) outlive this scope.
        const id = yield* sendClient(svc).approve({ x: "q" }).pipe(Effect.provide(engine), Effect.scoped)

        // process 2: a fresh engine over the SAME s2 boot-recovers the execution —
        // it re-runs from the top, re-parks on the signal, is resident again, and so
        // an ingress resolution + attach work exactly as for a fresh submission.
        const approved = yield* Effect.gen(function*() {
          yield* resolveSignal(id, "approval", Approval, { approved: true })
          return yield* attach(id, Schema.Boolean)
        }).pipe(Effect.provide(engine), Effect.scoped)

        expect(approved).toBe(true)
      }))

    it.effect("recovers an awakeable-parked execution and resolves it by its replay-stable id", () =>
      Effect.gen(function*() {
        const svc = service({
          name: "recover-awakeable",
          handlers: {
            *go(_req: { x: string }) {
              const awk = yield* awakeable(Approval)
              return (yield* awk.promise).approved
            },
          },
        })
        const engine = serviceLayer(svc)

        const id = yield* sendClient(svc).go({ x: "q" }).pipe(Effect.provide(engine), Effect.scoped)

        // the awakeable id is a deterministic function of executionId + ordinal, so
        // the ingress caller can resolve it even though it was minted in process 1.
        const approved = yield* Effect.gen(function*() {
          yield* resolveAwakeable(id, `${id}/awk/0`, Approval, { approved: true })
          return yield* attach(id, Schema.Boolean)
        }).pipe(Effect.provide(engine), Effect.scoped)

        expect(approved).toBe(true)
      }))
  },
)
