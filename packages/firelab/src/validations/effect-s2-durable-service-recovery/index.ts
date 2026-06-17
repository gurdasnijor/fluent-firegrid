import { Effect, Schema } from "effect"
import {
  attach,
  awakeable,
  resolveAwakeable,
  resolveSignal,
  sendClient,
  service,
  serviceLayer,
  signal,
} from "effect-s2-durable"
import { assertEquals } from "../../assertions.ts"
import { S2LiteLive } from "../../s2lite.ts"
import { defineValidation } from "../../types.ts"

// Boot recovery is product behaviour over real S2, not a unit concern — so it is
// proven here as a vertical validation with OTel evidence (same as the object-call
// path), driving the PUBLIC serviceLayer/sendClient/resolveSignal/attach surface.
//
// A "restart" is a SECOND engine scope over the SAME s2 lite backend: process 1
// submits and parks, then its engine scope closes (the durable S2 streams outlive
// it); process 2 stands a fresh engine that boot-recovers the parked execution from
// the roster + WorkflowDb, re-drives it (re-parking), and an ingress resolution +
// attach then settle it. Only ONE engine is live at a time.
//
// (Object boot-recovery is the Slice A recovery gap — it lands in the Object API
// Completion Batch as its own vertical validation. This covers the service runtime,
// which keeps the WorkflowDb/roster model until the service-runtime pass.)

const Approval = Schema.Struct({ approved: Schema.Boolean })

export default defineValidation({
  id: "effect-s2-durable-service-recovery",
  description:
    "Proves boot recovery of a parked durable SERVICE execution across an engine restart over one s2 "
    + "lite backend, through the public serviceLayer/sendClient/resolveSignal/attach path: a fresh "
    + "engine re-drives a signal- or awakeable-parked execution from the roster + WorkflowDb, then an "
    + "ingress resolution settles it — with OTel spans for boot recovery and the StreamDb/S2 reads.",
  feature: {
    product: "effect-s2-durable",
    name: "object-actor-model",
  },
  // just the s2 lite backend (the S2Client); the claims build engine scopes over it.
  backend: S2LiteLive,
  component: () => Effect.void,
  requirements: [
    {
      id: "RECOVERY.4",
      description:
        "a fresh engine restarts a non-resident parked execution from durable state (signal-parked "
        + "service re-driven across an engine restart, then resolved + attached)",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.boot-recover")) && spans.exists(s, named(s, "effect-s2-durable.recover-execution")) && spans.exists(s, named(s, "effect-s2-stream-db.open"))',
      claim: () =>
        Effect.gen(function*() {
          const svc = service({
            name: "firelab-recover-signal",
            handlers: {
              *approve(_req: { x: string }) {
                return (yield* signal("approval", Approval)).approved
              },
            },
          })
          // `serviceLayer` seeds the registry so a fresh engine can re-drive by name.
          const engine = serviceLayer(svc)

          // process 1: submit, let it park on the signal, then tear the engine down.
          const id = yield* sendClient(svc).approve({ x: "q" }).pipe(Effect.provide(engine), Effect.scoped)

          // process 2: a fresh engine over the SAME s2 boot-recovers the execution,
          // re-parks it, and is resident again — so resolve + attach settle it.
          const approved = yield* Effect.gen(function*() {
            yield* resolveSignal(id, "approval", Approval, { approved: true })
            return yield* attach(id, Schema.Boolean)
          }).pipe(Effect.provide(engine), Effect.scoped)

          assertEquals(approved, true)
        }),
    },
    {
      id: "INGRESS.1",
      description:
        "resolving an awakeable on a recovered execution succeeds regardless of residency — the "
        + "replay-stable awakeable id minted in process 1 is resolvable in process 2 after recovery",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.boot-recover")) && spans.exists(s, named(s, "effect-s2-durable.recover-execution")) && spans.exists(s, named(s, "S2.append"))',
      claim: () =>
        Effect.gen(function*() {
          const svc = service({
            name: "firelab-recover-awakeable",
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
          // the ingress caller resolves it even though it was minted in process 1.
          const approved = yield* Effect.gen(function*() {
            yield* resolveAwakeable(id, `${id}/awk/0`, Approval, { approved: true })
            return yield* attach(id, Schema.Boolean)
          }).pipe(Effect.provide(engine), Effect.scoped)

          assertEquals(approved, true)
        }),
    },
  ],
})
