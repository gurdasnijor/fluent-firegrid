import { expect, layer } from "@effect/vitest"
import { Duration, Effect, Option, Schedule, Schema } from "effect"
import { primaryKey, Table } from "effect-s2-stream-db"
import {
  attach,
  awakeable,
  client,
  object,
  ObjectStateDb,
  resolveAwakeable,
  resolveSignal,
  sendClient,
  service,
  serviceLayer,
  signal,
  state,
} from "../src/index.ts"
import { S2LiteLive } from "./s2lite.ts"

const Approval = Schema.Struct({ approved: Schema.Boolean })

class Balance extends Table<Balance>("balance")({
  id: Schema.String.pipe(primaryKey),
  value: Schema.Number,
}) {}

// a virtual object whose `deposit` does a read-modify-write then PARKS (so it stays
// incomplete) — used to force two queued same-key invocations across a restart.
const ledger = object({
  name: "ledger",
  handlers: {
    *deposit(amount: number) {
      const bal = state(Balance)
      const cur = Option.match(yield* bal.get("b"), { onNone: () => 0, onSome: (r) => r.value })
      yield* bal.set({ id: "b", value: cur + amount })
      yield* signal("posted", Schema.Boolean) // park until released, keeping it incomplete
      return cur + amount
    },
    *total() {
      const bal = state(Balance)
      return Option.match(yield* bal.get("b"), { onNone: () => 0, onSome: (r) => r.value })
    },
  },
})

// a non-parking variant so a re-run would *complete* (double-apply → a wrong value)
// rather than hang — used to exercise the post-completion-crash dedup deterministically.
const accrual = object({
  name: "accrual",
  handlers: {
    *add(amount: number) {
      const bal = state(Balance)
      const cur = Option.match(yield* bal.get("b"), { onNone: () => 0, onSome: (r) => r.value })
      yield* bal.set({ id: "b", value: cur + amount })
      return cur + amount
    },
    *total() {
      const bal = state(Balance)
      return Option.match(yield* bal.get("b"), { onNone: () => 0, onSome: (r) => r.value })
    },
  },
})

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

    it.effect("object: queued same-key methods drain in order across a restart (no lost update)", () =>
      Effect.gen(function*() {
        const engine = serviceLayer(ledger)

        // process 1: enqueue two deposits on ONE key, then tear the engine down before
        // either completes (they park). Both invocations live in the durable inbox.
        const [idA, idB] = yield* Effect.gen(function*() {
          const a = yield* sendClient(ledger, "acct").deposit(5)
          const b = yield* sendClient(ledger, "acct").deposit(3)
          return [a, b] as const
        }).pipe(Effect.provide(engine), Effect.scoped)

        // process 2: a fresh engine drains the inbox IN ORDER — A then B — so B's RMW
        // sees A's committed write. Pre-fix, recovery re-raced both incomplete
        // executions and a reordered replay of A's journaled read could clobber B.
        const result = yield* Effect.gen(function*() {
          const release = (id: string) =>
            resolveSignal(id, "posted", Schema.Boolean, true).pipe(
              Effect.retry({ schedule: Schedule.spaced(Duration.millis(20)), times: 100 }),
            )
          yield* release(idA)
          const ra = yield* attach(idA, Schema.Number)
          yield* release(idB) // B only starts once A completes (single-writer per key)
          const rb = yield* attach(idB, Schema.Number)
          const total = yield* client(ledger, "acct").total()
          return { ra, rb, total }
        }).pipe(Effect.provide(engine), Effect.scoped)

        expect(result.ra).toBe(5)
        expect(result.rb).toBe(8) // B observed A's write — order preserved
        expect(result.total).toBe(8) // 0 + 5 + 3, nothing lost
      }))

    it.effect("object: a completed-but-not-dequeued head is not re-run after a crash (no double-apply)", () =>
      Effect.gen(function*() {
        const engine = serviceLayer(accrual)

        // process 1: one add runs fully to completion (balance = 5, inbox drained).
        const idA = yield* Effect.gen(function*() {
          const id = yield* sendClient(accrual, "dd").add(5)
          yield* attach(id, Schema.Number) // wait for completion
          return id
        }).pipe(Effect.provide(engine), Effect.scoped)

        // simulate a crash in the gap between `complete` (roster terminal + stream
        // dropped) and the inbox dequeue: the durable inbox row for the *already
        // completed* A survives. (Re-planting it is the only deterministic way to hit
        // this window — a real crash there is timing-dependent.) Re-open per attempt so
        // a fresh CAS tail is read; retry converges once process 1's drainer is gone.
        yield* ObjectStateDb.open("accrual:dd").pipe(
          Effect.flatMap((db) => db.inbox.insert({ executionId: idA, seq: 1, handlerName: "accrual/add", input: 5 })),
          Effect.retry({ schedule: Schedule.spaced(Duration.millis(25)), times: 40 }),
        )

        // process 2: a fresh op triggers a drain. The stale head must be dequeued, not
        // re-run — else its read-modify-write double-applies (balance → 10).
        const total = yield* client(accrual, "dd").total().pipe(Effect.provide(engine), Effect.scoped)

        expect(total).toBe(5)
      }).pipe(Effect.provide(S2LiteLive)))
  },
)
