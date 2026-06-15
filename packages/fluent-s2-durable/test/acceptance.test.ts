import {
  Array,
  Cause,
  Duration,
  Effect,
  Exit,
  HashMap,
  Layer,
  Option,
  Order,
  Result,
  Schedule,
  Schema,
  Stream,
} from "effect"
import { expect, layer } from "@effect/vitest"
import {
  Awakeable,
  S2,
  S2Write,
  Snapshot,
  Step,
  TimerFired,
  TimerSet,
  decodeRecord,
  fold,
  recordSignature,
  type Journal,
  type JournalRecord,
  type S2Service,
  type WorkerConfig,
} from "../src/index.ts"
import { S2LiteLive } from "./s2lite.ts"
import { crash, killOnStep, makeFaultyS2, spawnWorker } from "./harness.ts"
import {
  makeChargeBook,
  makeOrderHandler,
  type ChargeBook,
  type OrderInput,
  type Receipt,
} from "./demo.ts"

const config = (
  book: ChargeBook,
  cooloff: Duration.Duration,
): WorkerConfig<OrderInput, Receipt, never> => ({
  handler: makeOrderHandler(book, cooloff),
  handlerLayer: Layer.empty,
})

const wf = (execId: string): string => `wf/${execId}`

const journalOf = (s2: S2Service, execId: string): Effect.Effect<Journal, never> =>
  fold(s2.read(wf(execId), 0n)).pipe(Effect.orDie)

const ops = (j: Journal): ReadonlyArray<JournalRecord> => Array.fromIterable(HashMap.values(j.byName))
const hasStep = (name: string) => (j: Journal): boolean =>
  Array.some(Array.filter(ops(j), Schema.is(Step)), (s) => s.name === name)
const hasTimerSet = (j: Journal): boolean => Array.some(ops(j), Schema.is(TimerSet))
const hasTimerFired = (j: Journal): boolean => Array.some(ops(j), Schema.is(TimerFired))
const hasAwakeable = (j: Journal): boolean => Array.some(ops(j), Schema.is(Awakeable))

const signature = (j: Journal): ReadonlyArray<string> =>
  Array.sort(
    Array.map(ops(j), (r) => `${(r as { readonly name: string }).name}:${recordSignature(r)}`),
    Order.String,
  )

/** Poll the journal until `predicate` holds (real-time bounded). */
const waitForJournal = (
  s2: S2Service,
  execId: string,
  predicate: (j: Journal) => boolean,
): Effect.Effect<Journal> =>
  journalOf(s2, execId).pipe(
    Effect.flatMap((j) =>
      predicate(j) ? Effect.succeed(j) : Effect.fail(new Error("predicate not yet")),
    ),
    Effect.retry(Schedule.spaced(Duration.millis(10))),
    Effect.timeout(Duration.seconds(8)),
    Effect.orDie,
  )

layer(S2LiteLive, { excludeTestServices: true, timeout: Duration.seconds(30) })(
  "durable execution — acceptance (real s2-lite)",
  (it) => {
    it.effect("AC-1a exactly-once: crash after charge lands, before ack — counter stays 1", () =>
      Effect.gen(function* () {
        const faulty = yield* makeFaultyS2
        const book = makeChargeBook()
        yield* faulty.arm(killOnStep("charge", true))

        const w1 = yield* spawnWorker(faulty.service, config(book, Duration.millis(30)))
        yield* w1.worker.start("ac1a", { orderId: "ac1a", amount: 100 })
        yield* faulty.crashed
        yield* crash(w1)
        expect(book.charged).toBe(1)

        const w2 = yield* spawnWorker(faulty.service, config(book, Duration.millis(30)))
        yield* w2.worker.boot(["ac1a"])
        yield* w2.worker.resolveEvent("ac1a", "approval", true)
        const receipt = yield* w2.worker.awaitResult("ac1a")
        yield* crash(w2)

        expect(receipt.status).toBe("fulfilled")
        expect(book.charged).toBe(1)
        expect(book.calls).toBe(1) // landed record ⇒ replay short-circuits; never re-runs
      }))

    it.effect("AC-1b exactly-once: crash after charge runs, before append — idempotency holds", () =>
      Effect.gen(function* () {
        const faulty = yield* makeFaultyS2
        const book = makeChargeBook()
        yield* faulty.arm(killOnStep("charge", false))

        const w1 = yield* spawnWorker(faulty.service, config(book, Duration.millis(30)))
        yield* w1.worker.start("ac1b", { orderId: "ac1b", amount: 100 })
        yield* faulty.crashed
        yield* crash(w1)
        expect(book.charged).toBe(1)

        const w2 = yield* spawnWorker(faulty.service, config(book, Duration.millis(30)))
        yield* w2.worker.boot(["ac1b"])
        yield* w2.worker.resolveEvent("ac1b", "approval", true)
        const receipt = yield* w2.worker.awaitResult("ac1b")
        yield* crash(w2)

        expect(receipt.status).toBe("fulfilled")
        expect(book.charged).toBe(1) // idempotency key dedupes the re-run
        expect(book.calls).toBeGreaterThanOrEqual(2) // effect re-ran on resume
      }))

    it.effect("AC-2 determinism: identical input replays an identical entry signature", () =>
      Effect.gen(function* () {
        const s2 = yield* Effect.service(S2)
        const sig = (execId: string) =>
          Effect.gen(function* () {
            const book = makeChargeBook()
            const w = yield* spawnWorker(s2, config(book, Duration.millis(20)))
            yield* w.worker.start(execId, { orderId: execId, amount: 10 })
            yield* w.worker.resolveEvent(execId, "approval", true)
            yield* w.worker.awaitResult(execId)
            const j = yield* journalOf(s2, execId)
            yield* crash(w)
            return signature(j)
          })
        expect(yield* sig("rep-a")).toEqual(yield* sig("rep-b"))
      }))

    it.effect("AC-2 divergence: reusing a name for a different op kind fails loudly", () =>
      Effect.gen(function* () {
        const s2 = yield* Effect.service(S2)
        const book = makeChargeBook()
        const w1 = yield* spawnWorker(s2, config(book, Duration.seconds(60)))
        yield* w1.worker.start("div1", { orderId: "div1", amount: 5 })
        yield* waitForJournal(s2, "div1", hasStep("charge"))
        yield* crash(w1)

        const mutated: WorkerConfig<OrderInput, Receipt, never> = {
          handler: (ctx) =>
            Effect.gen(function* () {
              // "charge" was recorded as a Step; issuing it as a sleep is a divergence.
              yield* ctx.sleep("charge", Duration.millis(1))
              return { status: "rejected" } satisfies Receipt
            }),
          handlerLayer: Layer.empty,
        }
        const w2 = yield* spawnWorker(s2, mutated)
        const exit = yield* Effect.exit(w2.worker.tick("div1"))
        yield* crash(w2)

        expect(Exit.isFailure(exit)).toBe(true)
        const tag = Exit.isFailure(exit)
          ? Result.match(Cause.findDefect(exit.cause), {
              onSuccess: (d) => (d as { readonly _tag?: string })._tag,
              onFailure: () => undefined,
            })
          : undefined
        expect(tag).toBe("DivergenceError")
      }))

    it.effect("AC-3 durable sleep: a timer elapsed during downtime fires on recovery", () =>
      Effect.gen(function* () {
        const s2 = yield* Effect.service(S2)
        const book = makeChargeBook()
        const w1 = yield* spawnWorker(s2, config(book, Duration.millis(40)))
        yield* w1.worker.start("slp1", { orderId: "slp1", amount: 7 })
        yield* waitForJournal(s2, "slp1", hasTimerSet)
        yield* crash(w1)
        // let fireAt elapse with no worker alive
        yield* Effect.sleep(Duration.millis(90))
        const before = yield* journalOf(s2, "slp1")
        expect(hasTimerFired(before)).toBe(false)

        const w2 = yield* spawnWorker(s2, config(book, Duration.millis(40)))
        yield* w2.worker.boot(["slp1"])
        yield* w2.worker.resolveEvent("slp1", "approval", true)
        const receipt = yield* w2.worker.awaitResult("slp1")
        yield* crash(w2)

        expect(receipt.status).toBe("fulfilled")
        expect(hasTimerFired(yield* journalOf(s2, "slp1"))).toBe(true)
      }))

    it.effect("AC-4 durable await: resolved from a separate worker; double-resolve is idempotent", () =>
      Effect.gen(function* () {
        const s2 = yield* Effect.service(S2)
        const book = makeChargeBook()
        const host = yield* spawnWorker(s2, config(book, Duration.millis(20)))
        yield* host.worker.start("evt1", { orderId: "evt1", amount: 9 })
        yield* waitForJournal(s2, "evt1", hasAwakeable)

        // a *separate* worker resolves the event via the inbox, twice
        const resolver = yield* spawnWorker(s2, config(book, Duration.millis(20)))
        yield* resolver.worker.resolveEvent("evt1", "approval", true)
        yield* resolver.worker.resolveEvent("evt1", "approval", true)

        const receipt = yield* host.worker.awaitResult("evt1")
        yield* crash(host)
        yield* crash(resolver)

        expect(receipt.status).toBe("fulfilled")
        expect(book.fulfilled).toBe(1) // double-resolve advanced exactly once
      }))

    it.effect("AC-4 rejection branch: a false approval rejects", () =>
      Effect.gen(function* () {
        const s2 = yield* Effect.service(S2)
        const book = makeChargeBook()
        const w = yield* spawnWorker(s2, config(book, Duration.millis(20)))
        yield* w.worker.start("evt2", { orderId: "evt2", amount: 9 })
        yield* w.worker.resolveEvent("evt2", "approval", false)
        const receipt = yield* w.worker.awaitResult("evt2")
        yield* crash(w)
        expect(receipt.status).toBe("rejected")
        expect(book.fulfilled).toBe(0)
      }))

    it.effect("AC-5 bounded replay: after a snapshot, fold reads from the snapshot head", () =>
      Effect.gen(function* () {
        const s2 = yield* Effect.service(S2)
        const book = makeChargeBook()
        const w = yield* spawnWorker(s2, config(book, Duration.millis(20)))
        yield* w.worker.start("snap1", { orderId: "snap1", amount: 11 })
        yield* waitForJournal(s2, "snap1", hasAwakeable)

        yield* w.worker.snapshot("snap1")
        // A Snapshot record was written, and folding the stream reseeds byName
        // from it — recovery is bounded by the snapshot, not pre-snapshot history.
        // (Physical trim is eventual on S2, so we don't assert immediate truncation.)
        const physical = yield* s2.read(wf("snap1"), 0n).pipe(
          Stream.mapEffect((r) => decodeRecord(r.data)),
          Stream.runCollect,
        )
        expect(Array.some(physical, Schema.is(Snapshot))).toBe(true)
        const recovered = yield* journalOf(s2, "snap1")
        expect(hasStep("charge")(recovered)).toBe(true)
        expect(recovered.input).toEqual({ orderId: "snap1", amount: 11 })

        yield* w.worker.resolveEvent("snap1", "approval", true)
        const receipt = yield* w.worker.awaitResult("snap1")
        yield* crash(w)
        expect(receipt.status).toBe("fulfilled")
      }))

    it.effect("AC-6 fence safety: a stale lease cannot commit; the owner can; no double-commit", () =>
      Effect.gen(function* () {
        const s2 = yield* Effect.service(S2)
        const enc = (s: string): Uint8Array => new TextEncoder().encode(s)
        const stream = "wf/fence1"
        // The lease is acquired as a conditional fence append; fence/trim are real
        // records that consume seq numbers, so positions come from checkTail.
        const fence = (token: string) =>
          Effect.gen(function* () {
            const at = yield* s2.checkTail(stream)
            yield* s2.append(stream, [S2Write.Fence({ token })], { matchSeqNum: at })
          })
        const record = (token: string, body: string) =>
          Effect.gen(function* () {
            const at = yield* s2.checkTail(stream)
            return yield* s2.append(stream, [S2Write.Record({ body: enc(body) })], {
              fencingToken: token,
              matchSeqNum: at,
            })
          })
        yield* fence("ownerA")
        yield* record("ownerA", "seed")
        // a newer worker takes over the lease
        yield* fence("ownerB")
        // the stale owner A cannot commit; owner B can, at the same position
        const contested = yield* s2.checkTail(stream)
        const zombie = yield* s2
          .append(stream, [S2Write.Record({ body: enc("zombie") })], {
            fencingToken: "ownerA",
            matchSeqNum: contested,
          })
          .pipe(Effect.exit)
        const owner = yield* s2.append(stream, [S2Write.Record({ body: enc("owner") })], {
          fencingToken: "ownerB",
          matchSeqNum: contested,
        })

        expect(Exit.isFailure(zombie)).toBe(true)
        const reason = Exit.isFailure(zombie)
          ? Option.match(Cause.findErrorOption(zombie.cause), {
              onNone: () => undefined,
              onSome: (e) => (e as { readonly reason?: string }).reason,
            })
          : undefined
        expect(reason).toBe("fence-mismatch")
        // owner committed exactly once at the contested position; no double-commit.
        expect(owner.tail).toBe(contested + 1n)
      }))
  },
)
