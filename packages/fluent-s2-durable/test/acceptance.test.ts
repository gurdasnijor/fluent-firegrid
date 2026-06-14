import { Cause, Duration, Effect, Exit, Layer, Result, Stream } from "effect"
import { describe, expect, it } from "vitest"
import {
  S2InMemory,
  decodeRecord,
  fold,
  type Journal,
  type S2Service,
  type WorkerConfig,
} from "../src/index.ts"
import {
  awaitCrash,
  delay,
  makeFaultyS2,
  spawnWorker,
  type FaultPredicate,
} from "./harness.ts"
import {
  makeChargeBook,
  makeOrderHandler,
  type ChargeBook,
  type OrderInput,
  type Receipt,
} from "./demo.ts"

const run = <A, E>(effect: Effect.Effect<A, E>): Promise<A> => Effect.runPromise(effect)

const config = (
  book: ChargeBook,
  cooloff: Duration.Duration,
): WorkerConfig<OrderInput, Receipt, never> => ({
  handler: makeOrderHandler(book, cooloff),
  handlerLayer: Layer.empty,
})

const wfStream = (execId: string): string => `wf/${execId}`

const journalOf = (s2: S2Service, execId: string): Promise<Journal> =>
  run(fold(s2.read(wfStream(execId), 0n)))

/** Poll the journal until `predicate` holds (or fail after ~2s). */
const waitForJournal = async (
  s2: S2Service,
  execId: string,
  predicate: (j: Journal) => boolean,
): Promise<Journal> => {
  for (let i = 0; i < 200; i++) {
    const j = await journalOf(s2, execId)
    if (predicate(j)) return j
    await delay(10)
  }
  throw new Error(`journal predicate never held for ${execId}`)
}

const hasStep = (name: string) => (j: Journal): boolean =>
  [...j.byOp.values()].some((r) => r.kind === "step" && r.name === name)

const hasKind = (kind: string) => (j: Journal): boolean =>
  [...j.byOp.values()].some((r) => r.kind === kind)

const opSignature = (j: Journal): ReadonlyArray<string> =>
  [...j.byOp.entries()]
    .sort(([a], [b]) => a - b)
    .map(([op, r]) =>
      r.kind === "step" || r.kind === "timer-set" || r.kind === "awakeable"
        ? `${op}:${r.kind}:${r.name}`
        : `${op}:${r.kind}`,
    )

const killOnCharge = (land: boolean): FaultPredicate => ({
  onStream: (s) => s.startsWith("wf/") && !s.endsWith("/inbox"),
  onRecord: (r) => r?.kind === "step" && r.name === "charge",
  landBeforeCrash: land,
})

describe("AC-1 — exactly-once side effects under crash", () => {
  it("crash after charge lands but before ack: charged stays 1, no re-run", async () => {
    const faulty = await run(makeFaultyS2)
    const book = makeChargeBook()
    await run(faulty.arm(killOnCharge(true)))

    const w1 = await spawnWorker(faulty.service, config(book, Duration.millis(30)))
    w1.run()
    await run(w1.worker.start("ord-1", { orderId: "ord-1", amount: 100 }))
    await awaitCrash(faulty)
    await w1.crash()
    expect(book.charged).toBe(1)

    const w2 = await spawnWorker(faulty.service, config(book, Duration.millis(30)))
    w2.run()
    await run(w2.worker.boot(["ord-1"]))
    await run(w2.worker.resolveEvent("ord-1", "approval", true))
    const receipt = await run(w2.worker.awaitResult("ord-1"))
    await w2.crash()

    expect(receipt.status).toBe("fulfilled")
    expect(book.charged).toBe(1)
    expect(book.calls).toBe(1) // landed record ⇒ replay short-circuits, effect never re-runs
  })

  // The §8 invariant is "external charge counter == 1"; `calls` (raw effect
  // invocations) is at-least-once and not asserted exactly.

  it("crash after charge runs but before append: idempotency keeps charged at 1", async () => {
    const faulty = await run(makeFaultyS2)
    const book = makeChargeBook()
    await run(faulty.arm(killOnCharge(false)))

    const w1 = await spawnWorker(faulty.service, config(book, Duration.millis(30)))
    w1.run()
    await run(w1.worker.start("ord-2", { orderId: "ord-2", amount: 100 }))
    await awaitCrash(faulty)
    await w1.crash()
    expect(book.charged).toBe(1)

    const w2 = await spawnWorker(faulty.service, config(book, Duration.millis(30)))
    w2.run()
    await run(w2.worker.boot(["ord-2"]))
    await run(w2.worker.resolveEvent("ord-2", "approval", true))
    const receipt = await run(w2.worker.awaitResult("ord-2"))
    await w2.crash()

    expect(receipt.status).toBe("fulfilled")
    expect(book.charged).toBe(1) // idempotency key dedupes the re-run
    expect(book.calls).toBe(2) // effect re-ran exactly once on resume (no journaled record)
  })
})

describe("AC-2 — replay determinism & divergence", () => {
  it("identical input replays an identical op-index → (kind,name) sequence", async () => {
    const s2 = await run(S2InMemory.make)
    const sig = async (execId: string): Promise<ReadonlyArray<string>> => {
      const book = makeChargeBook()
      const w = await spawnWorker(s2, config(book, Duration.millis(20)))
      w.run()
      await run(w.worker.start(execId, { orderId: execId, amount: 10 }))
      await run(w.worker.resolveEvent(execId, "approval", true))
      await run(w.worker.awaitResult(execId))
      const j = await journalOf(s2, execId)
      await w.crash()
      return opSignature(j)
    }
    expect(await sig("rep-a")).toEqual(await sig("rep-b"))
  })

  it("a handler that issues a different op at an index fails loudly with DivergenceError", async () => {
    const s2 = await run(S2InMemory.make)
    const book = makeChargeBook()
    const w1 = await spawnWorker(s2, config(book, Duration.seconds(60)))
    w1.run()
    await run(w1.worker.start("div-1", { orderId: "div-1", amount: 5 }))
    await waitForJournal(s2, "div-1", hasStep("charge"))
    await w1.crash()

    const mutated: WorkerConfig<OrderInput, Receipt, never> = {
      handler: (ctx) =>
        Effect.gen(function* () {
          yield* ctx.run("charge-MUTATED", Effect.succeed("x"))
          return { status: "rejected" } satisfies Receipt
        }),
      handlerLayer: Layer.empty,
    }
    const w2 = await spawnWorker(s2, mutated)
    const exit = await run(Effect.exit(w2.worker.tick("div-1")))
    await w2.crash()

    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit)) {
      const defect = Cause.findDefect(exit.cause)
      const tag = Result.isSuccess(defect)
        ? (defect.success as { readonly _tag?: string })._tag
        : undefined
      expect(tag).toBe("DivergenceError")
    }
  })
})

describe("AC-3 — durable sleep across restart", () => {
  it("a timer whose fireAt elapsed during downtime fires immediately on recovery", async () => {
    const s2 = await run(S2InMemory.make)
    const book = makeChargeBook()
    const w1 = await spawnWorker(s2, config(book, Duration.millis(40)))
    w1.run()
    await run(w1.worker.start("slp-1", { orderId: "slp-1", amount: 7 }))
    // wait until the cooloff timer is journaled, then kill before it fires
    await waitForJournal(s2, "slp-1", hasKind("timer-set"))
    await w1.crash()
    // let fireAt elapse while no worker is alive
    await delay(80)
    const beforeFired = await journalOf(s2, "slp-1")
    expect(hasKind("timer-fired")(beforeFired)).toBe(false)

    const w2 = await spawnWorker(s2, config(book, Duration.millis(40)))
    w2.run()
    await run(w2.worker.boot(["slp-1"]))
    await run(w2.worker.resolveEvent("slp-1", "approval", true))
    const receipt = await run(w2.worker.awaitResult("slp-1"))
    await w2.crash()

    expect(receipt.status).toBe("fulfilled")
    const j = await journalOf(s2, "slp-1")
    expect(hasKind("timer-fired")(j)).toBe(true)
  })
})

describe("AC-4 — durable await", () => {
  it("resolves from a separate worker; resolving twice does not double-advance", async () => {
    const s2 = await run(S2InMemory.make)
    const book = makeChargeBook()
    const host = await spawnWorker(s2, config(book, Duration.millis(20)))
    host.run()
    await run(host.worker.start("evt-1", { orderId: "evt-1", amount: 9 }))
    await waitForJournal(s2, "evt-1", hasKind("awakeable"))

    // a *separate* process resolves the event via the inbox, twice
    const resolver = await spawnWorker(s2, config(book, Duration.millis(20)))
    await run(resolver.worker.resolveEvent("evt-1", "approval", true))
    await run(resolver.worker.resolveEvent("evt-1", "approval", true))

    const receipt = await run(host.worker.awaitResult("evt-1"))
    await host.crash()
    await resolver.crash()

    expect(receipt.status).toBe("fulfilled")
    expect(book.fulfilled).toBe(1) // idempotent: double-resolve advanced exactly once
  })

  it("a false approval drives the rejection branch", async () => {
    const s2 = await run(S2InMemory.make)
    const book = makeChargeBook()
    const w = await spawnWorker(s2, config(book, Duration.millis(20)))
    w.run()
    await run(w.worker.start("evt-2", { orderId: "evt-2", amount: 9 }))
    await run(w.worker.resolveEvent("evt-2", "approval", false))
    const receipt = await run(w.worker.awaitResult("evt-2"))
    await w.crash()
    expect(receipt.status).toBe("rejected")
    expect(book.fulfilled).toBe(0)
  })
})

describe("AC-5 — bounded replay via snapshot-and-follow", () => {
  it("after a snapshot, a fresh fold reads from head and only later deltas", async () => {
    const s2 = await run(S2InMemory.make)
    const book = makeChargeBook()
    const w = await spawnWorker(s2, config(book, Duration.millis(20)))
    w.run()
    await run(w.worker.start("snap-1", { orderId: "snap-1", amount: 11 }))
    await waitForJournal(s2, "snap-1", hasKind("awakeable"))

    const before = await run(s2.read(wfStream("snap-1"), 0n).pipe(Stream.runCollect))
    expect(before.length).toBeGreaterThan(2)

    await run(w.worker.snapshot("snap-1"))

    const physical = await run(s2.read(wfStream("snap-1"), 0n).pipe(Stream.runCollect))
    const firstKind = await run(decodeRecord(physical[0]!.data))
    expect(firstKind.kind).toBe("snapshot") // history below the cursor was trimmed away
    expect(physical.length).toBeLessThan(before.length)

    // recovery from the snapshot is still correct
    const j = await journalOf(s2, "snap-1")
    expect(hasStep("charge")(j)).toBe(true)
    expect(j.input).toEqual({ orderId: "snap-1", amount: 11 })

    // …and the workflow still completes off the snapshot
    await run(w.worker.resolveEvent("snap-1", "approval", true))
    const receipt = await run(w.worker.awaitResult("snap-1"))
    await w.crash()
    expect(receipt.status).toBe("fulfilled")
  })
})

describe("AC-6 — fence safety (two-worker probe)", () => {
  it("a stale lease cannot commit; the current fence holder can; no double-commit", async () => {
    // The runtime-level invariant reduces to the S2 fence primitive: a writer
    // presenting an out-of-date fencing token is rejected (412 fence-mismatch),
    // while the lease holder commits exactly once.
    const s2 = await run(S2InMemory.make)
    const probe = await run(
      Effect.gen(function* () {
        const stream = "wf/fence-1"
        yield* s2.fence(stream, "00000000000000000010")
        yield* s2.append(stream, [new TextEncoder().encode("seed")], {
          fencingToken: "00000000000000000010",
          matchSeqNum: 0n,
        })
        // a newer worker takes the lease
        yield* s2.fence(stream, "00000000000000000020")
        const zombie = yield* s2
          .append(stream, [new TextEncoder().encode("zombie")], {
            fencingToken: "00000000000000000010",
            matchSeqNum: 1n,
          })
          .pipe(Effect.result)
        const owner = yield* s2.append(stream, [new TextEncoder().encode("owner")], {
          fencingToken: "00000000000000000020",
          matchSeqNum: 1n,
        })
        const tail = yield* s2.checkTail(stream)
        return { zombie, ownerTail: owner.tail, tail }
      }),
    )
    expect(probe.zombie._tag).toBe("Failure")
    if (probe.zombie._tag === "Failure" && probe.zombie.failure._tag === "AppendCondFailed") {
      expect(probe.zombie.failure.reason).toBe("fence-mismatch")
    }
    expect(probe.ownerTail).toBe(2n)
    expect(probe.tail).toBe(2n) // exactly one commit at the contested position
  })
})
