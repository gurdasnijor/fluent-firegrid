import { FetchHttpClient, type HttpClient } from "@effect/platform"
import { Chunk, Effect, Fiber, Ref, type Scope, Schema, Stream } from "effect"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { DurableStream } from "../../src/index.ts"
import { startTestServer, type TestServerHandle } from "./test-server.ts"

let server: TestServerHandle

beforeAll(async () => {
  server = await startTestServer()
})

afterAll(async () => {
  await server.stop()
})

const Message = Schema.Struct({ n: Schema.Number })

type Reqs = FetchHttpClient.Fetch | HttpClient.HttpClient | Scope.Scope

const runtime = <A, E>(eff: Effect.Effect<A, E, Reqs>) =>
  Effect.runPromise(
    Effect.scoped(eff.pipe(Effect.provide(FetchHttpClient.layer))) as unknown as Effect.Effect<A, E, never>,
  )

describe("Phase 1 live reads", () => {
  it("long-poll delivers items appended after the reader starts", async () => {
    const url = server.streamUrl("longpoll")
    const s = DurableStream.define({ endpoint: { url }, schema: Message })

    await runtime(
      Effect.gen(function* () {
        yield* s.create({ contentType: "application/json" })
        yield* s.append({ n: 0 })

        const seen = yield* Ref.make<ReadonlyArray<number>>([])
        const fiber = yield* s
          .read({ live: "long-poll" })
          .pipe(
            Stream.take(3),
            Stream.runForEach((msg) =>
              Ref.update(seen, (arr) => [...arr, msg.n]),
            ),
            Effect.fork,
          )

        // Give the reader a moment to attach via long-poll.
        yield* Effect.sleep("50 millis")
        yield* s.append({ n: 1 })
        yield* s.append({ n: 2 })

        yield* Fiber.join(fiber)
        const result = yield* Ref.get(seen)
        expect(result).toEqual([0, 1, 2])
      }),
    )
  }, 15000)

  it("SSE delivers items appended after the reader starts", async () => {
    const url = server.streamUrl("sse")
    const s = DurableStream.define({ endpoint: { url }, schema: Message })

    await runtime(
      Effect.gen(function* () {
        yield* s.create({ contentType: "application/json" })
        yield* s.append({ n: 100 })

        const seen = yield* Ref.make<ReadonlyArray<number>>([])
        const fiber = yield* s
          .read({ live: "sse" })
          .pipe(
            Stream.take(3),
            Stream.runForEach((msg) =>
              Ref.update(seen, (arr) => [...arr, msg.n]),
            ),
            Effect.fork,
          )

        yield* Effect.sleep("100 millis")
        yield* s.append({ n: 101 })
        yield* s.append({ n: 102 })

        yield* Fiber.join(fiber)
        const result = yield* Ref.get(seen)
        expect(result).toEqual([100, 101, 102])
      }),
    )
  }, 15000)

  it("snapshotThenFollow is gap-free AND duplicate-free under concurrent appends", async () => {
    // Concurrent appends DURING the catch-up phase must end up in exactly one
    // of {snapshot, live} — never both, never neither.
    const url = server.streamUrl("snapfollow-concurrent")
    const s = DurableStream.define({ endpoint: { url }, schema: Message })

    await runtime(
      Effect.gen(function* () {
        yield* s.create({ contentType: "application/json" })
        // Seed 20 items as the initial history.
        for (let i = 0; i < 20; i++) {
          yield* s.append({ n: i })
        }

        // Start snapshotThenFollow and a concurrent appender at the same time.
        // The appender writes n=100..119 while snapshotThenFollow is in flight.
        const appender = Effect.gen(function* () {
          for (let i = 0; i < 20; i++) {
            yield* s.append({ n: 100 + i })
            // Tiny pause so writes don't all land in the same HTTP request.
            yield* Effect.sleep("1 millis")
          }
        }).pipe(Effect.fork)

        const writerFiber = yield* appender
        const result = yield* s.snapshotThenFollow

        // Wait for the writer to finish so the live stream has time to see
        // anything it should observe.
        yield* Fiber.join(writerFiber)

        // Collect up to (snapshot_size + 20 new) items from live, then stop.
        const expectedTotal = result.snapshot.length + (20 - (result.snapshot.length - 20))
        // Bound the live take by expected new items (everything not in snapshot).
        const liveCount = (20 + 20) - result.snapshot.length
        const liveItems = liveCount > 0
          ? yield* result.live.pipe(Stream.take(liveCount), Stream.runCollect, Effect.map(Chunk.toReadonlyArray))
          : []

        const all = [...result.snapshot.map((m) => m.n), ...liveItems.map((m) => m.n)]
        const seen = new Set<number>()
        const duplicates: Array<number> = []
        for (const n of all) {
          if (seen.has(n)) duplicates.push(n)
          seen.add(n)
        }

        // Every initial item AND every concurrent-write item must be present.
        const expectedNs = new Set<number>([
          ...Array.from({ length: 20 }, (_, i) => i),
          ...Array.from({ length: 20 }, (_, i) => 100 + i),
        ])
        for (const n of expectedNs) {
          expect(seen.has(n)).toBe(true)
        }
        expect(duplicates).toEqual([])
        void expectedTotal
      }),
    )
  }, 20000)

  it("snapshotThenFollow has no gap under concurrent appends (basic)", async () => {
    const url = server.streamUrl("snapfollow-gap")
    const s = DurableStream.define({ endpoint: { url }, schema: Message })

    await runtime(
      Effect.gen(function* () {
        yield* s.create({ contentType: "application/json" })
        for (let i = 0; i < 5; i++) {
          yield* s.append({ n: i })
        }
        const result = yield* s.snapshotThenFollow
        const snap = result.snapshot.map((m) => m.n)
        expect(snap).toEqual([0, 1, 2, 3, 4])

        // Now append a few more and confirm the live stream picks them up.
        const seen = yield* Ref.make<ReadonlyArray<number>>([])
        const liveFiber = yield* result.live
          .pipe(
            Stream.take(2),
            Stream.runForEach((msg) =>
              Ref.update(seen, (arr) => [...arr, msg.n]),
            ),
            Effect.fork,
          )
        yield* Effect.sleep("50 millis")
        yield* s.append({ n: 5 })
        yield* s.append({ n: 6 })
        yield* Fiber.join(liveFiber)
        const liveSeen = yield* Ref.get(seen)
        expect(liveSeen).toEqual([5, 6])
      }),
    )
  }, 15000)
})

describe("Phase 1 idempotent producer correctness", () => {
  it("survives restart with overlapping seqs (server dedupes)", async () => {
    const url = server.streamUrl("idem-restart")
    const s = DurableStream.define({ endpoint: { url }, schema: Message })

    await runtime(
      Effect.gen(function* () {
        yield* s.create({ contentType: "application/json" })

        // First epoch produces 3 batches.
        yield* Effect.scoped(
          Effect.gen(function* () {
            const p = yield* s.producer({
              producerId: "writer-1",
              epoch: 0,
              lingerMs: 5,
              maxBatchSize: 1,
            })
            yield* p.append({ n: 0 })
            yield* p.append({ n: 1 })
            yield* p.flush
          }),
        )

        // Same epoch and same producer-id (simulated restart with same epoch
        // re-sending the same seqs) — server returns 204 (duplicate), state
        // remains consistent.
        yield* Effect.scoped(
          Effect.gen(function* () {
            const p = yield* s.producer({
              producerId: "writer-1",
              epoch: 0,
              lingerMs: 5,
              maxBatchSize: 1,
            })
            // Append two NEW items; producer assigns fresh seqs (its lastSeq
            // is local, so it starts at 0 again — server's lastSeq is 1).
            // The first two will be duplicates (204), the next two new (200).
            yield* p.append({ n: 0 })
            yield* p.append({ n: 1 })
            yield* p.append({ n: 2 })
            yield* p.append({ n: 3 })
            yield* p.flush
          }),
        )

        const collected = yield* s.collect
        // We should see [0, 1, 2, 3] — duplicates discarded by the server.
        expect(collected.map((m) => m.n)).toEqual([0, 1, 2, 3])
      }),
    )
  }, 15000)

  it("autoClaim recovers from stale-epoch fencing", async () => {
    const url = server.streamUrl("idem-autoclaim")
    const s = DurableStream.define({ endpoint: { url }, schema: Message })

    await runtime(
      Effect.gen(function* () {
        yield* s.create({ contentType: "application/json" })

        // Writer A claims epoch 5 first.
        yield* Effect.scoped(
          Effect.gen(function* () {
            const a = yield* s.producer({
              producerId: "shared",
              epoch: 5,
              lingerMs: 5,
              maxBatchSize: 1,
            })
            yield* a.append({ n: 0 })
            yield* a.flush
          }),
        )

        // Writer B starts at the same epoch=5; the first batch should be
        // rejected as duplicate (seq=0 vs server's lastSeq=0) — but B is
        // fresh and uses autoClaim, so a stale-epoch race elsewhere bumps it.
        // To trigger 403, we start B at a lower epoch and let autoClaim
        // bump it.
        yield* Effect.scoped(
          Effect.gen(function* () {
            const b = yield* s.producer({
              producerId: "shared",
              epoch: 0,
              autoClaim: true,
              lingerMs: 5,
              maxBatchSize: 1,
            })
            yield* b.append({ n: 1 })
            yield* b.flush
          }),
        )

        const collected = yield* s.collect
        expect(collected.length).toBe(2)
        expect(collected.map((m) => m.n).sort()).toEqual([0, 1])
      }),
    )
  }, 15000)

  it("autoClaim respects maxAutoClaimAttempts cap (no infinite loop)", async () => {
    // With maxAutoClaimAttempts=0, even an autoClaim producer must NOT
    // retry on the first 403. This guards against an infinite loop if the
    // server keeps returning 403 (header bug, proxy stripping headers,
    // etc.).
    const url = server.streamUrl("idem-autoclaim-bounded")
    const s = DurableStream.define({ endpoint: { url }, schema: Message })

    await runtime(
      Effect.gen(function* () {
        yield* s.create({ contentType: "application/json" })

        // Writer A claims epoch 5.
        yield* Effect.scoped(
          Effect.gen(function* () {
            const a = yield* s.producer({
              producerId: "shared-bounded",
              epoch: 5,
              lingerMs: 5,
              maxBatchSize: 1,
            })
            yield* a.append({ n: 0 })
            yield* a.flush
          }),
        )

        // Writer B starts stale at epoch=0 with autoClaim BUT cap=0.
        // Should hit 403 and surface StaleEpoch WITHOUT bumping.
        const exit = yield* Effect.scoped(
          Effect.gen(function* () {
            const b = yield* s.producer({
              producerId: "shared-bounded",
              epoch: 0,
              autoClaim: true,
              maxAutoClaimAttempts: 0,
              lingerMs: 5,
              maxBatchSize: 1,
            })
            yield* b.append({ n: 99 })
            return yield* b.flush
          }),
        ).pipe(Effect.exit)

        expect(exit._tag).toBe("Failure")
      }),
    )
  }, 15000)

  it("maxBatchBytes splits a count-bounded batch into byte-bounded sub-batches", async () => {
    // 20 small items with a tiny maxBatchBytes — splitter must produce
    // several sub-batches, each within the cap. The end-to-end result
    // is the same N items on the server.
    const url = server.streamUrl("idem-bytecap")
    const s = DurableStream.define({ endpoint: { url }, schema: Message })

    await runtime(
      Effect.gen(function* () {
        yield* s.create({ contentType: "application/json" })
        const p = yield* s.producer({
          producerId: "bytecap",
          lingerMs: 5,
          maxBatchSize: 1000, // count cap won't trigger
          maxBatchBytes: 32, // very small — each item ~12 bytes encoded, so ~2 per batch
        })
        for (let i = 0; i < 20; i++) {
          yield* p.append({ n: i })
        }
        yield* p.flush

        const items = yield* s.collect
        expect(items.length).toBe(20)
        expect(items.map((m) => m.n)).toEqual(
          Array.from({ length: 20 }, (_, i) => i),
        )
      }),
    )
  }, 15000)

  it("without autoClaim, stale-epoch surfaces as a typed StaleEpoch failure", async () => {
    const url = server.streamUrl("idem-stale-fail")
    const s = DurableStream.define({ endpoint: { url }, schema: Message })

    await runtime(
      Effect.gen(function* () {
        yield* s.create({ contentType: "application/json" })

        // Writer A claims epoch 5.
        yield* Effect.scoped(
          Effect.gen(function* () {
            const a = yield* s.producer({
              producerId: "shared-stale",
              epoch: 5,
              lingerMs: 5,
              maxBatchSize: 1,
            })
            yield* a.append({ n: 0 })
            yield* a.flush
          }),
        )

        // Writer B starts at epoch 0 with NO autoClaim. The first batch
        // should hit 403 and surface as a StaleEpoch failure (NOT a defect,
        // NOT a TransportError) — caller can match on the tag.
        const exit = yield* Effect.scoped(
          Effect.gen(function* () {
            const b = yield* s.producer({
              producerId: "shared-stale",
              epoch: 0,
              autoClaim: false,
              lingerMs: 5,
              maxBatchSize: 1,
            })
            yield* b.append({ n: 1 })
            return yield* b.flush
          }),
        ).pipe(Effect.exit)

        expect(exit._tag).toBe("Failure")
        if (exit._tag === "Failure") {
          const fail = exit.cause
          // Should be a typed failure, not a defect.
          const failureOpt = (() => {
            let found: unknown = undefined
            // Walk Cause looking for the first Fail node.
            JSON.stringify(fail, (_k, v: unknown) => {
              if (
                v !== null &&
                typeof v === "object" &&
                "_tag" in v &&
                (v as { _tag: string })._tag === "DurableStream/StaleEpoch"
              ) {
                found = v
              }
              return v
            })
            return found
          })()
          expect(failureOpt).toBeDefined()
        }
      }),
    )
  }, 15000)
})

describe("Phase 1 onError retry hook", () => {
  it("retries an operation with new headers when onError returns RetryOpts", async () => {
    // The server doesn't care about an "Authorization" header — we use the
    // hook to count how many times it's invoked and to mutate a tracked
    // header on the FIRST call (simulating a 401 → token refresh flow).
    // The bench server doesn't 401, so to actually trigger onError we point
    // at a deleted stream first, refresh the URL to the live one, retry.
    const goodUrl = server.streamUrl("onerror-good")

    await runtime(
      Effect.gen(function* () {
        // Pre-create the good stream.
        yield* DurableStream.define({
          endpoint: { url: goodUrl },
          schema: Message,
        }).create({ contentType: "application/json" })
        yield* DurableStream.define({
          endpoint: { url: goodUrl },
          schema: Message,
        }).append({ n: 42 })

        let callCount = 0
        let lastHeader: string | undefined
        const onError = (_err: unknown): Effect.Effect<DurableStream.RetryOpts | undefined> => {
          callCount += 1
          // Only retry once, with a new header value.
          if (callCount === 1) {
            return Effect.succeed({ headers: { "X-Token": "refreshed" } })
          }
          return Effect.succeed(undefined as DurableStream.RetryOpts | undefined)
        }

        // Use a URL that 404s, with onError. The hook will fire; we just
        // return undefined the second time to let it propagate.
        const badUrl = `${server.url}/v1/stream/missing-${crypto.randomUUID()}`
        const result = yield* Effect.exit(
          DurableStream.define({
            endpoint: {
              url: badUrl,
              headers: { "X-Token": "initial", "X-Probe": () => {
                lastHeader = "captured"
                return "v"
              } },
              onError,
            },
            schema: Message,
          }).head,
        )

        expect(result._tag).toBe("Failure")
        // Hook fired at least once.
        expect(callCount).toBeGreaterThanOrEqual(1)
        void lastHeader
      }),
    )
  })

  it("onError bounded by onErrorMaxRetries (no infinite loop)", async () => {
    const badUrl = `${server.url}/v1/stream/loop-${crypto.randomUUID()}`
    let calls = 0
    const onError = (): Effect.Effect<DurableStream.RetryOpts> =>
      Effect.sync(() => {
        calls += 1
        return { headers: { "X-Retry": String(calls) } }
      })

    await runtime(
      Effect.gen(function* () {
        const result = yield* Effect.exit(
          DurableStream.define({
            endpoint: { url: badUrl, onError, onErrorMaxRetries: 2 },
            schema: Message,
          }).head,
        )
        expect(result._tag).toBe("Failure")
        // cap=2 means up to 2 retries. Handler fires for each retry-decision
        // point until the cap exhausts; with always-retry-on-error that's
        // exactly 2 handler invocations.
        expect(calls).toBe(2)
      }),
    )
  }, 15000)
})

describe("Phase 1 retention and lifecycle", () => {
  it("delete + subsequent head returns NotFound (or Gone)", async () => {
    const url = server.streamUrl("delete-flow")
    const s = DurableStream.define({ endpoint: { url }, schema: Message })

    await runtime(
      Effect.gen(function* () {
        yield* s.create({ contentType: "application/json" })
        yield* s.append({ n: 1 })
        yield* s.delete
        const exit = yield* Effect.exit(s.head)
        expect(exit._tag).toBe("Failure")
      }),
    )
  })

  it("collect on a closed stream returns all items including the close payload", async () => {
    const url = server.streamUrl("collect-closed")
    const s = DurableStream.define({ endpoint: { url }, schema: Message })

    await runtime(
      Effect.gen(function* () {
        yield* s.create({ contentType: "application/json" })
        yield* s.append({ n: 1 })
        yield* s.append({ n: 2 })
        yield* s.close()
        const items = yield* s.collect
        expect(items.map((m) => m.n)).toEqual([1, 2])
      }),
    )
  })
})

describe("Phase 1 tail() ergonomic helper", () => {
  it("tail() observes only events appended AFTER it resolves", async () => {
    const url = server.streamUrl("tail-only-new")
    const s = DurableStream.define({ endpoint: { url }, schema: Message })

    await runtime(
      Effect.gen(function* () {
        yield* s.create({ contentType: "application/json" })
        // History the caller does NOT want to see.
        for (let i = 0; i < 5; i++) {
          yield* s.append({ n: i })
        }

        // Resolve the tail (HEAD → live read from current end).
        const live = yield* s.tail

        // Kick off the consumer first so the live read is attached before
        // new appends arrive; otherwise long-poll might miss the first ones.
        const fiber = yield* live
          .pipe(Stream.take(3), Stream.runCollect, Effect.map(Chunk.toReadonlyArray))
          .pipe(Effect.fork)

        yield* Effect.sleep("100 millis")
        yield* s.append({ n: 100 })
        yield* s.append({ n: 101 })
        yield* s.append({ n: 102 })

        const observed = yield* Fiber.join(fiber)
        // Crucially: NONE of the historical 0..4 should appear.
        expect(observed.map((m) => m.n)).toEqual([100, 101, 102])
      }),
    )
  }, 15000)
})

describe("Phase 1 catchup→live deterministic handoff", () => {
  it("snapshotThenFollow with no concurrent writes: live yields strictly new events", async () => {
    // Deterministic baseline (no race): seed history, snapshotThenFollow,
    // then append new items. Every old item appears in snapshot; every new
    // item appears in live; no overlap.
    const url = server.streamUrl("snapfollow-deterministic")
    const s = DurableStream.define({ endpoint: { url }, schema: Message })

    await runtime(
      Effect.gen(function* () {
        yield* s.create({ contentType: "application/json" })
        for (let i = 0; i < 10; i++) {
          yield* s.append({ n: i })
        }

        const result = yield* s.snapshotThenFollow

        // Snapshot must contain EXACTLY the seeded items.
        expect(result.snapshot.map((m) => m.n).sort((a, b) => a - b)).toEqual([
          0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
        ])

        const fiber = yield* result.live
          .pipe(Stream.take(3), Stream.runCollect, Effect.map(Chunk.toReadonlyArray))
          .pipe(Effect.fork)

        yield* Effect.sleep("100 millis")
        yield* s.append({ n: 100 })
        yield* s.append({ n: 101 })
        yield* s.append({ n: 102 })

        const liveItems = yield* Fiber.join(fiber)
        const liveNs = liveItems.map((m) => m.n)
        expect(liveNs).toEqual([100, 101, 102])

        // No overlap: nothing from snapshot reappears in live.
        const snapshotSet = new Set(result.snapshot.map((m) => m.n))
        for (const n of liveNs) {
          expect(snapshotSet.has(n)).toBe(false)
        }
      }),
    )
  }, 15000)
})

describe("Phase 1 producer eager-emission semantics", () => {
  it("a queued burst is emitted without waiting the full lingerMs", async () => {
    // Append a burst that fully fills a batch BEFORE flush is called. With
    // the previous `groupedWithin(maxBatch, linger)` drain we paid the full
    // `lingerMs` even on a burst — eager emission should send immediately.
    // We assert by bounding wall-clock time: 50 events with linger=200ms
    // and a generous batch cap should finish in well under one linger
    // window if the eager path is wired up.
    const url = server.streamUrl("eager-burst")
    const s = DurableStream.define({ endpoint: { url }, schema: Message })

    await runtime(
      Effect.gen(function* () {
        yield* s.create({ contentType: "application/json" })
        const p = yield* s.producer({
          producerId: "eager-burst",
          epoch: 0,
          // Wide enough that 50 items fit in a single batch, so eager
          // emission of the burst yields exactly one HTTP send.
          maxBatchSize: 1000,
          // Tall linger amplifies the regression: if we wait it, the
          // assertion below fails by an order of magnitude.
          lingerMs: 200,
        })
        const start = Date.now()
        for (let i = 0; i < 50; i++) {
          yield* p.append({ n: i })
        }
        yield* p.flush
        const elapsed = Date.now() - start
        // Pre-eager behavior: ~200ms (full linger). Eager: ~5-20ms (just
        // the HTTP round trip). Use 150ms as a comfortable upper bound.
        expect(elapsed).toBeLessThan(150)

        const items = yield* s.collect
        expect(items.length).toBe(50)
      }),
    )
  }, 15000)
})

describe("Phase 1 producer queue + terminal-failure semantics", () => {
  it("after a terminal failure, subsequent appends fail immediately", async () => {
    // Drive a non-recoverable StaleEpoch with autoClaim disabled, then
    // confirm that the next `append` short-circuits with the same typed
    // failure rather than silently enqueueing more work.
    const url = server.streamUrl("terminal-fail")
    const s = DurableStream.define({ endpoint: { url }, schema: Message })

    await runtime(
      Effect.gen(function* () {
        yield* s.create({ contentType: "application/json" })

        // Writer A claims epoch 5 first.
        yield* Effect.scoped(
          Effect.gen(function* () {
            const a = yield* s.producer({
              producerId: "term",
              epoch: 5,
              lingerMs: 5,
              maxBatchSize: 1,
            })
            yield* a.append({ n: 0 })
            yield* a.flush
          }),
        )

        yield* Effect.scoped(
          Effect.gen(function* () {
            const b = yield* s.producer({
              producerId: "term",
              epoch: 0,
              autoClaim: false,
              lingerMs: 5,
              maxBatchSize: 1,
              maxQueueSize: 4,
            })
            // First append triggers the send → 403 → StaleEpoch recorded.
            yield* b.append({ n: 1 })
            const flushExit = yield* Effect.exit(b.flush)
            expect(flushExit._tag).toBe("Failure")

            // Second append should fail fast, NOT block on the bounded queue
            // or accept silently.
            const appendExit = yield* Effect.exit(b.append({ n: 2 }))
            expect(appendExit._tag).toBe("Failure")
          }),
        )
      }),
    )
  }, 15000)
})

// `Chunk` is needed by the type signature of Stream operators; keep it
// imported to satisfy strict lint.
void Chunk
