// Test fixture: layers a fake `FetchHttpClient.Fetch` under the http client via two
// scoped provides — readable + correct for this package fixture.
// @effect-diagnostics effect/multipleEffectProvide:off
import { FetchHttpClient, type HttpClient } from "@effect/platform"
import { Effect, Layer, type Scope } from "effect"
import { describe, expect, it } from "vitest"
import { DurableStream } from "effect-durable-streams"
import {
  FluentEventIngress,
  FluentEventIngressLive,
  FluentRuntimeError,
  FluentSources,
  FluentSourcesLive,
  FluentStore,
  FluentStoreLive,
} from "../src/index.ts"

type Reqs =
  | FetchHttpClient.Fetch
  | HttpClient.HttpClient
  | Scope.Scope
  | FluentStore
  | FluentSources
  | FluentEventIngress

const lastOffset = (events: ReadonlyArray<unknown>): string =>
  events.length === 0 ? "-1" : String(events.length - 1)

const parseOffset = (raw: string | null): number =>
  raw === null || raw === "-1" ? -1 : Number(raw)

const makeMemoryDurableStreamsFetch = (): typeof globalThis.fetch => {
  interface StreamState {
    readonly events: Array<unknown>
    closed: boolean
    readonly producers: Map<string, { epoch: number; lastSeq: number }>
  }

  const streams = new Map<string, StreamState>()

  const streamHeaders = (stream: StreamState) => ({
    "content-type": "application/json",
    "stream-next-offset": lastOffset(stream.events),
    "stream-closed": String(stream.closed),
  })

  return async (
    input: RequestInfo | URL,
    init?: RequestInit,
  ): Promise<Response> => {
    const request = input instanceof Request ? input : new Request(input, init)
    const url = new URL(request.url)
    const streamKey = url.pathname
    const method = request.method.toUpperCase()

    if (method === "PUT") {
      const exists = streams.has(streamKey)
      if (!exists) {
        const forkedFrom = request.headers.get("stream-forked-from")
        const forkOffset = request.headers.get("stream-fork-offset")
        const parent = forkedFrom === null ? undefined : streams.get(forkedFrom)
        if (forkedFrom !== null && parent === undefined) {
          return new Response("", { status: 404 })
        }
        const forkEnd = forkOffset === null ? parent?.events.length ?? 0 : parseOffset(forkOffset) + 1
        const inheritedEvents = parent === undefined
          ? []
          : parent.events.slice(0, Math.max(0, forkEnd))
        streams.set(streamKey, {
          events: inheritedEvents.slice(),
          closed: false,
          producers: new Map(),
        })
      }
      const stream = streams.get(streamKey)
      return new Response("", {
        status: exists ? 200 : 201,
        headers: stream === undefined
          ? { "content-type": "application/json" }
          : streamHeaders(stream),
      })
    }

    const stream = streams.get(streamKey)
    if (stream === undefined) return new Response("", { status: 404 })

    if (method === "HEAD") {
      return new Response("", {
        status: 200,
        headers: streamHeaders(stream),
      })
    }

    if (method === "POST") {
      if (stream.closed) {
        return new Response("", {
          status: 409,
          headers: {
            "stream-next-offset": lastOffset(stream.events),
            "stream-closed": "true",
          },
        })
      }
      const body = await request.text()
      const parsed: unknown = body.trim() === "" ? [] : JSON.parse(body)
      const batch: ReadonlyArray<unknown> = Array.isArray(parsed) ? parsed : [parsed]
      const producerId = request.headers.get("producer-id")
      const producerEpoch = Number(request.headers.get("producer-epoch") ?? "0")
      const producerSeq = Number(request.headers.get("producer-seq") ?? "0")
      if (producerId !== null) {
        const current = stream.producers.get(producerId) ?? { epoch: 0, lastSeq: -1 }
        if (producerEpoch < current.epoch) {
          return new Response("", {
            status: 403,
            headers: {
              ...streamHeaders(stream),
              "producer-epoch": String(current.epoch),
            },
          })
        }
        const base = producerEpoch > current.epoch
          ? { epoch: producerEpoch, lastSeq: -1 }
          : current
        const expectedSeq = base.lastSeq + 1
        if (producerSeq < expectedSeq) {
          return new Response(null, {
            status: 204,
            headers: streamHeaders(stream),
          })
        }
        if (producerSeq > expectedSeq) {
          return new Response("", {
            status: 409,
            headers: {
              ...streamHeaders(stream),
              "producer-expected-seq": String(expectedSeq),
              "producer-received-seq": String(producerSeq),
            },
          })
        }
        stream.producers.set(producerId, {
          epoch: producerEpoch,
          lastSeq: producerSeq,
        })
      }
      for (let index = 0; index < batch.length; index += 1) {
        stream.events.push(batch[index])
      }
      if (request.headers.get("stream-closed") === "true") {
        stream.closed = true
      }
      return new Response("", {
        status: 200,
        headers: streamHeaders(stream),
      })
    }

    if (method === "GET") {
      const offset = parseOffset(url.searchParams.get("offset"))
      return new Response(JSON.stringify(stream.events.slice(offset + 1)), {
        status: 200,
        headers: {
          ...streamHeaders(stream),
          "stream-up-to-date": "true",
        },
      })
    }

    return new Response("", { status: 405 })
  }
}

const runtimeWith = <A, E>(
  fakeFetch: typeof globalThis.fetch,
  effect: Effect.Effect<A, E, Reqs>,
): Promise<A> =>
  Effect.runPromise(
    Effect.scoped(
      effect.pipe(
        Effect.provide(FluentEventIngressLive),
        Effect.provide(FluentSourcesLive),
        Effect.provide(FluentStoreLive({
          durableStreamsBaseUrl: "https://durable.example",
          namespace: "fluent-runtime-test",
        })),
        Effect.provide(FetchHttpClient.layer),
        Effect.provide(Layer.succeed(FetchHttpClient.Fetch, fakeFetch)),
      ),
    ),
  )

describe("@firegrid/fluent-runtime Store", () => {
  const prMergedPredicate =
    "event.type == \"github.pr\" && event.value.state == \"merged\" && event.value.issueId == self.issueId"

  it("completes a turn by append-and-close and reads closure", async () => {
    const fakeFetch = makeMemoryDurableStreamsFetch()

    const read = await runtimeWith(
      fakeFetch,
      Effect.gen(function* () {
        const store = yield* FluentStore
        yield* store.createSession({
          sessionId: "session-1",
          agent: "agent",
        })
        yield* store.startTurn({
          sessionId: "session-1",
          turnId: "turn-1",
          prompt: "hello",
        })
        yield* store.completeTurn({
          sessionId: "session-1",
          turnId: "turn-1",
          result: { ok: true },
        })
        return yield* store.readTurn("session-1", "turn-1")
      }),
    )

    expect(read.streamClosed).toBe(true)
    expect(read.events.map((event) => event.type)).toEqual([
      "turn.started",
      "turn.completed",
    ])
  })

  it("deduplicates fenced session appends by producer id epoch and 0-based seq", async () => {
    const fakeFetch = makeMemoryDurableStreamsFetch()

    const result = await runtimeWith(
      fakeFetch,
      Effect.gen(function* () {
        const store = yield* FluentStore
        yield* store.createSession({
          sessionId: "session-fenced",
          agent: "agent",
        })
        const first = yield* store.appendSessionEventFenced({
          sessionId: "session-fenced",
          name: "side-effect",
          payload: { value: "first" },
          fence: { producerId: "session-fenced-writer", epoch: 0, seq: 0 },
        })
        const duplicate = yield* store.appendSessionEventFenced({
          sessionId: "session-fenced",
          name: "side-effect",
          payload: { value: "duplicate" },
          fence: { producerId: "session-fenced-writer", epoch: 0, seq: 0 },
        })
        const events = yield* store.collectSession("session-fenced")
        return { first, duplicate, events }
      }),
    )

    expect(result.first.write._tag).toBe("Appended")
    expect(result.duplicate.write._tag).toBe("Duplicate")
    expect(result.events).toHaveLength(2)
    expect(result.events[1]).toEqual({
      type: "session.event_appended",
      sessionId: "session-fenced",
      name: "side-effect",
      payload: { value: "first" },
    })
  })

  it("rejects fenced session appends that skip the initial zero seq", async () => {
    const fakeFetch = makeMemoryDurableStreamsFetch()

    const exit = await runtimeWith(
      fakeFetch,
      Effect.gen(function* () {
        const store = yield* FluentStore
        yield* store.createSession({
          sessionId: "session-gap",
          agent: "agent",
        })
        return yield* store.appendSessionEventFenced({
          sessionId: "session-gap",
          name: "side-effect",
          payload: { value: "gap" },
          fence: { producerId: "session-gap-writer", epoch: 0, seq: 1 },
        }).pipe(Effect.exit)
      }),
    )

    expect(exit._tag).toBe("Failure")
    if (exit._tag === "Failure") {
      expect(exit.cause._tag).toBe("Fail")
      if (exit.cause._tag === "Fail") {
        expect(exit.cause.error).toBeInstanceOf(FluentRuntimeError)
        expect(exit.cause.error.cause).toBeInstanceOf(DurableStream.SequenceGap)
      }
    }
  })

  it("records durable timer intent and dedupes timer-source fire events", async () => {
    const fakeFetch = makeMemoryDurableStreamsFetch()

    const result = await runtimeWith(
      fakeFetch,
      Effect.gen(function* () {
        const store = yield* FluentStore
        yield* store.createSession({
          sessionId: "timer-session",
          agent: "agent",
        })
        yield* store.startTurn({
          sessionId: "timer-session",
          turnId: "turn-timer",
          prompt: "wait",
        })
        const schedule = yield* store.scheduleTurnTimer({
          sessionId: "timer-session",
          turnId: "turn-timer",
          timerId: "sleep-1",
          fireAtEpochMs: 1_000,
        })
        const duplicateSchedule = yield* store.scheduleTurnTimer({
          sessionId: "timer-session",
          turnId: "turn-timer",
          timerId: "sleep-1",
          fireAtEpochMs: 1_000,
        })
        const first = yield* store.fireTurnTimer({
          sessionId: "timer-session",
          turnId: "turn-timer",
          timerId: "sleep-1",
          firedAtEpochMs: 1_000,
        })
        const duplicate = yield* store.fireTurnTimer({
          sessionId: "timer-session",
          turnId: "turn-timer",
          timerId: "sleep-1",
          firedAtEpochMs: 1_000,
        })
        yield* store.scheduleTurnTimer({
          sessionId: "timer-session",
          turnId: "turn-timer",
          timerId: "sleep-2",
          fireAtEpochMs: 2_000,
        })
        const secondTimer = yield* store.fireTurnTimer({
          sessionId: "timer-session",
          turnId: "turn-timer",
          timerId: "sleep-2",
          firedAtEpochMs: 2_000,
        })
        const read = yield* store.readTurn("timer-session", "turn-timer")
        return { schedule, duplicateSchedule, first, duplicate, secondTimer, read }
      }),
    )

    expect(result.schedule.write._tag).toBe("Appended")
    expect(result.duplicateSchedule.write._tag).toBe("Duplicate")
    expect(result.first.write._tag).toBe("Appended")
    expect(result.duplicate.write._tag).toBe("Duplicate")
    expect(result.secondTimer.write._tag).toBe("Appended")
    expect(result.read.events.map((event) => event.type)).toEqual([
      "turn.started",
      "turn.timer_scheduled",
      "turn.timer_fired",
      "turn.timer_scheduled",
      "turn.timer_fired",
    ])
    expect(result.read.events[1]).toEqual({
      type: "turn.timer_scheduled",
      sessionId: "timer-session",
      turnId: "turn-timer",
      timerId: "sleep-1",
      fireAtEpochMs: 1_000,
    })
    expect(result.read.events[2]).toEqual({
      type: "turn.timer_fired",
      sessionId: "timer-session",
      turnId: "turn-timer",
      timerId: "sleep-1",
      firedAtEpochMs: 1_000,
    })
    expect(result.read.events[3]).toEqual({
      type: "turn.timer_scheduled",
      sessionId: "timer-session",
      turnId: "turn-timer",
      timerId: "sleep-2",
      fireAtEpochMs: 2_000,
    })
    expect(result.read.events[4]).toEqual({
      type: "turn.timer_fired",
      sessionId: "timer-session",
      turnId: "turn-timer",
      timerId: "sleep-2",
      firedAtEpochMs: 2_000,
    })
  })

  it("records timer intent and fired rows with producer dedupe", async () => {
    const fakeFetch = makeMemoryDurableStreamsFetch()

    const result = await runtimeWith(
      fakeFetch,
      Effect.gen(function* () {
        const store = yield* FluentStore
        yield* store.createSession({
          sessionId: "sleep-session",
          agent: "agent",
        })
        yield* store.startTurn({
          sessionId: "sleep-session",
          turnId: "sleep-turn",
          prompt: "sleep",
        })
        const scheduled = yield* store.scheduleTurnTimer({
          sessionId: "sleep-session",
          turnId: "sleep-turn",
          timerId: "sleep-wait",
          fireAtEpochMs: 10_000,
        })
        yield* store.fireTurnTimer({
          sessionId: "sleep-session",
          turnId: "sleep-turn",
          timerId: "sleep-wait",
          firedAtEpochMs: 10_000,
        })
        const duplicateSchedule = yield* store.scheduleTurnTimer({
          sessionId: "sleep-session",
          turnId: "sleep-turn",
          timerId: "sleep-wait",
          fireAtEpochMs: 10_000,
        })
        const read = yield* store.readTurn("sleep-session", "sleep-turn")
        return { scheduled, duplicateSchedule, read }
      }),
    )

    expect(result.scheduled.write._tag).toBe("Appended")
    expect(result.duplicateSchedule.write._tag).toBe("Duplicate")
    expect(result.read.events.map((event) => event.type)).toEqual([
      "turn.started",
      "turn.timer_scheduled",
      "turn.timer_fired",
    ])
  })

  it("materializes only due timers from the timer source and reports replayed fires", async () => {
    const fakeFetch = makeMemoryDurableStreamsFetch()

    const result = await runtimeWith(
      fakeFetch,
      Effect.gen(function* () {
        const store = yield* FluentStore
        const sources = yield* FluentSources
        yield* store.createSession({
          sessionId: "timer-source-session",
          agent: "agent",
        })
        yield* store.startTurn({
          sessionId: "timer-source-session",
          turnId: "timer-source-turn",
          prompt: "source",
        })
        yield* store.scheduleTurnTimer({
          sessionId: "timer-source-session",
          turnId: "timer-source-turn",
          timerId: "due",
          fireAtEpochMs: 100,
        })
        yield* store.scheduleTurnTimer({
          sessionId: "timer-source-session",
          turnId: "timer-source-turn",
          timerId: "later",
          fireAtEpochMs: 500,
        })
        const first = yield* sources.fireDueTurnTimers({
          sessionId: "timer-source-session",
          turnId: "timer-source-turn",
          nowEpochMs: 150,
        })
        const second = yield* sources.fireDueTurnTimers({
          sessionId: "timer-source-session",
          turnId: "timer-source-turn",
          nowEpochMs: 150,
        })
        const read = yield* store.readTurn("timer-source-session", "timer-source-turn")
        return { first, second, read }
      }),
    )

    expect(result.first.fired).toEqual([
      {
        timerId: "due",
        fireAtEpochMs: 100,
        firedAtEpochMs: 150,
        write: { _tag: "Appended", offset: "3" },
      },
    ])
    expect(result.first.pending).toEqual([{ timerId: "later", fireAtEpochMs: 500 }])
    expect(result.first.alreadyFired).toEqual([])
    expect(result.second.fired).toEqual([])
    expect(result.second.pending).toEqual([{ timerId: "later", fireAtEpochMs: 500 }])
    expect(result.second.alreadyFired).toEqual([{
      timerId: "due",
      fireAtEpochMs: 100,
      firedAtEpochMs: 150,
    }])
    expect(result.read.events.map((event) => event.type)).toEqual([
      "turn.started",
      "turn.timer_scheduled",
      "turn.timer_scheduled",
      "turn.timer_fired",
    ])
  })

  it("registers wait intent and records a matching event row", async () => {
    const fakeFetch = makeMemoryDurableStreamsFetch()

    const result = await runtimeWith(
      fakeFetch,
      Effect.gen(function* () {
        const store = yield* FluentStore
        yield* store.createSession({
          sessionId: "wait-session",
          agent: "agent",
        })
        yield* store.startTurn({
          sessionId: "wait-session",
          turnId: "wait-turn",
          prompt: "wait_for",
        })
        const registered = yield* store.registerTurnWait({
          sessionId: "wait-session",
          turnId: "wait-turn",
          waitId: "github-pr-merged",
          predicate: prMergedPredicate,
          afterOffset: "3",
          self: { issueId: "ISS-1" },
        })
        const staleOffset = yield* store.matchTurnWait({
          sessionId: "wait-session",
          turnId: "wait-turn",
          waitId: "github-pr-merged",
          matchedOffset: "3",
          event: {
            type: "github.pr",
            key: "pr-000",
            value: { state: "merged", issueId: "ISS-1" },
            headers: { operation: "update" },
          },
        })
        const nonMatch = yield* store.matchTurnWait({
          sessionId: "wait-session",
          turnId: "wait-turn",
          waitId: "github-pr-merged",
          matchedOffset: "4",
          event: {
            type: "github.pr",
            key: "pr-456",
            value: { state: "merged", issueId: "ISS-2" },
            headers: { operation: "update" },
          },
        })
        const match = yield* store.matchTurnWait({
          sessionId: "wait-session",
          turnId: "wait-turn",
          waitId: "github-pr-merged",
          matchedOffset: "5",
          event: {
            type: "github.pr",
            key: "pr-123",
            value: { state: "merged", issueId: "ISS-1" },
            headers: { operation: "update" },
          },
        })
        const read = yield* store.readTurn("wait-session", "wait-turn")
        return { registered, staleOffset, nonMatch, match, read }
      }),
    )

    expect(result.registered.write._tag).toBe("Appended")
    expect(result.staleOffset._tag).toBe("NotMatched")
    expect(result.nonMatch._tag).toBe("NotMatched")
    expect(result.match._tag).toBe("Matched")
    if (result.match._tag === "Matched") {
      expect(result.match.write._tag).toBe("Appended")
    }
    expect(result.read.events.map((event) => event.type)).toEqual([
      "turn.started",
      "turn.wait_registered",
      "turn.wait_matched",
    ])
    expect(result.read.events[1]).toEqual({
      type: "turn.wait_registered",
      sessionId: "wait-session",
      turnId: "wait-turn",
      waitId: "github-pr-merged",
      predicate: prMergedPredicate,
      afterOffset: "3",
      self: { issueId: "ISS-1" },
    })
    expect(result.read.events[2]).toEqual({
      type: "turn.wait_matched",
      sessionId: "wait-session",
      turnId: "wait-turn",
      waitId: "github-pr-merged",
      matchedOffset: "5",
      event: {
        type: "github.pr",
        key: "pr-123",
        value: { state: "merged", issueId: "ISS-1" },
        headers: { operation: "update" },
      },
    })
  })

  it("matches waits with real Durable Streams composite offsets", async () => {
    const fakeFetch = makeMemoryDurableStreamsFetch()

    const result = await runtimeWith(
      fakeFetch,
      Effect.gen(function* () {
        const store = yield* FluentStore
        yield* store.createSession({
          sessionId: "wait-composite-session",
          agent: "agent",
        })
        yield* store.startTurn({
          sessionId: "wait-composite-session",
          turnId: "wait-composite-turn",
          prompt: "wait_for",
        })
        yield* store.registerTurnWait({
          sessionId: "wait-composite-session",
          turnId: "wait-composite-turn",
          waitId: "composite-offset-wait",
          predicate: "event.type == \"review.posted\"",
          afterOffset: "0000000000000000_0000000000000001",
        })
        const stale = yield* store.matchTurnWait({
          sessionId: "wait-composite-session",
          turnId: "wait-composite-turn",
          waitId: "composite-offset-wait",
          matchedOffset: "0000000000000000_0000000000000001",
          event: {
            type: "review.posted",
            key: "review/stale",
            value: {},
            headers: { operation: "external" },
          },
        })
        const matched = yield* store.matchTurnWait({
          sessionId: "wait-composite-session",
          turnId: "wait-composite-turn",
          waitId: "composite-offset-wait",
          matchedOffset: "0000000000000000_0000000000000002",
          event: {
            type: "review.posted",
            key: "review/matched",
            value: {},
            headers: { operation: "external" },
          },
        })
        const turn = yield* store.readTurn("wait-composite-session", "wait-composite-turn")
        return { stale, matched, turn }
      }),
    )

    expect(result.stale._tag).toBe("NotMatched")
    expect(result.matched._tag).toBe("Matched")
    expect(result.turn.events.at(-1)).toEqual({
      type: "turn.wait_matched",
      sessionId: "wait-composite-session",
      turnId: "wait-composite-turn",
      waitId: "composite-offset-wait",
      matchedOffset: "0000000000000000_0000000000000002",
      event: {
        type: "review.posted",
        key: "review/matched",
        value: {},
        headers: { operation: "external" },
      },
    })
  })

  it("fans one candidate event across pending CEL waits from the wait source", async () => {
    const fakeFetch = makeMemoryDurableStreamsFetch()

    const result = await runtimeWith(
      fakeFetch,
      Effect.gen(function* () {
        const store = yield* FluentStore
        const sources = yield* FluentSources
        yield* store.createSession({
          sessionId: "wait-source-session",
          agent: "agent",
        })
        yield* store.startTurn({
          sessionId: "wait-source-session",
          turnId: "wait-source-turn",
          prompt: "wait_for",
        })
        yield* store.registerTurnWait({
          sessionId: "wait-source-session",
          turnId: "wait-source-turn",
          waitId: "issue-1",
          predicate: prMergedPredicate,
          afterOffset: "10",
          self: { issueId: "ISS-1" },
        })
        yield* store.registerTurnWait({
          sessionId: "wait-source-session",
          turnId: "wait-source-turn",
          waitId: "issue-2",
          predicate: prMergedPredicate,
          afterOffset: "10",
          self: { issueId: "ISS-2" },
        })
        const stale = yield* sources.matchPendingTurnWaits({
          sessionId: "wait-source-session",
          turnId: "wait-source-turn",
          matchedOffset: "10",
          event: {
            type: "github.pr",
            key: "pr-123",
            value: { state: "merged", issueId: "ISS-1" },
            headers: { operation: "update" },
          },
        })
        const first = yield* sources.matchPendingTurnWaits({
          sessionId: "wait-source-session",
          turnId: "wait-source-turn",
          matchedOffset: "11",
          event: {
            type: "github.pr",
            key: "pr-123",
            value: { state: "merged", issueId: "ISS-1" },
            headers: { operation: "update" },
          },
        })
        const second = yield* sources.matchPendingTurnWaits({
          sessionId: "wait-source-session",
          turnId: "wait-source-turn",
          matchedOffset: "12",
          event: {
            type: "github.pr",
            key: "pr-456",
            value: { state: "merged", issueId: "ISS-2" },
            headers: { operation: "update" },
          },
        })
        const read = yield* store.readTurn("wait-source-session", "wait-source-turn")
        return { stale, first, second, read }
      }),
    )

    expect(result.stale.matched).toEqual([])
    expect(result.stale.notMatched).toEqual([{ waitId: "issue-1" }, { waitId: "issue-2" }])
    expect(result.first.matched).toEqual([{ waitId: "issue-1", write: { _tag: "Appended", offset: "3" } }])
    expect(result.first.notMatched).toEqual([{ waitId: "issue-2" }])
    expect(result.second.matched).toEqual([{ waitId: "issue-2", write: { _tag: "Appended", offset: "4" } }])
    expect(result.second.alreadyMatched).toEqual([{ waitId: "issue-1", matchedOffset: "11" }])
    expect(result.read.events.map((event) => event.type)).toEqual([
      "turn.started",
      "turn.wait_registered",
      "turn.wait_registered",
      "turn.wait_matched",
      "turn.wait_matched",
    ])
  })

  it("fluent-event-ingress: External delivery becomes a durable event", async () => {
    const fakeFetch = makeMemoryDurableStreamsFetch()

    const result = await runtimeWith(
      fakeFetch,
      Effect.gen(function* () {
        const store = yield* FluentStore
        const ingress = yield* FluentEventIngress
        yield* store.createSession({
          sessionId: "ingress-session",
          agent: "agent",
        })
        yield* store.startTurn({
          sessionId: "ingress-session",
          turnId: "ingress-turn",
          prompt: "wait_for",
        })
        yield* store.registerTurnWait({
          sessionId: "ingress-session",
          turnId: "ingress-turn",
          waitId: "review-wait",
          predicate: "event.type == \"review.posted\"",
          afterOffset: "-1",
        })
        const ingested = yield* ingress.ingestExternalEvent({
          sessionId: "ingress-session",
          turnId: "ingress-turn",
          deliveryId: "d-1",
          type: "review.posted",
          key: "review/d-1",
          value: { state: "posted" },
          source: "reviews",
        })
        const session = yield* store.collectSession("ingress-session")
        return { ingested, session }
      }),
    )

    expect(result.ingested._tag).toBe("Appended")
    expect(result.ingested.write).toEqual({ _tag: "Appended", offset: "1" })
    expect(result.session[1]).toEqual({
      type: "review.posted",
      key: "review/d-1",
      value: { state: "posted" },
      headers: {
        operation: "external",
        delivery_id: "d-1",
        producer_id: "fluent-runtime/event-ingress/reviews/d-1",
        source: "reviews",
      },
    })
  })

  it("fluent-event-ingress: Duplicate delivery is deduplicated", async () => {
    const fakeFetch = makeMemoryDurableStreamsFetch()

    const result = await runtimeWith(
      fakeFetch,
      Effect.gen(function* () {
        const store = yield* FluentStore
        const ingress = yield* FluentEventIngress
        yield* store.createSession({
          sessionId: "ingress-duplicate-session",
          agent: "agent",
        })
        yield* store.startTurn({
          sessionId: "ingress-duplicate-session",
          turnId: "ingress-duplicate-turn",
          prompt: "wait_for",
        })
        yield* store.registerTurnWait({
          sessionId: "ingress-duplicate-session",
          turnId: "ingress-duplicate-turn",
          waitId: "review-wait",
          predicate: "event.type == \"review.posted\"",
          afterOffset: "-1",
        })
        const first = yield* ingress.ingestExternalEvent({
          sessionId: "ingress-duplicate-session",
          turnId: "ingress-duplicate-turn",
          deliveryId: "d-1",
          type: "review.posted",
          key: "review/d-1",
          value: { state: "posted" },
          source: "reviews",
        })
        const duplicate = yield* ingress.ingestExternalEvent({
          sessionId: "ingress-duplicate-session",
          turnId: "ingress-duplicate-turn",
          deliveryId: "d-1",
          type: "review.posted",
          key: "review/d-1",
          value: { state: "posted" },
          source: "reviews",
        })
        const session = yield* store.collectSession("ingress-duplicate-session")
        const turn = yield* store.readTurn("ingress-duplicate-session", "ingress-duplicate-turn")
        return { first, duplicate, session, turn }
      }),
    )

    expect(result.first._tag).toBe("Appended")
    expect(result.duplicate._tag).toBe("Duplicate")
    expect(result.duplicate.waits.matched).toEqual([])
    expect(result.duplicate.redrive).toBe(false)
    expect(result.session.map((event) => event.type)).toEqual(["session.created", "review.posted"])
    expect(result.turn.events.filter((event) => event.type === "turn.wait_matched")).toHaveLength(1)
  })

  it("fluent-event-ingress: Matching webhook wakes a waiting session", async () => {
    const fakeFetch = makeMemoryDurableStreamsFetch()

    const result = await runtimeWith(
      fakeFetch,
      Effect.gen(function* () {
        const store = yield* FluentStore
        const ingress = yield* FluentEventIngress
        yield* store.createSession({
          sessionId: "ingress-match-session",
          agent: "agent",
        })
        yield* store.startTurn({
          sessionId: "ingress-match-session",
          turnId: "ingress-match-turn",
          prompt: "wait_for",
        })
        yield* store.registerTurnWait({
          sessionId: "ingress-match-session",
          turnId: "ingress-match-turn",
          waitId: "review-wait",
          predicate: "event.type == \"review.posted\"",
          afterOffset: "-1",
        })
        const ingested = yield* ingress.ingestExternalEvent({
          sessionId: "ingress-match-session",
          turnId: "ingress-match-turn",
          deliveryId: "d-match",
          type: "review.posted",
          key: "review/d-match",
          value: { state: "posted" },
          source: "reviews",
        })
        const turn = yield* store.readTurn("ingress-match-session", "ingress-match-turn")
        return { ingested, turn }
      }),
    )

    expect(result.ingested._tag).toBe("Appended")
    expect(result.ingested.redrive).toBe(true)
    expect(result.ingested.waits.matched.map((wait) => wait.waitId)).toEqual(["review-wait"])
    expect(result.turn.events.at(-1)).toEqual({
      type: "turn.wait_matched",
      sessionId: "ingress-match-session",
      turnId: "ingress-match-turn",
      waitId: "review-wait",
      matchedOffset: "1",
      event: result.ingested.change,
    })
  })

  it("fluent-event-ingress: Non-matching webhook does not wake unrelated waits", async () => {
    const fakeFetch = makeMemoryDurableStreamsFetch()

    const result = await runtimeWith(
      fakeFetch,
      Effect.gen(function* () {
        const store = yield* FluentStore
        const ingress = yield* FluentEventIngress
        yield* store.createSession({
          sessionId: "ingress-nonmatch-session",
          agent: "agent",
        })
        yield* store.startTurn({
          sessionId: "ingress-nonmatch-session",
          turnId: "ingress-nonmatch-turn",
          prompt: "wait_for",
        })
        yield* store.registerTurnWait({
          sessionId: "ingress-nonmatch-session",
          turnId: "ingress-nonmatch-turn",
          waitId: "pr-wait",
          predicate: "event.type == \"github.pr\"",
          afterOffset: "-1",
        })
        const ingested = yield* ingress.ingestExternalEvent({
          sessionId: "ingress-nonmatch-session",
          turnId: "ingress-nonmatch-turn",
          deliveryId: "d-issue",
          type: "github.issue",
          key: "issue/1",
          value: { state: "opened" },
          source: "github",
        })
        const turn = yield* store.readTurn("ingress-nonmatch-session", "ingress-nonmatch-turn")
        return { ingested, turn }
      }),
    )

    expect(result.ingested._tag).toBe("Appended")
    expect(result.ingested.redrive).toBe(false)
    expect(result.ingested.waits.notMatched).toEqual([{ waitId: "pr-wait" }])
    expect(result.turn.events.map((event) => event.type)).toEqual([
      "turn.started",
      "turn.wait_registered",
    ])
  })
})
