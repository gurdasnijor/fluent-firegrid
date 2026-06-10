import { readFile } from "node:fs/promises"
import { FetchHttpClient } from "@effect/platform"
import { Effect, Layer, Schema, Stream } from "effect"
import { describe, expect, expectTypeOf, it } from "vitest"
import {
  CEL,
  DurableStreamClient,
  DurableStreamClientLayer,
  DurableStreamClientLayerFetch,
  ReadFrom,
} from "../../src/index.ts"
import { startTestServer } from "./test-server.ts"
import type { Scope } from "effect"
import type { SubscriptionClient } from "../../src/index.ts"

const Message = Schema.Struct({
  id: Schema.String,
  kind: Schema.String,
})

type Message = Schema.Schema.Type<typeof Message>

const StringArrayEvent = Schema.Array(Schema.String)

const run = <A, E>(
  eff: Effect.Effect<A, E, DurableStreamClient | Scope.Scope>,
) =>
  Effect.runPromise(
    Effect.scoped(eff).pipe(Effect.provide(DurableStreamClientLayerFetch)),
  )

describe("effect-client package and HttpApi conformance", () => {
  it("effect-client.PACKAGE.1 effect-client.PACKAGE.3 effect-client.PACKAGE.4 effect-client.PACKAGE.5 wires the renamed package to shared protocol only", async () => {
    const pkg = JSON.parse(
      await readFile(new URL("../../package.json", import.meta.url), "utf8"),
    ) as {
      readonly name: string
      readonly dependencies?: Record<string, string>
    }

    expect(pkg.name).toBe("effect-durable-client")
    expect(pkg.dependencies).not.toHaveProperty("@durable-streams/client")
    expect(pkg.dependencies).not.toHaveProperty("@durable-streams/server")
    expect(pkg.dependencies).not.toHaveProperty("effect-durable-streams")
  })

  it("effect-client.PACKAGE.2 effect-client.CONFORMANCE.1 effect-client.CONFORMANCE.4 preserves conformance over source compatibility", async () => {
    const exports = JSON.parse(
      await readFile(new URL("../../package.json", import.meta.url), "utf8"),
    ) as { readonly exports?: Record<string, unknown> }

    expect(exports.exports).toHaveProperty(".")
    expect(exports.exports).toHaveProperty("./CEL")
  })

  it("effect-client.PACKAGE.6 effect-client.LOG.10 effect-client.CONFORMANCE.12 uses client.stream(path, schema) as the primary schema-bound projection", async () => {
    const server = await startTestServer()
    try {
      const url = server.streamUrl("effect-typed-messages")
      await run(
        Effect.gen(function* () {
          const client = yield* DurableStreamClient
          const messages = client.stream(url, Message)
          expect(messages.path).toBe(url)

          yield* messages.create({ contentType: "application/json" })
          yield* messages.append({ id: "1", kind: "typed" })

          const values = yield* messages
            .read({ from: ReadFrom.beginning, until: "tail" })
            .pipe(
              Stream.runCollect,
              Effect.map((chunk) => Array.from(chunk)),
            )
          expect(values).toEqual([{ id: "1", kind: "typed" }])
        }),
      )
    } finally {
      await server.stop()
    }
  })
})

describe("effect-client read-only stream surface", () => {
  it("effect-client.LOG.1 effect-client.LOG.2 effect-client.LOG.3 effect-client.LOG.4 effect-client.LOG.5 effect-client.LOG.6 effect-client.LOG.7 effect-client.CONFORMANCE.2 exposes a read-only handle over a real server", async () => {
    const server = await startTestServer()
    try {
      const url = server.streamUrl("effect-readonly")
      await run(
        Effect.gen(function* () {
          const client = yield* DurableStreamClient
          yield* client.create(url, { contentType: "application/json" })
          yield* client.append(url, JSON.stringify({ id: "1", kind: "seen" }))

          const readonly = client.readonlyStream(url, Message)
          const full = client.stream(url, Message)

          expect("producer" in readonly).toBe(false)
          expect("producer" in full).toBe(true)

          const values = yield* readonly.read({ until: "tail" }).pipe(
            Stream.runCollect,
            Effect.map((chunk) => Array.from(chunk)),
          )
          expect(values).toEqual([{ id: "1", kind: "seen" }])

          const controls = yield* readonly
            .readWithControl({ until: "tail" })
            .pipe(
              Stream.runCollect,
              Effect.map((chunk) => Array.from(chunk)),
            )
          expect(controls.some((event) => event._tag === "UpToDate")).toBe(true)
        }),
      )
    } finally {
      await server.stop()
    }
  })

  it("effect-client.LOG.8 effect-client.LOG.9 effect-client.CONFORMANCE.11 pins beginning and now read sentinels", async () => {
    const server = await startTestServer()
    try {
      const url = server.streamUrl("effect-readfrom")
      await run(
        Effect.gen(function* () {
          const client = yield* DurableStreamClient
          yield* client.create(url, { contentType: "application/json" })
          yield* client.append(url, JSON.stringify({ id: "1", kind: "old" }))

          const stream = client.readonlyStream(url, Message)
          const fromBeginning = yield* stream
            .read({ from: ReadFrom.beginning, until: "tail" })
            .pipe(
              Stream.runCollect,
              Effect.map((chunk) => Array.from(chunk)),
            )
          const fromNow = yield* stream
            .read({ from: ReadFrom.now, until: "tail" })
            .pipe(
              Stream.runCollect,
              Effect.map((chunk) => Array.from(chunk)),
            )

          expect(fromBeginning).toEqual([{ id: "1", kind: "old" }])
          expect(fromNow).toEqual([])
        }),
      )
    } finally {
      await server.stop()
    }
  })
})

describe("effect-client append shape conformance", () => {
  it("effect-client.PRODUCER.8 effect-client.CONFORMANCE.9 appends an array-valued JSON event as one event", async () => {
    const server = await startTestServer()
    try {
      const url = server.streamUrl("effect-array-event")
      await run(
        Effect.gen(function* () {
          const client = yield* DurableStreamClient
          const arrays = client.stream(url, StringArrayEvent)
          yield* client.create(url, { contentType: "application/json" })
          yield* arrays.append(["a", "b"])

          const values = yield* arrays.read({ until: "tail" }).pipe(
            Stream.runCollect,
            Effect.map((chunk) => Array.from(chunk)),
          )
          expect(values).toEqual([["a", "b"]])
        }),
      )
    } finally {
      await server.stop()
    }
  })

  it("effect-client.PRODUCER.8 effect-client.CONFORMANCE.10 appends multiple JSON events only through explicit batch append", async () => {
    const server = await startTestServer()
    try {
      const url = server.streamUrl("effect-batch-events")
      await run(
        Effect.gen(function* () {
          const client = yield* DurableStreamClient
          const arrays = client.stream(url, StringArrayEvent)
          yield* client.create(url, { contentType: "application/json" })
          yield* arrays.appendBatch([
            ["a", "b"],
            ["c", "d"],
          ])

          const values = yield* arrays.read({ until: "tail" }).pipe(
            Stream.runCollect,
            Effect.map((chunk) => Array.from(chunk)),
          )
          expect(values).toEqual([
            ["a", "b"],
            ["c", "d"],
          ])
        }),
      )
    } finally {
      await server.stop()
    }
  })
})

describe("effect-client producer recovery conformance", () => {
  it("effect-client.PRODUCER.1 effect-client.PRODUCER.2 effect-client.PRODUCER.3 effect-client.PRODUCER.4 effect-client.PRODUCER.5 effect-client.PRODUCER.6 effect-client.PRODUCER.7 effect-client.CONFORMANCE.5 effect-client.CONFORMANCE.6 starts fresh producer epoch sequence without reading event count", async () => {
    const requests: Array<Request> = []
    const fakeFetch: typeof globalThis.fetch = (
      input: RequestInfo | URL,
      init?: RequestInit,
    ) => {
      const request =
        input instanceof Request ? input : new Request(input, init)
      requests.push(request)
      return Promise.resolve(
        new Response("", {
          status: 200,
          headers: { "stream-next-offset": "opaque-offset-b" },
        }),
      )
    }

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const client = yield* DurableStreamClient
          const stream = client.stream(
            "https://server.example/v1/stream/events",
            Message,
          )
          const producer = yield* stream.producer("writer", {
            lingerMs: 0,
            maxBatchSize: 1,
          })
          yield* producer.append({ id: "1", kind: "created" })
          yield* producer.flush
        }).pipe(
          Effect.provide(DurableStreamClientLayer),
          Effect.provide(FetchHttpClient.layer),
          Effect.provide(Layer.succeed(FetchHttpClient.Fetch, fakeFetch)),
        ),
      ),
    )

    expect(requests.map((request) => request.method)).toEqual(["POST"])
    expect(requests[0]?.headers.get("producer-id")).toBe("writer")
    expect(requests[0]?.headers.get("producer-epoch")).toBe("0")
    expect(requests[0]?.headers.get("producer-seq")).toBe("0")
  })
})

describe("effect-client CEL conformance", () => {
  it("effect-client.CONFORMANCE.3 effect-client.CONFORMANCE.7 effect-client.CONFORMANCE.8 serializes branded CEL without local predicate evaluation", async () => {
    await run(
      Effect.gen(function* () {
        const client = yield* DurableStreamClient
        const filter = CEL.and(
          CEL.eq(CEL.path("event", "kind"), "ready"),
          CEL.raw("event.tenant == self.tenant"),
        )
        const config = client.subscriptions.filteredPullWakeConfig({
          streamPath: "events/source",
          wakeStream: "wake/pool",
          filter,
          self: { tenant: "acme" },
        })

        expect(config.filter).toEqual({
          language: "cel",
          expression: "event.kind == \"ready\" && event.tenant == self.tenant",
          self: { tenant: "acme" },
        })
        expect(config.filter).not.toHaveProperty("predicate")
      }),
    )
  })

  it("effect-client.CONFORMANCE.8 brands raw CEL strings before filtered helpers accept them", () => {
    const branded = CEL.raw("event.kind == 'ready'")
    expectTypeOf(branded).not.toEqualTypeOf<string>()

    const acceptsFilteredConfig = (
      opts: Parameters<SubscriptionClient["filteredPullWakeConfig"]>[0],
    ) => opts

    const rejected = acceptsFilteredConfig({
      streamPath: "events/source",
      wakeStream: "wake/pool",
      // @ts-expect-error effect-client.CONFORMANCE.8
      filter: "event.kind == 'ready'",
    })

    expect(rejected.filter).toBe("event.kind == 'ready'")
  })
})
