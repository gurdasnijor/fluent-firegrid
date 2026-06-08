// Test fixture: layers a fake `FetchHttpClient.Fetch` under the http client via two
// scoped provides — readable + correct; the production "combine provides" advice
// doesn't apply here.
// @effect-diagnostics effect/multipleEffectProvide:off
import { FetchHttpClient, type HttpClient } from "@effect/platform"
import { Data, Effect, Fiber, Layer, Schema, type Scope } from "effect"
import { describe, expect, it } from "vitest"
import {
  execute,
  all,
  run,
  service,
  workflow,
  type ExecutionContext,
  type Operation,
} from "../src/index.ts"
import { bindTestDefinition } from "./bind-test-definition.ts"

type Reqs = FetchHttpClient.Fetch | HttpClient.HttpClient | Scope.Scope
type MemoryDurableStreamsFetch = typeof globalThis.fetch & {
  readonly eventsFor: (url: string) => ReadonlyArray<unknown>
}

class TestFailure extends Schema.TaggedError<TestFailure>()("TestFailure", {
  message: Schema.String,
}) {}

class LocalControlError extends Data.TaggedError("LocalControlError")<{
  readonly message: string
}> {}

const lastOffset = (events: ReadonlyArray<unknown>): string =>
  events.length === 0 ? "-1" : String(events.length - 1)

const parseOffset = (raw: string | null): number =>
  raw === null || raw === "-1" ? -1 : Number(raw)

const makeMemoryDurableStreamsFetch = (): MemoryDurableStreamsFetch => {
  const streams = new Map<string, Array<unknown>>()
  const fetchImpl: typeof globalThis.fetch = async (
    input: RequestInfo | URL,
    init?: RequestInit,
  ): Promise<Response> => {
    const request = input instanceof Request ? input : new Request(input, init)
    const url = new URL(request.url)
    const streamKey = url.pathname
    const method = request.method.toUpperCase()

    if (method === "PUT") {
      const exists = streams.has(streamKey)
      if (!exists) streams.set(streamKey, [])
      return new Response("", {
        status: exists ? 200 : 201,
        headers: {
          "content-type": "application/json",
          "stream-next-offset": lastOffset(streams.get(streamKey) ?? []),
        },
      })
    }

    const events = streams.get(streamKey)
    if (events === undefined) {
      return new Response("", { status: 404 })
    }

    if (method === "POST") {
      const body = await request.text()
      const parsed: unknown = body.trim() === "" ? [] : JSON.parse(body)
      const batch: ReadonlyArray<unknown> = Array.isArray(parsed) ? parsed : [parsed]
      for (const event of batch) {
        events.push(event)
      }
      return new Response("", {
        status: 200,
        headers: { "stream-next-offset": lastOffset(events) },
      })
    }

    if (method === "GET") {
      const offset = parseOffset(url.searchParams.get("offset"))
      const items = events.slice(offset + 1)
      return new Response(JSON.stringify(items), {
        status: 200,
        headers: {
          "content-type": "application/json",
          "stream-next-offset": lastOffset(events),
          "stream-up-to-date": "true",
        },
      })
    }

    return new Response("", { status: 405 })
  }
  return Object.assign(fetchImpl, {
    eventsFor: (rawUrl: string) => streams.get(new URL(rawUrl).pathname) ?? [],
  })
}

const runtimeWith = <A, E>(
  fakeFetch: typeof globalThis.fetch,
  effect: Effect.Effect<A, E, Reqs>,
): Promise<A> =>
  Effect.runPromise(
    Effect.scoped(
      effect.pipe(
        Effect.provide(FetchHttpClient.layer),
        Effect.provide(Layer.succeed(FetchHttpClient.Fetch, fakeFetch)),
      ),
    ),
  )

const runtimeEitherWith = <A, E>(
  fakeFetch: typeof globalThis.fetch,
  effect: Effect.Effect<A, E, Reqs>,
) =>
  runtimeWith(fakeFetch, Effect.either(effect))

const invocation = (name: string): ExecutionContext => ({
  journal: {
    endpoint: {
      url: `https://journal.example/v1/stream/fluent-effect-core/${name}`,
    },
  },
})

describe("fluent-engine-substrate-free", () => {
  it("fluent-engine-substrate-free: named journal step is a plain Effect and replays from journal", async () => {
    const fakeFetch = makeMemoryDurableStreamsFetch()
    const executions = { count: 0 }
    const greeter = service({
      name: "greeter",
      handlers: {
        greet: (ctx: ExecutionContext, name: string) =>
          execute(
            ctx,
            run(
              "classify",
              Effect.sync(() => {
                executions.count += 1
                return `Hello, ${name}! run=${executions.count}`
              }),
            ),
          ),
      },
    })

    const first = await runtimeWith(fakeFetch, bindTestDefinition(greeter, invocation("greet")).greet("Ada"))
    const replayed = await runtimeWith(fakeFetch, bindTestDefinition(greeter, invocation("greet")).greet("Ada"))

    expect(first).toBe("Hello, Ada! run=1")
    expect(replayed).toBe("Hello, Ada! run=1")
    expect(executions.count).toBe(1)
  })

  it("fluent-engine-substrate-free: generator handlers provide Restate-like authoring affordances", async () => {
    const fakeFetch = makeMemoryDurableStreamsFetch()
    const executions: Array<string> = []
    const svc = service({
      name: "generatorBasics",
      handlers: {
        *hello(name: string): Operation<string> {
          return yield* run(() => {
            executions.push("hello")
            return `Hello, ${name}!`
          }, { name: "compose" })
        },
        *parallel(base: number): Operation<number> {
          const left = run(() => {
            executions.push("left")
            return base + 1
          }, { name: "left" })
          const right = run(() => {
            executions.push("right")
            return base + 2
          }, { name: "right" })
          const [leftValue, rightValue] = yield* all([left, right])
          return leftValue + rightValue
        },
      },
    })

    const hello = await runtimeWith(fakeFetch, bindTestDefinition(svc, invocation("generator-hello")).hello("Ada"))
    const parallel = await runtimeWith(fakeFetch, bindTestDefinition(svc, invocation("generator-parallel")).parallel(10))
    const replayed = await runtimeWith(fakeFetch, bindTestDefinition(svc, invocation("generator-parallel")).parallel(10))

    expect(hello).toBe("Hello, Ada!")
    expect(parallel).toBe(23)
    expect(replayed).toBe(23)
    expect(executions.sort()).toEqual(["hello", "left", "right"])
  })

  it("fluent-engine-substrate-free: Effect.all replaces Future combinators without positional counters", async () => {
    const fakeFetch = makeMemoryDurableStreamsFetch()
    const executions: Array<string> = []
    const calculator = service({
      name: "calculator",
      handlers: {
        addPair: (ctx: ExecutionContext, base: number) =>
          execute(
            ctx,
            Effect.gen(function* () {
              const [left, right] = yield* Effect.all([
                run("left", Effect.sync(() => {
                  executions.push("left")
                  return base + 1
                })),
                run("right", Effect.sync(() => {
                  executions.push("right")
                  return base + 2
                })),
              ], { concurrency: "unbounded" })
              return left + right
            }),
          ),
      },
    })

    const first = await runtimeWith(fakeFetch, bindTestDefinition(calculator, invocation("add-pair")).addPair(10))
    const replayed = await runtimeWith(fakeFetch, bindTestDefinition(calculator, invocation("add-pair")).addPair(10))

    expect(first).toBe(23)
    expect(replayed).toBe(23)
    expect(executions.sort()).toEqual(["left", "right"])
  })

  it("fluent-engine-substrate-free: Effect.fork represents local spawned work without child-session durability", async () => {
    const fakeFetch = makeMemoryDurableStreamsFetch()
    const svc = service({
      name: "forkSvc",
      handlers: {
        nested: (ctx: ExecutionContext, input: number) =>
          execute(
            ctx,
            Effect.gen(function* () {
              const child = yield* run("spawn-local", Effect.gen(function* () {
                const fiber = yield* Effect.fork(Effect.succeed(input + 1))
                return yield* Fiber.join(fiber)
              }))
              return child + 1
            }),
          ),
      },
    })

    await expect(runtimeWith(fakeFetch, bindTestDefinition(svc, invocation("fork")).nested(10)))
      .resolves.toBe(12)
  })

  it("fluent-engine-substrate-free: schema-encoded failures replay through the typed boundary", async () => {
    const fakeFetch = makeMemoryDurableStreamsFetch()
    const executions = { count: 0 }
    const svc = service({
      name: "failureSvc",
      handlers: {
        fail: (ctx: ExecutionContext, _: void) =>
          execute(
            ctx,
            run(
              "schema-failure",
              Effect.gen(function* () {
                executions.count += 1
                return yield* new TestFailure({ message: "nope" })
              }),
              { errorSchema: TestFailure },
            ),
          ),
      },
    })

    await expect(runtimeEitherWith(fakeFetch, bindTestDefinition(svc, invocation("failure")).fail(undefined)))
      .resolves.toMatchObject({
        _tag: "Left",
        left: { _tag: "TestFailure", message: "nope" },
      })
    await expect(runtimeEitherWith(fakeFetch, bindTestDefinition(svc, invocation("failure")).fail(undefined)))
      .resolves.toMatchObject({
        _tag: "Left",
        left: { _tag: "TestFailure", message: "nope" },
      })
    expect(executions.count).toBe(1)
  })

  it("fluent-engine-substrate-free: local control errors are not written as domain journal rows", async () => {
    const fakeFetch = makeMemoryDurableStreamsFetch()
    const svc = service({
      name: "controlErrorSvc",
      handlers: {
        fail: (ctx: ExecutionContext, _: void) =>
          execute(
            ctx,
            Effect.fail(new LocalControlError({ message: "parked" })),
          ),
      },
    })

    const ctx = invocation("control")
    await expect(runtimeEitherWith(fakeFetch, bindTestDefinition(svc, ctx).fail(undefined)))
      .resolves.toMatchObject({
        _tag: "Left",
        left: { _tag: "LocalControlError", message: "parked" },
      })
    expect(fakeFetch.eventsFor(String(ctx.journal.endpoint.url))).toEqual([])
  })

  it("fluent-engine-substrate-free: workflow definitions invoke Effect-native handlers", async () => {
    const fakeFetch = makeMemoryDurableStreamsFetch()
    const patchWorkflow = workflow({
      name: "patchWorkflow",
      handlers: {
        run: (ctx: ExecutionContext, title: string) =>
          execute(ctx, run("open-patch", Effect.succeed(`opened:${title}`))),
        status: (ctx: ExecutionContext, id: string) =>
          execute(ctx, run("read-status", Effect.succeed(`status:${id}:modeled`))),
      },
    })

    await expect(runtimeWith(fakeFetch, bindTestDefinition(patchWorkflow, invocation("workflow-run")).run("tf-29fy")))
      .resolves.toBe("opened:tf-29fy")
    await expect(runtimeWith(fakeFetch, bindTestDefinition(patchWorkflow, invocation("workflow-status")).status("tf-29fy")))
      .resolves.toBe("status:tf-29fy:modeled")
  })
})
