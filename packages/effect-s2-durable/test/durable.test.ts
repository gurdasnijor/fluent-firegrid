import { expect, layer } from "@effect/vitest"
import { Clock, Duration, Effect, Layer, Option, Schema } from "effect"
import { primaryKey, Table } from "effect-s2-stream-db"
import {
  awakeable,
  client,
  deferred,
  DurableExecutionRuntime,
  handler,
  handlerRequest,
  resolveAwakeable,
  resolveSignal,
  run,
  sendClient,
  service,
  signal,
  sleep,
  state,
} from "../src/index.ts"
import { S2LiteLive } from "./s2lite.ts"

// One long-lived engine for the whole suite, over a real s2 lite server.
const TestLive = DurableExecutionRuntime.layer.pipe(Layer.provide(S2LiteLive))

const GreetInput = Schema.Struct({ name: Schema.String })
const GreetOutput = Schema.Struct({ greeting: Schema.String, count: Schema.Number })

class BoomError extends Schema.TaggedErrorClass<BoomError>()("BoomError", { why: Schema.String }) {}

class Cart extends Table<Cart>("cart")({
  cartId: Schema.String.pipe(primaryKey),
  items: Schema.Array(Schema.String),
}) {}

const Approval = Schema.Struct({ approved: Schema.Boolean })

layer(TestLive, { excludeTestServices: true, timeout: Duration.seconds(40) })(
  "effect-s2-durable engine over s2 lite",
  (it) => {
    it.effect("submit + attach round-trips the decoded output", () =>
      Effect.gen(function*() {
        const sideEffects = { count: 0 }
        const greet = handler("greet", { input: GreetInput, output: GreetOutput })(
          Effect.gen(function*() {
            const req = yield* handlerRequest(GreetInput)
            const n = yield* run("bump", Effect.sync(() => ++sideEffects.count), { output: Schema.Number })
            return { greeting: `hi ${req.name}`, count: n }
          }),
        )
        const rt = yield* DurableExecutionRuntime
        yield* rt.submit(greet, "greet-1", { name: "ada" })
        const out = yield* rt.attach(greet, "greet-1")
        expect(out).toStrictEqual({ greeting: "hi ada", count: 1 })
        expect(sideEffects.count).toBe(1)
      }))

    it.effect("submit is idempotent for a live execution", () =>
      Effect.gen(function*() {
        const sideEffects = { count: 0 }
        const once = handler("once", { input: GreetInput, output: Schema.Number })(
          Effect.gen(function*() {
            yield* handlerRequest(GreetInput)
            return yield* run("bump", Effect.sync(() => ++sideEffects.count), { output: Schema.Number })
          }),
        )
        const rt = yield* DurableExecutionRuntime
        yield* rt.submit(once, "once-1", { name: "x" })
        yield* rt.submit(once, "once-1", { name: "x" }) // second submit is a no-op
        const out = yield* rt.attach(once, "once-1")
        expect(out).toBe(1)
        expect(sideEffects.count).toBe(1)
      }))

    it.effect("poll serves the completed result from the roster", () =>
      Effect.gen(function*() {
        const wf = handler("polled", { input: GreetInput, output: Schema.Number })(
          Effect.gen(function*() {
            yield* handlerRequest(GreetInput)
            return yield* run("answer", Effect.succeed(42), { output: Schema.Number })
          }),
        )
        const rt = yield* DurableExecutionRuntime
        yield* rt.submit(wf, "poll-1", { name: "y" })
        yield* rt.attach(wf, "poll-1") // ensure completion
        const polled = yield* rt.poll(wf, "poll-1")
        expect(Option.getOrNull(polled)).toBe(42)
      }))

    it.effect("a typed run failure propagates and attach fails", () =>
      Effect.gen(function*() {
        const boom = handler("boom", { input: GreetInput, output: Schema.Number })(
          Effect.gen(function*() {
            yield* handlerRequest(GreetInput)
            return yield* run("explode", Effect.fail(new BoomError({ why: "nope" })), {
              output: Schema.Number,
              error: BoomError,
            })
          }),
        )
        const rt = yield* DurableExecutionRuntime
        yield* rt.submit(boom, "boom-1", { name: "z" })
        const exit = yield* Effect.exit(rt.attach(boom, "boom-1"))
        expect(exit._tag).toBe("Failure")
      }))

    it.effect("state(Table) is a mutable durable record across steps", () =>
      Effect.gen(function*() {
        const shopper = handler("shopper", { input: GreetInput, output: Schema.Number })(
          Effect.gen(function*() {
            yield* handlerRequest(GreetInput)
            const cart = state(Cart) // synchronous — no yield to obtain the binding
            yield* cart.set({ cartId: "c1", items: ["apple"] })
            const after1 = yield* cart.get("c1") // read-after-ack sees the write
            yield* cart.set({ cartId: "c1", items: [...Option.getOrThrow(after1).items, "pear"] })
            const final = Option.getOrThrow(yield* cart.get("c1"))
            return final.items.length
          }),
        )
        const rt = yield* DurableExecutionRuntime
        yield* rt.submit(shopper, "shop-1", { name: "q" })
        expect(yield* rt.attach(shopper, "shop-1")).toBe(2)
      }))

    it.effect("sleep durably delays the handler before completing", () =>
      Effect.gen(function*() {
        const napper = handler("napper", { input: GreetInput, output: Schema.Number })(
          Effect.gen(function*() {
            yield* handlerRequest(GreetInput)
            yield* sleep("nap", Duration.millis(120))
            return yield* run("after-nap", Effect.succeed(7), { output: Schema.Number })
          }),
        )
        const rt = yield* DurableExecutionRuntime
        const napStart = yield* Clock.currentTimeMillis
        yield* rt.submit(napper, "nap-1", { name: "rip" })
        const out = yield* rt.attach(napper, "nap-1")
        const elapsed = (yield* Clock.currentTimeMillis) - napStart
        expect(out).toBe(7)
        expect(elapsed).toBeGreaterThanOrEqual(110)
      }))

    it.effect("deferred resolves and reads back within a handler", () =>
      Effect.gen(function*() {
        const wf = handler("deferred-wf", { input: GreetInput, output: Schema.Boolean })(
          Effect.gen(function*() {
            yield* handlerRequest(GreetInput)
            const done = deferred("done", Approval)
            yield* done.resolve({ approved: true })
            return (yield* done.get()).approved
          }),
        )
        const rt = yield* DurableExecutionRuntime
        yield* rt.submit(wf, "def-1", { name: "q" })
        expect(yield* rt.attach(wf, "def-1")).toBe(true)
      }))

    it.effect("signal parks the handler until an external resolution", () =>
      Effect.gen(function*() {
        const approve = handler("approve", { input: GreetInput, output: Schema.Boolean })(
          Effect.gen(function*() {
            yield* handlerRequest(GreetInput)
            return (yield* signal("approval", Approval)).approved
          }),
        )
        const rt = yield* DurableExecutionRuntime
        yield* rt.submit(approve, "sig-1", { name: "q" })
        yield* resolveSignal("sig-1", "approval", Approval, { approved: true })
        expect(yield* rt.attach(approve, "sig-1")).toBe(true)
      }))

    it.effect("resolve-before-await is picked up from the row (no lost wake)", () =>
      Effect.gen(function*() {
        const approve = handler("approve-early", { input: GreetInput, output: Schema.Boolean })(
          Effect.gen(function*() {
            yield* handlerRequest(GreetInput)
            // a run precedes the await; the resolution fires during it
            yield* run("warmup", Effect.succeed(1), { output: Schema.Number })
            return (yield* signal("approval", Approval)).approved
          }),
        )
        const rt = yield* DurableExecutionRuntime
        yield* rt.submit(approve, "sig-early", { name: "q" })
        // fire immediately — before the handler reaches the await
        yield* resolveSignal("sig-early", "approval", Approval, { approved: true })
        expect(yield* rt.attach(approve, "sig-early")).toBe(true)
      }))

    it.effect("service + client invokes by method — no submit/attach/exec-id", () =>
      Effect.gen(function*() {
        const sideEffects = { count: 0 }
        const greeter = service({
          name: "greeter",
          handlers: {
            *greet(req: { name: string }) {
              const n = yield* run("bump", Effect.sync(() => ++sideEffects.count), { output: Schema.Number })
              return { greeting: `hi ${req.name}`, count: n }
            },
          },
        })
        const out = yield* client(greeter).greet({ name: "ada" })
        expect(out).toStrictEqual({ greeting: "hi ada", count: 1 })
        // sendClient returns the execution id without waiting for the result
        const id = yield* sendClient(greeter).greet({ name: "bob" })
        expect(typeof id).toBe("string")
      }))

    it.effect("awakeable resolves by its replay-stable id", () =>
      Effect.gen(function*() {
        const captured = { id: "" }
        const wf = handler("awk-wf", { input: GreetInput, output: Schema.Boolean })(
          Effect.gen(function*() {
            yield* handlerRequest(GreetInput)
            const awk = yield* awakeable(Approval)
            captured.id = awk.id // side-channel for the test (normally sent via a run)
            return (yield* awk.promise).approved
          }),
        )
        const rt = yield* DurableExecutionRuntime
        yield* rt.submit(wf, "awk-1", { name: "q" })
        // the id is a deterministic function of executionId + ordinal
        yield* resolveAwakeable("awk-1", "awk-1/awk/0", Approval, { approved: true })
        expect(yield* rt.attach(wf, "awk-1")).toBe(true)
        expect(captured.id).toBe("awk-1/awk/0")
      }))
  },
)
