import { expect, layer } from "@effect/vitest"
import { Clock, Duration, Effect, Layer, Option, Schema } from "effect"
import { primaryKey, Table } from "effect-s2-stream-db"
import {
  attach,
  awakeable,
  client,
  deferred,
  DurableExecutionRuntime,
  handler,
  handlerRequest,
  object,
  poll,
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
const TestLive = DurableExecutionRuntime.layer().pipe(Layer.provide(S2LiteLive))

const GreetInput = Schema.Struct({ name: Schema.String })
const GreetOutput = Schema.Struct({ greeting: Schema.String, count: Schema.Number })
const Approval = Schema.Struct({ approved: Schema.Boolean })

class BoomError extends Schema.TaggedErrorClass<BoomError>()("BoomError", { why: Schema.String }) {}

class Cart extends Table<Cart>("cart")({
  cartId: Schema.String.pipe(primaryKey),
  items: Schema.Array(Schema.String),
}) {}

class CounterState extends Table<CounterState>("counterState")({
  id: Schema.String.pipe(primaryKey),
  value: Schema.Number,
}) {}

// a keyed virtual object: durable per-key state + exclusive (serialized) methods.
const counter = object({
  name: "counter",
  handlers: {
    *add(amount: number) {
      const st = state(CounterState)
      const cur = Option.match(yield* st.get("v"), { onNone: () => 0, onSome: (r) => r.value })
      const next = cur + amount
      yield* st.set({ id: "v", value: next })
      return next
    },
    *value() {
      const st = state(CounterState)
      return Option.match(yield* st.get("v"), { onNone: () => 0, onSome: (r) => r.value })
    },
  },
})

layer(TestLive, { excludeTestServices: true, timeout: Duration.seconds(40) })(
  "effect-s2-durable engine over s2 lite",
  (it) => {
    // ── the authoring surface: service + client ─────────────────────────────

    it.effect("client invokes a handler by method and returns the decoded result", () =>
      Effect.gen(function*() {
        const calls = { count: 0 }
        const greeter = service({
          name: "greeter",
          handlers: {
            *greet(req: { name: string }) {
              const n = yield* run(Effect.sync(() => ++calls.count), { output: Schema.Number })
              return { greeting: `hi ${req.name}`, count: n }
            },
          },
        })
        const out = yield* client(greeter).greet({ name: "ada" })
        expect(out).toStrictEqual({ greeting: "hi ada", count: 1 })
        expect(calls.count).toBe(1)
      }))

    it.effect("sendClient fire-and-forget + poll/attach by id", () =>
      Effect.gen(function*() {
        const svc = service({
          name: "answerer",
          handlers: {
            *answer(_req: { q: string }) {
              return yield* run(Effect.succeed(42), { output: Schema.Number })
            },
          },
        })
        const id = yield* sendClient(svc).answer({ q: "life" })
        expect(yield* attach(id, Schema.Number)).toBe(42)
        expect(Option.getOrNull(yield* poll(id, Schema.Number))).toBe(42)
      }))

    it.effect("a typed run failure propagates to the caller", () =>
      Effect.gen(function*() {
        const svc = service({
          name: "exploder",
          handlers: {
            *boom(_req: { n: number }) {
              return yield* run(Effect.fail(new BoomError({ why: "nope" })), {
                output: Schema.Number,
                error: BoomError,
              })
            },
          },
        })
        const exit = yield* Effect.exit(client(svc).boom({ n: 1 }))
        expect(exit._tag).toBe("Failure")
      }))

    it.effect("state(Table) is a mutable durable record across steps", () =>
      Effect.gen(function*() {
        const shop = service({
          name: "shop",
          handlers: {
            *checkout(_req: { user: string }) {
              const cart = state(Cart)
              yield* cart.set({ cartId: "c1", items: ["apple"] })
              const after1 = yield* cart.get("c1") // read-after-ack sees the write
              yield* cart.set({ cartId: "c1", items: [...Option.getOrThrow(after1).items, "pear"] })
              return Option.getOrThrow(yield* cart.get("c1")).items.length
            },
          },
        })
        expect(yield* client(shop).checkout({ user: "q" })).toBe(2)
      }))

    it.effect("sleep durably delays the handler before completing", () =>
      Effect.gen(function*() {
        const svc = service({
          name: "napper",
          handlers: {
            *nap(_req: { ms: number }) {
              yield* sleep("nap", Duration.millis(120))
              return yield* run(Effect.succeed(7), { output: Schema.Number })
            },
          },
        })
        const start = yield* Clock.currentTimeMillis
        const out = yield* client(svc).nap({ ms: 120 })
        expect(out).toBe(7)
        expect((yield* Clock.currentTimeMillis) - start).toBeGreaterThanOrEqual(110)
      }))

    it.effect("deferred resolves and reads back within a handler", () =>
      Effect.gen(function*() {
        const svc = service({
          name: "deferrer",
          handlers: {
            *go(_req: { x: string }) {
              const done = deferred("done", Approval)
              yield* done.resolve({ approved: true })
              return (yield* done.get()).approved
            },
          },
        })
        expect(yield* client(svc).go({ x: "q" })).toBe(true)
      }))

    it.effect("signal parks the handler until an external resolution", () =>
      Effect.gen(function*() {
        const svc = service({
          name: "approver",
          handlers: {
            *approve(_req: { x: string }) {
              return (yield* signal("approval", Approval)).approved
            },
          },
        })
        const id = yield* sendClient(svc).approve({ x: "q" })
        yield* resolveSignal(id, "approval", Approval, { approved: true })
        expect(yield* attach(id, Schema.Boolean)).toBe(true)
      }))

    it.effect("resolve-before-await is picked up from the row (no lost wake)", () =>
      Effect.gen(function*() {
        const svc = service({
          name: "approver-early",
          handlers: {
            *approve(_req: { x: string }) {
              yield* run(Effect.succeed(1), { output: Schema.Number }) // a step precedes the await
              return (yield* signal("approval", Approval)).approved
            },
          },
        })
        const id = yield* sendClient(svc).approve({ x: "q" })
        // fire immediately — likely before the handler reaches the await
        yield* resolveSignal(id, "approval", Approval, { approved: true })
        expect(yield* attach(id, Schema.Boolean)).toBe(true)
      }))

    it.effect("awakeable resolves by its replay-stable id", () =>
      Effect.gen(function*() {
        const captured = { id: "" }
        const svc = service({
          name: "awaker",
          handlers: {
            *go(_req: { x: string }) {
              const awk = yield* awakeable(Approval)
              captured.id = awk.id // normally handed to an ingress client via a run
              return (yield* awk.promise).approved
            },
          },
        })
        const id = yield* sendClient(svc).go({ x: "q" })
        // the awakeable id is a deterministic function of executionId + ordinal
        yield* resolveAwakeable(id, `${id}/awk/0`, Approval, { approved: true })
        expect(yield* attach(id, Schema.Boolean)).toBe(true)
        expect(captured.id).toBe(`${id}/awk/0`)
      }))

    it.effect("re-invoking a completed id is idempotent (no re-run)", () =>
      Effect.gen(function*() {
        const calls = { count: 0 }
        const svc = service({
          name: "dedup",
          handlers: {
            *go(_req: { x: string }) {
              return yield* run(Effect.sync(() => ++calls.count), { output: Schema.Number })
            },
          },
        })
        const c = client(svc)
        const first = yield* c.go({ x: "q" }, { idempotencyKey: "dup-1" })
        const second = yield* c.go({ x: "q" }, { idempotencyKey: "dup-1" }) // after `first` completed
        expect(first).toBe(1)
        expect(second).toBe(1) // served from the roster, not a re-run
        expect(calls.count).toBe(1)
      }))

    it.effect("attach decodes via its schema on the live (owned) path too", () =>
      Effect.gen(function*() {
        // returns a number, but parks first so attach takes the live (running) path
        const svc = service({
          name: "live-decode",
          handlers: {
            *go(_req: { x: string }) {
              yield* signal("go", Schema.Boolean)
              return 7
            },
          },
        })
        const id = yield* sendClient(svc).go({ x: "q" })
        // decoding a number as a string must fail — proving the live path honors the
        // schema rather than returning the raw in-memory value (7).
        const [exit] = yield* Effect.all([
          Effect.exit(attach(id, Schema.String)),
          resolveSignal(id, "go", Schema.Boolean, true),
        ], { concurrency: 2 })
        expect(exit._tag).toBe("Failure")
      }))

    it.effect("a pinned id with a divergent input does not alias steps (dedup wins)", () =>
      Effect.gen(function*() {
        const seen: Array<number> = []
        const svc = service({
          name: "pinned",
          handlers: {
            // the step value is derived from the input; a re-run under a different
            // input would write a semantically different `run/0` fact to the SAME
            // stream — dedup prevents the re-run, so positional keys can't alias.
            *go(req: { n: number }) {
              return yield* run(Effect.sync(() => (seen.push(req.n), req.n * 10)), { output: Schema.Number })
            },
          },
        })
        const c = client(svc)
        const a = yield* c.go({ n: 1 }, { idempotencyKey: "pin-1" })
        const b = yield* c.go({ n: 2 }, { idempotencyKey: "pin-1" }) // different input, same id
        expect(a).toBe(10)
        expect(b).toBe(10) // served from the first execution — NOT a re-run of n=2
        expect(seen).toStrictEqual([1]) // the action ran exactly once, for n=1
      }))

    // ── keyed virtual objects: per-key durable state + exclusive methods ──────

    it.effect("object: keyed state persists across calls and is isolated per key", () =>
      Effect.gen(function*() {
        expect(yield* client(counter, "c1").add(5)).toBe(5)
        expect(yield* client(counter, "c1").add(3)).toBe(8) // a fresh call sees the prior write
        expect(yield* client(counter, "c1").value()).toBe(8) // no-arg method, fresh client
        expect(yield* client(counter, "c2").value()).toBe(0) // a different key is isolated
      }))

    it.effect("object: same-key methods run exclusively (serialized read-modify-write)", () =>
      Effect.gen(function*() {
        const c = client(counter, "race")
        // 12 concurrent increments; exclusive access makes the RMW lost-update-free.
        yield* Effect.all(Array.from({ length: 12 }, () => c.add(1)), { concurrency: "unbounded" })
        expect(yield* client(counter, "race").value()).toBe(12)
      }))

    it.effect("object: fire-and-forget via sendClient + attach by id", () =>
      Effect.gen(function*() {
        const id = yield* sendClient(counter, "sc").add(4)
        expect(yield* attach(id, Schema.Number)).toBe(4)
        expect(yield* client(counter, "sc").value()).toBe(4)
      }))

    // ── the low-level primitive: handler + DurableExecutionRuntime.submit ────
    // service/client are sugar over this; power users can drive it directly.

    it.effect("handler + submit is idempotent for a live execution", () =>
      Effect.gen(function*() {
        const calls = { count: 0 }
        const greet = handler("greet-ll", { input: GreetInput, output: GreetOutput })(
          Effect.gen(function*() {
            const req = yield* handlerRequest(GreetInput)
            const n = yield* run(Effect.sync(() => ++calls.count), { output: Schema.Number })
            return { greeting: `hi ${req.name}`, count: n }
          }),
        )
        const rt = yield* DurableExecutionRuntime
        yield* rt.submit(greet, "ll-1", { name: "ada" })
        yield* rt.submit(greet, "ll-1", { name: "ada" }) // second submit is a no-op
        expect(yield* attach("ll-1", GreetOutput)).toStrictEqual({ greeting: "hi ada", count: 1 })
        expect(calls.count).toBe(1)
      }))
  },
)
