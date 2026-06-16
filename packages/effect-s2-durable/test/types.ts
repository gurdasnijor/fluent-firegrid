import { Effect, Schema } from "effect"
import { primaryKey, Table } from "effect-s2-stream-db"
import { handler, handlerRequest, run, state } from "../src/index.ts"

// Typecheck-only (not run by vitest): a `run` action may NOT use durable
// primitives. When the action requires `DurableExecutionRuntime`, `run` resolves
// to `RunActionViolation` (not an `Effect`), so `yield*`-ing it is a compile
// error — asserted here with `@ts-expect-error`. If the guard regressed, the
// directive would itself error ("unused @ts-expect-error").

const Input = Schema.Struct({ n: Schema.Number })

class Note extends Table<Note>("note")({
  id: Schema.String.pipe(primaryKey),
  text: Schema.String,
}) {}

// A plain action (no durable runtime in R) is legal.
export const legal = handler("legal", { input: Input, output: Schema.Number })(
  Effect.gen(function*() {
    yield* handlerRequest(Input)
    return yield* run(Effect.succeed(1), { output: Schema.Number })
  }),
)

// A state write inside a run action is rejected at the type level.
export const illegalState = handler("illegal-state", { input: Input, output: Schema.Void })(
  Effect.gen(function*() {
    const notes = state(Note)
    // @ts-expect-error — durable primitive inside a run action
    yield* run("bad", notes.set({ id: "x", text: "y" }), { output: Schema.Void })
  }),
)

// A nested durable primitive inside a run action is likewise rejected.
export const illegalNested = handler("illegal-nested", { input: Input, output: Schema.Void })(
  Effect.gen(function*() {
    // @ts-expect-error — durable primitive inside a run action
    yield* run("bad", run("inner", Effect.succeed(1), { output: Schema.Number }), { output: Schema.Number })
  }),
)
