import * as Cause from "effect/Cause"
import { DurableExecutionError } from "../errors.ts"
import * as Context from "effect/Context"
import type { ObjectCallIdParts } from "../object/address.ts"
import * as Deferred from "effect/Deferred"
import { objectPartsOption } from "./address.ts"
import * as Duration from "effect/Duration"
import { DurableStores } from "./durable-stores.ts"
import * as Effect from "effect/Effect"
import { decode, fail, toError } from "./helpers.ts"
import * as Exit from "effect/Exit"
import * as HashMap from "effect/HashMap"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import type * as Schema from "effect/Schema"
import { EngineState } from "./state.ts"

export interface ResultReaderApi {
  readonly attach: <A, I>(
    executionId: string,
    schema: Schema.Codec<A, I, never, never>
  ) => Effect.Effect<A, DurableExecutionError>
  readonly poll: <A, I>(
    executionId: string,
    schema: Schema.Codec<A, I, never, never>
  ) => Effect.Effect<Option.Option<A>, DurableExecutionError>
}

const UNKNOWN_ATTACH_RETRIES = 40

const make: Effect.Effect<ResultReaderApi, never, EngineState | DurableStores> = Effect.gen(function*() {
  const { running } = yield* EngineState
  const { objectDriver: store, provideClient, roster } = yield* DurableStores

  const attachObject = <A, I>(
    callId: string,
    parts: ObjectCallIdParts,
    schema: Schema.Codec<A, I, never, never>,
    unknownBudget: number
  ): Effect.Effect<A, DurableExecutionError> =>
    provideClient(store.status(callId, parts)).pipe(Effect.flatMap((st): Effect.Effect<A, DurableExecutionError> => {
      switch (st._tag) {
        case "Success":
          return decode(schema, st.value)
        case "Failure":
          return fail("attach", st.error)
        case "Defect":
          return fail("attach", st.defect)
        case "Interrupt":
          return fail("attach", "call was interrupted")
        case "Pending":
          return Effect.sleep(Duration.millis(25)).pipe(
            Effect.andThen(attachObject(callId, parts, schema, UNKNOWN_ATTACH_RETRIES))
          )
        case "Unknown":
          return unknownBudget <= 0
            ? fail("attach", `unknown call: ${callId}`)
            : Effect.sleep(Duration.millis(25)).pipe(
              Effect.andThen(attachObject(callId, parts, schema, unknownBudget - 1))
            )
      }
    }))

  const attach = <A, I>(
    executionId: string,
    schema: Schema.Codec<A, I, never, never>
  ): Effect.Effect<A, DurableExecutionError> =>
    Effect.gen(function*() {
      const parts = yield* objectPartsOption(executionId)
      if (Option.isSome(parts)) {
        return yield* attachObject(executionId, parts.value, schema, UNKNOWN_ATTACH_RETRIES)
      }

      const live = yield* Ref.get(running)
      const entry = HashMap.get(live, executionId)
      if (Option.isSome(entry)) {
        const exit = yield* Deferred.await(entry.value.deferred)
        if (Exit.isFailure(exit)) {
          return yield* new DurableExecutionError({
            operation: "attach",
            message: `execution failed: ${Cause.pretty(exit.cause)}`,
            cause: exit.cause
          })
        }
      }
      const row = yield* roster.get(executionId).pipe(Effect.mapError(toError("attach")))
      if (Option.isNone(row)) return yield* fail("attach", `unknown execution: ${executionId}`)
      if (row.value.status === "completed") return yield* decode(schema, row.value.result)
      if (row.value.status === "failed") return yield* fail("attach", row.value.error ?? "execution failed")
      yield* Effect.sleep(Duration.millis(25))
      return yield* attach(executionId, schema)
    })

  const poll = <A, I>(
    executionId: string,
    schema: Schema.Codec<A, I, never, never>
  ): Effect.Effect<Option.Option<A>, DurableExecutionError> =>
    Effect.gen(function*() {
      const parts = yield* objectPartsOption(executionId)
      if (Option.isSome(parts)) {
        const st = yield* provideClient(store.status(executionId, parts.value))
        return st._tag === "Success" ? Option.some(yield* decode(schema, st.value)) : Option.none<A>()
      }
      const row = yield* roster.get(executionId).pipe(Effect.mapError(toError("poll")))
      return Option.isSome(row) && row.value.status === "completed"
        ? Option.some(yield* decode(schema, row.value.result))
        : Option.none<A>()
    })

  return { attach, poll }
})

export class ResultReader extends Context.Service<ResultReader, ResultReaderApi>()(
  "effect-s2-durable/engine/result-reader/ResultReader"
) {
  static readonly layer: Layer.Layer<ResultReader, never, EngineState | DurableStores> = Layer.effect(
    ResultReader,
    make
  )
}
