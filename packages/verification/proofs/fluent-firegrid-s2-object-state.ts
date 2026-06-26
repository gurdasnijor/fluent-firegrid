import {
  FluentDurableContext,
  type FluentDurableContextService,
  FluentFiregridError,
  type ObjectStateBackend,
  state
} from "@firegrid/fluent-firegrid"
import { primaryKey, Table } from "@firegrid/fluent-firegrid/state"
import { createS2ObjectStateBackend } from "@firegrid/fluent-firegrid-s2"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"

import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"

class CounterState extends Table<CounterState>("counterState")({
  id: Schema.String.pipe(primaryKey),
  value: Schema.Number
}) {}

const counter = state(CounterState)

const testSleep = Effect.fn("testSleep")(function*() {})
const testSleepUntil = Effect.fn("testSleepUntil")(function*() {})
const unusedWaitForSignal = Effect.fn("unusedWaitForSignal")(function*() {
  return yield* new FluentFiregridError({ message: "waitForSignal is not used in this proof" })
})
const proofStep: FluentDurableContextService["step"] = Effect.fn("proofStep")(function*(name, action) {
  const value = action({ attempt: 1, id: name, signal: new AbortController().signal })
  return yield* (Effect.isEffect(value)
    ? value.pipe(Effect.mapError((cause) => new FluentFiregridError({ cause, message: "proof step failed" })))
    : Effect.promise(() => Promise.resolve(value)))
})

const contextFor = (stateBackend: ObjectStateBackend): FluentDurableContextService => ({
  key: "counter-1",
  state: stateBackend,
  sleep: testSleep,
  sleepUntil: testSleepUntil,
  step: proofStep,
  waitForSignal: unusedWaitForSignal
})

export default proof("fluent-firegrid-s2.object-state")
  .describedAs(
    "Proves the fluent table/materialization state layer over S2: state(Table).set writes to an object state stream and a fresh backend folds S2 to read the typed row."
  )
  .spec(({ property, trialId }) =>
    property("fluent-firegrid-s2.object-state-proof")
      .s2Lite({ persistence: "local-root" })
      .workload(({ s2Endpoint }) =>
        Effect.gen(function*() {
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({ message: "fluent S2 object state proof requires s2Lite" })
          }
          const config = {
            namespace: `fluent-state-${trialId}`,
            s2Endpoint
          }
          const address = { key: "counter-1", objectName: "counter" }
          const writeBackend = createS2ObjectStateBackend(config, address)
          yield* counter.set({ id: "v", value: 5 }).pipe(
            Effect.provideService(FluentDurableContext, FluentDurableContext.of(contextFor(writeBackend)))
          )

          const freshBackend = createS2ObjectStateBackend(config, address)
          const loaded = yield* counter.get("v").pipe(
            Effect.provideService(FluentDurableContext, FluentDurableContext.of(contextFor(freshBackend)))
          )
          yield* counter.delete("v").pipe(
            Effect.provideService(FluentDurableContext, FluentDurableContext.of(contextFor(freshBackend)))
          )
          const removed = yield* counter.get("v").pipe(
            Effect.provideService(FluentDurableContext, FluentDurableContext.of(contextFor(freshBackend)))
          )

          return {
            loaded: Option.getOrUndefined(loaded),
            removed: Option.isNone(removed)
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          loaded: { id: "v", value: 5 },
          removed: true
        }),
        traceSql(
          "fluent-object-state-used-s2-http",
          `
          SELECT countIf(SpanName = 'http.client GET') >= 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "fluent-object-state-supervised-s2-lite",
          `
          SELECT countIf(SpanName = 'S2LiteSupervisor.spawn') = 1 AS ok
          FROM trial_spans
        `
        )
      ])
  )
