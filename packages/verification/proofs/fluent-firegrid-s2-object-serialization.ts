import { createS2WorkflowRuntimeHost } from "@firegrid/tanstack-workflow-s2"
import { bindFluentDefinitions, object, objectClient, run, state } from "@firegrid/fluent-firegrid"
import { primaryKey, Table } from "@firegrid/fluent-firegrid/state"
import { createS2ObjectRuntimeBinding, s2FluentDefinitionBindingOptions } from "@firegrid/fluent-firegrid-s2"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"

import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"

class CounterState extends Table<CounterState>("counterState")({
  id: Schema.String.pipe(primaryKey),
  value: Schema.Number
}) {}

const counterState = state(CounterState)

const counter = object({
  name: "counter",
  handlers: {
    *add(input: { readonly by: number }) {
      const current = yield* counterState.get("v")
      const value = Option.match(current, {
        onNone: () => 0,
        onSome: (row) => row.value
      })
      const next = value + input.by
      yield* run(() => ({ next }), { name: `compute-${input.by}` })
      yield* counterState.set({ id: "v", value: next })
      return next
    },
    *value() {
      const current = yield* counterState.get("v")
      return Option.match(current, {
        onNone: () => 0,
        onSome: (row) => row.value
      })
    }
  }
})

export default proof("fluent-firegrid-s2.object-serialization")
  .describedAs(
    "Proves S2-backed objectClient serialization: concurrent same-key object calls are admitted to an owner stream, drained in order, and update table state without losing writes."
  )
  .spec(({ property, trialId }) =>
    property("fluent-firegrid-s2.object-serialization-proof")
      .s2Lite({ persistence: "local-root" })
      .workload(({ s2Endpoint }) =>
        Effect.gen(function*() {
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({ message: "fluent S2 object serialization proof requires s2Lite" })
          }
          const config = {
            namespace: `fluent-object-serialization-${trialId}`,
            s2Endpoint
          }
          const host = createS2WorkflowRuntimeHost({
            ...config,
            workflows: bindFluentDefinitions([counter], s2FluentDefinitionBindingOptions(config))
          })
          const binding = createS2ObjectRuntimeBinding(host, {
            ...config,
            now: () => 1_000
          })
          const client = objectClient(binding, counter)("counter-1")
          const results = yield* Effect.all([
            client.add({ by: 5 }),
            client.add({ by: 7 })
          ], { concurrency: "unbounded" })
          const value = yield* client.value(undefined)
          return {
            completedCalls: results.length,
            maxResult: Math.max(...results),
            value
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          completedCalls: 2,
          maxResult: 12,
          value: 12
        }),
        traceSql(
          "fluent-object-serialization-used-s2-http",
          `
          SELECT countIf(SpanName = 'http.client GET') >= 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "fluent-object-serialization-supervised-s2-lite",
          `
          SELECT countIf(SpanName = 'S2LiteSupervisor.spawn') = 1 AS ok
          FROM trial_spans
        `
        )
      ])
  )
