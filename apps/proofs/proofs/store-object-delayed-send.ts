import { bindFluentDefinitions, object, objectClient, run, sendObjectClient, state } from "@firegrid/fluent"
import { primaryKey, Table } from "@firegrid/fluent/state"
import {
  createS2ObjectRuntimeBinding,
  createS2WorkflowRuntimeHost,
  s2FluentDefinitionBindingOptions
} from "@firegrid/fluent/s2"
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
  name: "delayed-counter",
  handlers: {
    *add(input: { readonly by: number }) {
      const current = yield* counterState.get("counter")
      const next = Option.match(current, {
        onNone: () => input.by,
        onSome: (row) => row.value + input.by
      })
      yield* run(() => next, { name: "compute-next" })
      yield* counterState.set({ id: "counter", value: next })
      return next
    },
    *value() {
      const current = yield* counterState.get("counter")
      return Option.match(current, {
        onNone: () => 0,
        onSome: (row) => row.value
      })
    }
  }
})

export default proof("store.object-delayed-send")
  .describedAs(
    "Proves S2-backed object delayed send: a delayed same-key send is durably admitted, does not block earlier-than-due same-key work, and executes when attached after its notBefore time."
  )
  .spec(({ property, trialId }) =>
    property("store.object-delayed-send-proof")
      .s2Lite({ persistence: "local-root" })
      .workload(({ s2Endpoint }) =>
        Effect.gen(function*() {
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({ message: "fluent S2 object delayed-send proof requires s2Lite" })
          }
          let currentTime = 1_000
          const config = {
            namespace: `fluent-object-delayed-send-${trialId}`,
            s2Endpoint
          }
          const host = createS2WorkflowRuntimeHost({
            ...config,
            workflows: bindFluentDefinitions([counter], s2FluentDefinitionBindingOptions(config))
          })
          const binding = createS2ObjectRuntimeBinding(host, {
            ...config,
            now: () => currentTime
          })

          const delayed = yield* sendObjectClient(binding, counter)("counter-1").add({ by: 5 }, {
            delay: { seconds: 5 },
            runId: "object:delayed-counter:counter-1:add-delayed"
          })
          const beforeDue = yield* objectClient(binding, counter)("counter-1").value(undefined)

          currentTime = 7_000
          const attached = yield* delayed.attach()
          const afterDue = yield* objectClient(binding, counter)("counter-1").value(undefined)

          return {
            afterDue,
            attached,
            beforeDue,
            reference: {
              handler: delayed.handler,
              invocationId: delayed.invocationId,
              key: delayed.key,
              kind: delayed.kind,
              name: delayed.name
            }
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          afterDue: 5,
          attached: 5,
          beforeDue: 0,
          reference: {
            handler: "add",
            invocationId: "object:delayed-counter:counter-1:add-delayed",
            key: "counter-1",
            kind: "object",
            name: "delayed-counter"
          }
        }),
        traceSql(
          "fluent-object-delayed-send-used-s2-http",
          `
          SELECT countIf(SpanName = 'http.client GET') >= 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "fluent-object-delayed-send-supervised-s2-lite",
          `
          SELECT countIf(SpanName = 'S2LiteSupervisor.spawn') = 1 AS ok
          FROM trial_spans
        `
        )
      ])
  )
