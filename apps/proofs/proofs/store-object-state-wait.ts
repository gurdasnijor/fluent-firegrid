import { bindFluentDefinitions, cel, object, objectClient, sendObjectClient, state } from "@firegrid/fluent"
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

class GateState extends Table<GateState>("gateState")({
  id: Schema.String.pipe(primaryKey),
  status: Schema.String
}) {}

const gateState = state(GateState)

const gate = object({
  name: "gate",
  handlers: {
    *open() {
      yield* gateState.set({ id: "gate", status: "open" })
      return "opened"
    },
    *status() {
      const current = yield* gateState.get("gate")
      return Option.match(current, {
        onNone: () => "missing",
        onSome: (row) => row.status
      })
    },
    *wait() {
      const row = yield* gateState.waitFor("gate", {
        name: "opened",
        when: cel("row.status == 'open'")
      })
      return row.status
    }
  }
})

export default proof("store.object-state-wait")
  .describedAs(
    "Proves S2-backed object state waits: a same-key call parks on state(Table).waitFor, a later call mutates the durable table, and the object queue resumes the parked run without blocking the mutation."
  )
  .spec(({ property, trialId }) =>
    property("store.object-state-wait-proof")
      .s2Lite({ persistence: "local-root" })
      .workload(({ s2Endpoint }) =>
        Effect.gen(function*() {
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({ message: "fluent S2 object state-wait proof requires s2Lite" })
          }
          const config = {
            namespace: `fluent-object-state-wait-${trialId}`,
            s2Endpoint
          }
          const host = createS2WorkflowRuntimeHost({
            ...config,
            workflows: bindFluentDefinitions([gate], s2FluentDefinitionBindingOptions(config))
          })
          const binding = createS2ObjectRuntimeBinding(host, {
            ...config,
            now: () => 1_000
          })
          const waiting = yield* sendObjectClient(binding, gate)("gate-1").wait(undefined, {
            runId: "object:gate:gate-1:wait"
          })
          const opened = yield* objectClient(binding, gate)("gate-1").open(undefined)
          const waited = yield* waiting.attach()
          const status = yield* objectClient(binding, gate)("gate-1").status(undefined)
          return {
            opened,
            reference: {
              handler: waiting.handler,
              invocationId: waiting.invocationId,
              key: waiting.key,
              kind: waiting.kind,
              name: waiting.name
            },
            status,
            waited
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          opened: "opened",
          reference: {
            handler: "wait",
            invocationId: "object:gate:gate-1:wait",
            key: "gate-1",
            kind: "object",
            name: "gate"
          },
          status: "open",
          waited: "open"
        }),
        traceSql(
          "fluent-object-state-wait-used-s2-http",
          `
          SELECT countIf(SpanName = 'http.client GET') >= 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "fluent-object-state-wait-supervised-s2-lite",
          `
          SELECT countIf(SpanName = 'S2LiteSupervisor.spawn') = 1 AS ok
          FROM trial_spans
        `
        )
      ])
  )
