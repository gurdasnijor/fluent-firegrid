import { bindFluentDefinitions, celFor, object, objectClient, sendObjectClient, state } from "@firegrid/fluent"
import { primaryKey, Table } from "@firegrid/fluent/state"
import {
  createS2ObjectRuntimeBinding,
  createS2WorkflowRuntimeHost,
  s2FluentDefinitionBindingOptions
} from "@firegrid/store"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"

import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"

class InvoiceState extends Table<InvoiceState>("invoiceState")({
  id: Schema.String.pipe(primaryKey),
  accountId: Schema.String,
  status: Schema.String
}) {}

const invoices = state(InvoiceState)

const invoiceObject = object({
  name: "indexed-invoice",
  handlers: {
    *put(input: { readonly accountId: string; readonly id: string; readonly status: string }) {
      yield* invoices.set(input)
      return input.id
    },
    *waitReady(input: { readonly accountId: string }) {
      const row = yield* invoices.waitFor({
        index: ["accountId", "status"],
        name: "first-ready-in-account",
        vars: { accountId: input.accountId, status: "ready" },
        where: celFor(InvoiceState).expr((t) => t.row.accountId.eq(input.accountId).and(t.row.status.eq("ready")))
      })
      return row.id
    }
  }
})

export default proof("store.object-index-wait")
  .describedAs(
    "Proves S2-backed object indexed state waits: an object call parks on a table index predicate, an unrelated indexed row does not resume it, and a matching row update resumes the parked run."
  )
  .spec(({ property, trialId }) =>
    property("store.object-index-wait-proof")
      .s2Lite({ persistence: "local-root" })
      .workload(({ s2Endpoint }) =>
        Effect.gen(function*() {
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({ message: "fluent S2 object index-wait proof requires s2Lite" })
          }
          const config = {
            namespace: `fluent-object-index-wait-${trialId}`,
            s2Endpoint
          }
          const host = createS2WorkflowRuntimeHost({
            ...config,
            workflows: bindFluentDefinitions([invoiceObject], s2FluentDefinitionBindingOptions(config))
          })
          const binding = createS2ObjectRuntimeBinding(host, {
            ...config,
            now: () => 1_000
          })
          const waiting = yield* sendObjectClient(binding, invoiceObject)("invoice-owner").waitReady(
            { accountId: "acct-1" },
            { runId: "object:indexed-invoice:invoice-owner:wait-ready" }
          )
          const unrelated = yield* objectClient(binding, invoiceObject)("invoice-owner").put({
            accountId: "acct-2",
            id: "invoice-other",
            status: "ready"
          })
          const matching = yield* objectClient(binding, invoiceObject)("invoice-owner").put({
            accountId: "acct-1",
            id: "invoice-ready",
            status: "ready"
          })
          const waited = yield* waiting.attach()

          return {
            matching,
            reference: {
              handler: waiting.handler,
              invocationId: waiting.invocationId,
              key: waiting.key,
              kind: waiting.kind,
              name: waiting.name
            },
            unrelated,
            waited
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          matching: "invoice-ready",
          reference: {
            handler: "waitReady",
            invocationId: "object:indexed-invoice:invoice-owner:wait-ready",
            key: "invoice-owner",
            kind: "object",
            name: "indexed-invoice"
          },
          unrelated: "invoice-other",
          waited: "invoice-ready"
        }),
        traceSql(
          "fluent-object-index-wait-used-s2-http",
          `
          SELECT countIf(SpanName = 'http.client GET') >= 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "fluent-object-index-wait-supervised-s2-lite",
          `
          SELECT countIf(SpanName = 'S2LiteSupervisor.spawn') = 1 AS ok
          FROM trial_spans
        `
        )
      ])
  )
