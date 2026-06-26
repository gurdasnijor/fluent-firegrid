import { s2WorkflowExecutionStore } from "@firegrid/store"
import * as Effect from "effect/Effect"

import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"

const promise = <A>(evaluate: () => Promise<A>): Effect.Effect<A, VerificationError> =>
  Effect.tryPromise({
    try: evaluate,
    catch: (cause) => new VerificationError({ cause, message: "TanStack Workflow S2 store promise failed" })
  })

export default proof("store.leases")
  .describedAs(
    "Proves the TanStack Workflow S2 store lease behavior: one owner claims a run, a live lease rejects contenders, and an expired lease can be claimed as stale."
  )
  .spec(({ property, trialId }) =>
    property("store.leases-proof")
      .s2Lite({ persistence: "local-root" })
      .workload(({ s2Endpoint }) =>
        Effect.gen(function*() {
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({ message: "store lease proof requires s2Lite" })
          }
          const store = s2WorkflowExecutionStore({
            namespace: `leases-${trialId}`,
            s2Endpoint
          })
          const runId = "run-leases"
          yield* promise(() =>
            store.createRun({
              input: {},
              now: 1_000,
              runId,
              workflowId: "lease-workflow"
            })
          )
          const first = yield* promise(() => store.claimRun({ leaseMs: 500, leaseOwner: "owner-a", now: 1_000, runId }))
          const contender = yield* promise(() =>
            store.claimRun({ leaseMs: 500, leaseOwner: "owner-b", now: 1_100, runId })
          )
          yield* promise(() => store.heartbeatRunLease({ leaseMs: 500, leaseOwner: "owner-a", now: 1_200, runId }))
          const stale = yield* promise(() =>
            store.claimStaleRuns({ leaseMs: 500, leaseOwner: "owner-b", limit: 10, now: 2_000 })
          )
          return {
            contenderKind: contender.kind,
            firstKind: first.kind,
            staleOwners: stale.map((claim) => claim.lease.owner),
            staleRunIds: stale.map((claim) => claim.run.runId)
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          contenderKind: "not-claimable",
          firstKind: "claimed",
          staleOwners: ["owner-b"],
          staleRunIds: ["run-leases"]
        }),
        traceSql(
          "leases-used-s2-http",
          `
          SELECT countIf(SpanName = 'http.client GET') >= 1 AS ok
          FROM trial_spans
        `
        )
      ])
  )
