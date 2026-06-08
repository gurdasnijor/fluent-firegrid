import {
  FluentEventIngress,
  FluentRuntimeLive,
  FluentStore,
} from "@firegrid/fluent-runtime"
import { Effect, Layer } from "effect"
import type {
  FirelabHost,
  FirelabHostEnv,
} from "../../types.ts"
import {
  deliveryId,
  reviewKey,
  reviewPredicate,
  sessionId,
  turnId,
  waitId,
} from "./scenario.ts"

const seedWorkbench = Effect.gen(function*() {
  const store = yield* FluentStore
  const ingress = yield* FluentEventIngress

  yield* store.createSession({
    sessionId,
    agent: "firelab-fluent-runtime-workbench",
  })
  yield* store.startTurn({
    sessionId,
    turnId,
    prompt: "Register a durable wait and ingest a matching review event.",
  })
  yield* store.registerTurnWait({
    sessionId,
    turnId,
    waitId,
    predicate: reviewPredicate,
    afterOffset: "-1",
    self: { issueId: "ISSUE-1" },
  })
  yield* ingress.ingestExternalEvent({
    sessionId,
    turnId,
    deliveryId,
    type: "review.posted",
    key: reviewKey,
    value: {
      issueId: "ISSUE-1",
      state: "posted",
    },
    source: "firelab-review",
  })
}).pipe(
  Effect.withSpan("firegrid.sim.fluent_runtime_workbench.host.seed"),
)

export const host = (
  env: FirelabHostEnv,
): Layer.Layer<FirelabHost, unknown> =>
  Layer.scopedDiscard(
    seedWorkbench.pipe(
      Effect.provide(FluentRuntimeLive({
        durableStreamsBaseUrl: env.durableStreamsBaseUrl,
        namespace: env.namespace,
      })),
    ),
  )
