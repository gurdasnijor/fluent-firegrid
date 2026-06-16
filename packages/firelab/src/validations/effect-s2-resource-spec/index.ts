import { Effect, Option } from "effect"
import { applySpec, planSpec, type S2Plan, S2Client, S2NotFound, type S2Spec } from "effect-s2"
import { assertEquals, assertTrue } from "../../assertions.ts"
import { S2LiteLive } from "../../s2lite.ts"
import { defineValidation } from "../../types.ts"

const DEFAULT_BASIN = "firelab-s2"

/** The planned change for an explicitly-named stream, or "noop" if not in the plan. */
const streamChange = (plan: S2Plan, name: string): string =>
  plan.changes.find((entry) => entry.resource === "stream" && entry.name === name)?.change ?? "noop"

export default defineValidation({
  id: "effect-s2-resource-spec",
  description:
    "Drives the effect-s2 S2Spec reconciler against s2 lite: plan/apply diff, "
    + "idempotent + partial reconcile over real S2Client basin/stream ops.",
  feature: {
    product: "effect-s2",
    name: "resource-spec",
  },
  backend: S2LiteLive,
  component: ({ keyFor }) =>
    Effect.succeed({
      // a fresh, requirement-scoped singleton stream name in the default basin.
      stream: (suffix: string) => `spec/${keyFor(suffix)}`,
      // non-creating config probe → Option (None when the stream is absent).
      configOption: (stream: string) =>
        S2Client.getStreamConfig({ stream }, { basinName: DEFAULT_BASIN }).pipe(
          Effect.map(Option.some),
          Effect.catch((cause) => (cause instanceof S2NotFound ? Effect.succeedNone : Effect.fail(cause))),
        ),
    }),
  requirements: [
    {
      id: "SPEC_RECONCILE.1",
      description: "S2Spec declares basins and explicitly-named streams with BasinConfig/StreamConfig",
      evidence: 'spans.exists(s, named(s, "S2.spec.apply")) && spans.exists(s, named(s, "S2.ensureBasin")) && spans.exists(s, named(s, "S2.ensureStream"))',
      claim: () =>
        Effect.gen(function*() {
          const basin = "firelab-spec-declare"
          const spec: S2Spec = {
            basins: [{
              name: basin,
              config: { createStreamOnAppend: false },
              streams: [
                { name: "control/inbox", config: { storageClass: "standard" } },
                { name: "control/results" },
              ],
            }],
          }
          yield* applySpec(spec)
          const basinCfg = yield* S2Client.getBasinConfig({ basin })
          assertEquals(basinCfg.createStreamOnAppend, false)
          // both explicitly-named streams were provisioned (getConfig does not throw).
          yield* S2Client.getStreamConfig({ stream: "control/inbox" }, { basinName: basin })
          yield* S2Client.getStreamConfig({ stream: "control/results" }, { basinName: basin })
        }),
    },
    {
      id: "SPEC_RECONCILE.2",
      description: "plan returns the +/~/= diff without mutating; apply performs ensure + reconfigure",
      evidence: 'spans.exists(s, named(s, "S2.spec.plan")) && spans.exists(s, named(s, "S2.spec.apply")) && spans.exists(s, named(s, "S2.ensureStream")) && spans.exists(s, named(s, "S2.getStreamConfig"))',
      claim: ({ stream, configOption }) =>
        Effect.gen(function*() {
          const name = stream("r2")
          const spec: S2Spec = {
            basins: [{ name: DEFAULT_BASIN, streams: [{ name, config: { storageClass: "standard" } }] }],
          }
          const before = yield* planSpec(spec)
          assertEquals(streamChange(before, name), "create") // +
          // plan is a dry run — the stream must not have been created.
          assertTrue(Option.isNone(yield* configOption(name)), "plan did not mutate")
          yield* applySpec(spec)
          const after = yield* planSpec(spec)
          assertEquals(streamChange(after, name), "noop") // =
        }),
    },
    {
      id: "SPEC_RECONCILE.3",
      description: "apply is idempotent — re-applying a converged spec is a no-op (all =)",
      evidence: 'spans.exists(s, named(s, "S2.spec.apply")) && spans.exists(s, named(s, "S2.getStreamConfig"))',
      claim: ({ stream }) =>
        Effect.gen(function*() {
          const name = stream("r3")
          const spec: S2Spec = {
            basins: [{ name: DEFAULT_BASIN, streams: [{ name, config: { storageClass: "standard" } }] }],
          }
          yield* applySpec(spec)
          const second = yield* applySpec(spec)
          assertTrue(
            second.changes.every((entry) => entry.change === "noop"),
            "re-applying a converged spec yields an all-no-change plan",
          )
        }),
    },
    {
      id: "SPEC_RECONCILE.4",
      description: "apply performs a partial reconcile — only fields present in the spec are updated",
      evidence: 'spans.exists(s, named(s, "S2.spec.apply")) && spans.exists(s, named(s, "S2.reconfigureStream")) && spans.exists(s, named(s, "S2.getStreamConfig"))',
      claim: ({ stream }) =>
        Effect.gen(function*() {
          const name = stream("r4")
          // create with basin defaults (no config fields specified).
          yield* applySpec({ basins: [{ name: DEFAULT_BASIN, streams: [{ name }] }] })
          const original = yield* S2Client.getStreamConfig({ stream: name }, { basinName: DEFAULT_BASIN })
          // now specify ONLY retentionPolicy.
          yield* applySpec({
            basins: [{ name: DEFAULT_BASIN, streams: [{ name, config: { retentionPolicy: { ageSecs: 3600 } } }] }],
          })
          const updated = yield* S2Client.getStreamConfig({ stream: name }, { basinName: DEFAULT_BASIN })
          assertEquals(updated.retentionPolicy, { ageSecs: 3600 }) // the specified field changed
          assertEquals(updated.storageClass, original.storageClass) // unspecified fields preserved
          assertEquals(updated.timestamping, original.timestamping)
        }),
    },
    {
      id: "SPEC_RECONCILE.5",
      description: "S2Spec is a fold over existing S2Client operations — no new transport or protocol",
      evidence: 'spans.exists(s, named(s, "S2.spec.apply")) && spans.exists(s, named(s, "S2.ensureStream")) && spans.exists(s, named(s, "S2.getStreamConfig"))',
      claim: ({ stream }) =>
        Effect.gen(function*() {
          const name = stream("r5")
          // provisioning happens purely through S2Client ops (the evidence spans
          // are all S2.* — there is no separate transport).
          yield* applySpec({
            basins: [{ name: DEFAULT_BASIN, streams: [{ name, config: { storageClass: "standard" } }] }],
          })
          const cfg = yield* S2Client.getStreamConfig({ stream: name }, { basinName: DEFAULT_BASIN })
          assertEquals(cfg.storageClass, "standard")
        }),
    },
    {
      id: "SCOPE.1",
      description: "S2Spec covers coarse, named resources only — per-key streams are not declared here",
      evidence: 'spans.exists(s, named(s, "S2.spec.apply")) && spans.exists(s, named(s, "S2.getStreamConfig"))',
      claim: ({ stream, configOption }) =>
        Effect.gen(function*() {
          const declared = stream("named")
          const dynamic = stream("dynamic")
          yield* applySpec({ basins: [{ name: DEFAULT_BASIN, streams: [{ name: declared }] }] })
          // the explicitly-named stream is provisioned...
          yield* S2Client.getStreamConfig({ stream: declared }, { basinName: DEFAULT_BASIN })
          // ...a non-declared (dynamic / per-key) sibling is not.
          assertTrue(
            Option.isNone(yield* configOption(dynamic)),
            "S2Spec provisions only explicitly-named streams, not per-key streams",
          )
        }),
    },
    {
      id: "SCOPE.2",
      description: "a control-plane basin sets createStreamOnAppend false so the stream set is intentional",
      evidence: 'spans.exists(s, named(s, "S2.spec.apply")) && spans.exists(s, named(s, "S2.ensureBasin")) && spans.exists(s, named(s, "S2.getBasinConfig"))',
      claim: () =>
        Effect.gen(function*() {
          const basin = "firelab-spec-control"
          yield* applySpec({ basins: [{ name: basin, config: { createStreamOnAppend: false } }] })
          const cfg = yield* S2Client.getBasinConfig({ basin })
          assertEquals(cfg.createStreamOnAppend, false)
        }),
    },
  ],
})
