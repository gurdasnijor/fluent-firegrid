import { Effect } from "effect"
import { execute, run, workflow } from "@firegrid/fluent-firegrid"
import {
  draftPatchPlan,
  notifyCoordinator,
  openRemediation,
  type IncidentInput,
} from "./fakes.ts"

export const remediationWorkflow = workflow({
  name: "remediationWorkflow",
  handlers: {
    // fluent-firegrid-keystone.EXAMPLES.3
    run: (ctx, input: IncidentInput) =>
      execute(
        ctx,
        Effect.gen(function* () {
          const plan = yield* run("draft-remediation-plan", Effect.sync(() => draftPatchPlan(input, {
            route: "coordinator",
            severity: "high",
          })))
          const remediationId = yield* run("open-remediation", Effect.sync(() => openRemediation(input, plan)))
          return yield* run("notify-coordinator", Effect.sync(() => notifyCoordinator(remediationId)))
        }),
      ),
    status: (ctx, id: string) =>
      execute(
        ctx,
        run("workflow-status", Effect.succeed(`workflow:${id}:status:modeled`)),
      ),
  },
})

export const workflowTutorial = {
  tier: "09-workflows",
  status: "implemented: workflow({ name, handlers }) over one journal endpoint",
} as const
