import { Effect } from "effect"
import { execute, run, service } from "@firegrid/fluent-firegrid"
import { delayedValue } from "./fakes.ts"

export const incidentTimeout = service({
  name: "incidentTimeout",
  handlers: {
    // fluent-firegrid-keystone.EXAMPLES.1
    boundedLookup: (ctx, request: {
      readonly incidentId: string
      readonly workMs: number
      readonly budgetMs: number
    }) =>
      execute(
        ctx,
        Effect.race(
          run("lookup", delayedValue(request.workMs, `lookup:${request.incidentId}`)),
          Effect.as(Effect.sleep(request.budgetMs), `timeout:${request.incidentId}`),
        ),
      ),
  },
})

export const timeoutTutorial = {
  tier: "03-timeout",
  status: "implemented: local Effect.race timeout; durable sleep is separate",
} as const
