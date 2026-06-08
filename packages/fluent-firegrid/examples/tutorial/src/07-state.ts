import { Effect } from "effect"
import { execute, object, run } from "@firegrid/fluent-firegrid"

export const incidentCounter = object({
  name: "incidentCounter",
  handlers: {
    // fluent-firegrid-keystone.EXAMPLES.1
    current: (ctx, _: void) =>
      execute(
        ctx,
        run("current", Effect.succeed(0)),
      ),

    // fluent-firegrid-keystone.EXAMPLES.1
    recordEscalation: (ctx, signal: string) =>
      execute(
        ctx,
        run("record-escalation", Effect.succeed({
          escalations: 1,
          lastSignal: signal,
        })),
      ),

    // fluent-firegrid-keystone.EXAMPLES.1
    reset: (ctx, _: void) =>
      execute(
        ctx,
        run("reset", Effect.void),
      ),
  },
})

export const stateTutorial = {
  tier: "07-state",
  status: "deferred: state/sharedState removed from the Part 1 engine core",
} as const
