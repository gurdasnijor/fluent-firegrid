import { all, race as raceFirst, run, select as selectFirst, service, type Operation } from "@firegrid/fluent-firegrid"
import {
  classifyIncident,
  collectIncidentContext,
  delayedValue,
  draftPatchPlan,
  publishTrace,
  type IncidentInput,
} from "./fakes.ts"

export const incidentReview = service({
  name: "incidentReview",
  handlers: {
    // fluent-firegrid-keystone.EXAMPLES.1
    *hello(name: string): Operation<string> {
      const greeting = yield* run(() => `Hello, ${name}!`, { name: "compose" })
      return greeting
    },

    // fluent-firegrid-keystone.EXAMPLES.1
    *sequential(input: IncidentInput): Operation<string> {
      const triage = yield* run(() => classifyIncident(input), { name: "a" })
      const context = yield* run(() => collectIncidentContext(input), { name: "b" })
      return `${triage.route}-${context}`
    },

    // fluent-firegrid-keystone.EXAMPLES.1
    *parallel(input: IncidentInput): Operation<string> {
      const triage = run(() => classifyIncident(input), { name: "a" })
      const context = run(() => collectIncidentContext(input), { name: "b" })
      const [triageValue, contextValue] = yield* all([triage, context])
      return `${triageValue.route}+${contextValue}`
    },

    // fluent-firegrid-keystone.EXAMPLES.1
    *race(incidentId: string): Operation<string> {
      return yield* raceFirst([
        run(() => delayedValue(1, `primary:${incidentId}`), { name: "primary" }),
        run(() => delayedValue(20, `secondary:${incidentId}`), { name: "secondary" }),
      ])
    },

    // fluent-firegrid-keystone.EXAMPLES.1
    *selectTagged(incidentId: string): Operation<string> {
      const selected = yield* selectFirst({
        fast: run(() => delayedValue(1, `fast:${incidentId}`), { name: "fast" }),
        slow: run(() => delayedValue(20, `slow:${incidentId}`), { name: "slow" }),
      })
      switch (selected.tag) {
        case "fast":
          return `fast-won: ${yield* selected.future}`
        case "slow":
          return `slow-won: ${yield* selected.future}`
      }
    },

    // fluent-firegrid-keystone.EXAMPLES.1
    *summarize(input: IncidentInput): Operation<string> {
      const triage = run(() => classifyIncident(input), { name: "classify" })
      const context = run(() => collectIncidentContext(input), { name: "collect-context" })
      const [triageValue, contextValue] = yield* all([triage, context])
      const plan = yield* run(() => draftPatchPlan({
        ...input,
        title: `${input.title} ${contextValue}`,
      }, triageValue), { name: "draft-plan" })
      return yield* run(() => publishTrace(plan), { name: "publish-trace" })
    },
  },
})
