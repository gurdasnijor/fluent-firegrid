import { Schema } from "effect"
import {
  iface,
  implement,
  run,
  type Operation,
} from "@firegrid/fluent-firegrid"

const incidentReviewInterface = iface.service("incidentReviewInterface", {
  summarize: iface.schemas({
    input: Schema.String,
    output: Schema.String,
  }),
})

export const incidentReviewImplementation = implement(incidentReviewInterface, {
  handlers: {
    *summarize(incidentId: string): Operation<string> {
      return yield* run(() => `interface-summary:${incidentId}`, { name: "summarize" })
    },
  },
})

export const interfacesTutorial = {
  tier: "10-ifaces",
  status: "implemented: descriptor-only contract plus typed implementation",
  contract: incidentReviewInterface.name,
} as const
