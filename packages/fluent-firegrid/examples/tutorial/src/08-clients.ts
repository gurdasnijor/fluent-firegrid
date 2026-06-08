import { Schema } from "effect"
import {
  client,
  run,
  schemas,
  sendClient,
  service,
  type FluentIngress,
  type Operation,
} from "@firegrid/fluent-firegrid"

export const incidentClientTarget = service({
  name: "incidentClientTarget",
  handlers: {
    *summarize(incidentId: string): Operation<string> {
      return yield* run(() => `summary:${incidentId}`, { name: "summarize" })
    },
  },
  descriptors: {
    summarize: schemas({
      input: Schema.String,
      output: Schema.String,
    }),
  },
})

const incidentClients = (ingress: FluentIngress) => ({
  call: client(ingress, incidentClientTarget),
  send: sendClient(ingress, incidentClientTarget),
})

export const clientsTutorial = {
  tier: "08-clients",
  status: "implemented: typed call/send clients derive from definition descriptors",
  bind: incidentClients,
} as const
