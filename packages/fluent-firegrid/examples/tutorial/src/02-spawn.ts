import { all, race, run, select, service, spawn, type Operation } from "@firegrid/fluent-firegrid"
import { delayedValue } from "./fakes.ts"

const replicateProbe = (
  label: string,
  durationMs: number,
) =>
  run(() => delayedValue(durationMs, label), { name: `${label}-probe` })

export const incidentFanout = service({
  name: "incidentFanout",
  handlers: {
    // fluent-firegrid-keystone.EXAMPLES.1
    *compareReplicas(incidentId: string): Operation<string> {
      const primary = spawn(replicateProbe(`${incidentId}:primary`, 20))
      const secondary = spawn(replicateProbe(`${incidentId}:secondary`, 10))
      const [left, right] = yield* all([primary, secondary])
      return `${left}|${right}`
    },

    // fluent-firegrid-keystone.EXAMPLES.1
    *fastestReplica(incidentId: string): Operation<string> {
      return yield* race([
        spawn(replicateProbe(`${incidentId}:slow`, 20)),
        spawn(replicateProbe(`${incidentId}:fast`, 1)),
      ])
    },

    // fluent-firegrid-keystone.EXAMPLES.1
    *taggedReplica(incidentId: string): Operation<string> {
      const selected = yield* select({
        slow: spawn(replicateProbe(`${incidentId}:slow`, 20)),
        fast: spawn(replicateProbe(`${incidentId}:fast`, 1)),
      })
      return `${String(selected.tag)}:${yield* selected.future}`
    },

    // fluent-firegrid-keystone.EXAMPLES.1
    *mixedSources(incidentId: string): Operation<string> {
      const journaled = run(() => delayedValue(20, `journal:${incidentId}`), {
        name: "journaled",
      })
      const local = spawn(replicateProbe(`local:${incidentId}`, 1))
      const [left, right] = yield* all([journaled, local])
      return `${left}+${right}`
    },
  },
})

export const spawnTutorial = {
  tier: "02-spawn",
  status: "implemented: local Effect fiber affordance; durable child sessions are separate",
} as const
