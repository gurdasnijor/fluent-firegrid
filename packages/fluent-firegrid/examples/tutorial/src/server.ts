import { incidentReview } from "./01-basics.ts"
import { incidentFanout, spawnTutorial } from "./02-spawn.ts"
import { incidentTimeout, timeoutTutorial } from "./03-timeout.ts"
import { retryTutorial } from "./04-retry.ts"
import { sagaTutorial } from "./05-saga.ts"
import { cancelTutorial } from "./06-cancel.ts"
import { incidentCounter, stateTutorial } from "./07-state.ts"
import { incidentClientTarget, clientsTutorial } from "./08-clients.ts"
import { remediationWorkflow, workflowTutorial } from "./09-workflows.ts"
import { incidentReviewImplementation, interfacesTutorial } from "./10-ifaces.ts"
import { serdesTutorial } from "./11-serdes.ts"
export { deferredSurface } from "./deferred-surface.ts"

export const services = [
  incidentReview,
  incidentFanout,
  incidentTimeout,
  incidentCounter,
  incidentClientTarget,
  remediationWorkflow,
  incidentReviewImplementation,
] as const

// fluent-firegrid-keystone.EXAMPLES.2
export const tutorialTiers = [
  { tier: "01-basics", status: "implemented" },
  spawnTutorial,
  timeoutTutorial,
  retryTutorial,
  sagaTutorial,
  cancelTutorial,
  stateTutorial,
  clientsTutorial,
  workflowTutorial,
  interfacesTutorial,
  serdesTutorial,
] as const
