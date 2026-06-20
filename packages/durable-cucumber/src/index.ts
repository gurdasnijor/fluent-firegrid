// @firegrid/durable-cucumber — a durable Cucumber runner.
//
// Parse Gherkin, execute scenarios over effect-s2-durable, emit canonical
// @cucumber/messages envelopes (CCK-compliant), and (the firegrid layer) validate
// `@sql:` chDB trace-proofs over the production spans a run emits.

// The runner core + authoring surface.
export {
  assemble,
  type AssembledRun,
  type CompiledStep,
  DataTable,
  defineSteps,
  directExec,
  type DurableDefinition,
  durableCucumberLayer,
  type Executor,
  type HookOptions,
  normalizeEnvelopes,
  type ParameterTypeOptions,
  reorderEnvelopes,
  RunnerError,
  runFeatures,
  runFeaturesDurable,
  type RunFeaturesOptions,
  type RunOptions,
  type StepBody,
  stepHost,
  type StepHost,
  stripIgnorable,
  type SupportApi,
  type SupportBundle,
  type SupportDescriptor,
  type World,
} from "./durable/index.ts"

// The firegrid trace-proof harness.
export { firstFailure, type FiregridResult, runFiregrid, statusesOf } from "./firegrid/run.ts"
export { SpecTracing, type WorldServices, WorldServicesLive } from "./firegrid/runtime.ts"
export { type ProofBlock, type ProofResult, scenarioKey, type SpecWorld, SqlProofError } from "./firegrid/proofs.ts"
