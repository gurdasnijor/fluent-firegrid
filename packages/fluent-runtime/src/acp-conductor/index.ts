/**
 * Editor-facing Firegrid ACP conductor (acp.Agent role).
 *
 * Narrow subpath per SDD_FLUENT_HARNESS_ADAPTER_CONTRACT: Zed / editor ACP
 * stdio launches Firegrid as an external agent; the conductor binds ACP session
 * calls to fluent-runtime session authority. The downstream ACP harness edge is
 * the SEPARATE `FiregridAcpClient` (acp.Client) role — this module exports the
 * conductor only and never a public `acp.Client | acp.Agent` union.
 */
export { FiregridAcpConductor, type FiregridAcpConductorOptions } from "./conductor.ts"
export { FiregridAcpError } from "./errors.ts"
export {
  connectFiregridAcpConductor,
  type ConnectFiregridAcpConductorInput,
  type FiregridAcpConductorConnection,
} from "./connect.ts"
export {
  makeConductorSessionPortFromRuntime,
  type ConductorRuntimePortOptions,
} from "./runtime-port.ts"
export type {
  AcceptPromptInput,
  AcceptPromptResult,
  ConductorClientChannel,
  ConductorDownstream,
  ConductorSessionPort,
  OpenSessionInput,
  OpenSessionResult,
  RecordCancellationInput,
} from "./port.ts"
