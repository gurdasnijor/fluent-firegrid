import type * as acp from "@agentclientprotocol/sdk"
import { Data, type Effect, type Scope } from "effect"
import type { ChildProcessSpawner } from "effect/unstable/process/ChildProcessSpawner"

/**
 * Either a known agent key (`"claude"`, `"codex"`) resolved to its ACP adapter
 * binary, or an explicit command override.
 */
export type AgentSpec =
  | string
  | { readonly command: string; readonly args?: ReadonlyArray<string> }

export interface AcpSpawnInput {
  /** Which ACP harness to launch. */
  readonly agent: AgentSpec
  /** Working directory for the agent process. */
  readonly cwd: string
  /** Extra environment variables for the agent process. */
  readonly env?: Record<string, string>
}

/**
 * A spawned ACP harness process, exposed as an ACP stream plus a teardown.
 *
 * Firegrid's ACP client runtime lane wraps this
 * `stream` in an `acp.ClientSideConnection` and drives the ACP lifecycle. This
 * package records nothing and decides nothing.
 */
export interface AcpProcessHandle {
  /** Duplex of parsed ACP JSON-RPC messages over the process stdio. */
  readonly stream: acp.Stream
  /** Terminate the process and release the transport. */
  readonly kill: Effect.Effect<void>
}

/**
 * Failure to spawn or resolve an ACP harness process. The owner only surfaces
 * process-level failures; protocol/coordination errors belong to Firegrid.
 */
export class AcpProcessError extends Data.TaggedError("AcpProcessError")<{
  readonly op: string
  readonly message: string
  readonly cause?: unknown
}> {}

/**
 * The entire Firegrid-facing surface of an ACP process package: spawn / kill an
 * ACP harness and expose its `acp.Stream`.
 *
 * Per SDD_FLUENT_HARNESS_ADAPTER_CONTRACT the owner must NOT implement
 * `acp.Client`/`acp.Agent` (F-A13), write Durable Streams (F-A1), record Layer 1
 * or commit Layer 2, evaluate waits/timers, or fork children (F-A4), nor own
 * queryable projection schemas (F-A12). The process lifetime is bound to the
 * provided `Scope`; `ChildProcessSpawner` is provided by the host (e.g.
 * `NodeServices.layer`).
 */
export interface AcpHarnessProcessOwnerService {
  readonly spawn: (
    input: AcpSpawnInput,
  ) => Effect.Effect<
    AcpProcessHandle,
    AcpProcessError,
    ChildProcessSpawner | Scope.Scope
  >
}
