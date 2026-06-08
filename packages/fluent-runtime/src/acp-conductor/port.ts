import type * as acp from "@agentclientprotocol/sdk"
import type { Effect } from "effect"
import type { FiregridAcpError } from "./errors.ts"

/**
 * The fluent-runtime-facing seam the conductor binds editor intents to. The
 * conductor knows ACP; the port knows fluent-runtime. Methods return `Effect`
 * (R = never) so production backs them with `FluentStore` (see
 * `makeConductorSessionPortFromRuntime`) while tests fake them with
 * `Effect.succeed`. The conductor never imports Store/Host/Sources directly —
 * this port is the entire surface it depends on.
 */
export interface ConductorSessionPort {
  /** Bind a new editor session to fluent-runtime session authority. */
  readonly openSession: (
    input: OpenSessionInput,
  ) => Effect.Effect<OpenSessionResult, FiregridAcpError>
  /**
   * Record accepted user intent for a prompt and drive fluent-runtime session
   * work. Returns the ACP stop reason plus any session updates the runtime
   * produced for the editor — the conductor forwards them verbatim and invents
   * nothing of its own.
   */
  readonly acceptPrompt: (
    input: AcceptPromptInput,
  ) => Effect.Effect<AcceptPromptResult, FiregridAcpError>
  /** Record durable cancellation / continuation evidence for a session. */
  readonly recordCancellation: (
    input: RecordCancellationInput,
  ) => Effect.Effect<void, FiregridAcpError>
}

export interface OpenSessionInput {
  /** Working directory the editor opened the session in. */
  readonly cwd: string
}

export interface OpenSessionResult {
  /** Host-owned session id surfaced back to the editor as the ACP session id. */
  readonly sessionId: acp.SessionId
}

export interface AcceptPromptInput {
  readonly sessionId: acp.SessionId
  readonly prompt: ReadonlyArray<acp.ContentBlock>
}

export interface AcceptPromptResult {
  readonly stopReason: acp.StopReason
  /** Session updates to relay to the editor before responding. */
  readonly updates: ReadonlyArray<acp.SessionUpdate>
}

export interface RecordCancellationInput {
  readonly sessionId: acp.SessionId
}

// Note: how the process is launched by an editor (stdio / ndJsonStream over
// process stdin/stdout) is deliberately NOT modelled here. The conductor binds
// to a ready `acp.Stream`; building that stream from Node stdio belongs to a
// CLI/bin edge, not the runtime package.

/**
 * The editor-facing notification channel — a seam over the outer
 * `AgentSideConnection.sessionUpdate`. Tests fake it to capture relayed
 * updates; production wraps the real connection.
 */
export interface ConductorClientChannel {
  readonly sessionUpdate: (
    notification: acp.SessionNotification,
  ) => Effect.Effect<void, FiregridAcpError>
}

/**
 * Optional downstream delegation seam. Per SDD_FLUENT_HARNESS_ADAPTER_CONTRACT
 * the downstream ACP edge uses the SEPARATE `FiregridAcpClient` role (acp.Client)
 * — this is only the interface the conductor would route through, so the two
 * roles stay distinct public exports and we never form a `Client | Agent` union.
 * Not implemented in this slice.
 */
export interface ConductorDownstream {
  readonly delegatePrompt: (
    input: AcceptPromptInput,
  ) => Effect.Effect<AcceptPromptResult, FiregridAcpError>
}

