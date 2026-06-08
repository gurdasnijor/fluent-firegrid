import * as acp from "@agentclientprotocol/sdk"
import { Cause, Effect, Exit, Option } from "effect"
import type { FiregridAcpError } from "./errors.ts"
import type {
  ConductorClientChannel,
  ConductorDownstream,
  ConductorSessionPort,
} from "./port.ts"

export interface FiregridAcpConductorOptions {
  /** Fluent-runtime-facing seam for accepted session intent. */
  readonly port: ConductorSessionPort
  /** Editor-facing notification channel (wraps the outer ACP connection). */
  readonly clientChannel: ConductorClientChannel
  /** Optional downstream ACP delegation seam (acp.Client role; unused in this slice). */
  readonly downstream?: ConductorDownstream
}

const PROMPT_CAPABILITIES: acp.PromptCapabilities = {
  image: false,
  audio: false,
  embeddedContext: false,
}

/**
 * Editor-facing ACP conductor. Implements the ACP `Agent` interface so Zed or
 * another ACP editor can launch Firegrid as an external agent over stdio, and
 * binds each ACP session call to fluent-runtime session authority through the
 * injected {@link ConductorSessionPort}.
 *
 * Per SDD_FLUENT_HARNESS_ADAPTER_CONTRACT this is the conductor (acp.Agent)
 * role ONLY. Downstream delegation to a real ACP harness is the separate
 * `FiregridAcpClient` (acp.Client) role and is exposed as a seam, never a
 * `Client | Agent` union. The conductor writes nothing to stdout itself — the
 * outer `AgentSideConnection` owns the ACP frame stream.
 */
export class FiregridAcpConductor implements acp.Agent {
  private readonly port: ConductorSessionPort
  private readonly clientChannel: ConductorClientChannel
  // Held to keep the downstream seam wired without forming a public union.
  private readonly downstream: ConductorDownstream | undefined

  constructor(options: FiregridAcpConductorOptions) {
    this.port = options.port
    this.clientChannel = options.clientChannel
    this.downstream = options.downstream
  }

  /** Whether a downstream ACP client is available for delegation. */
  hasDownstream(): boolean {
    return this.downstream !== undefined
  }

  initialize(params: acp.InitializeRequest): Promise<acp.InitializeResponse> {
    // Negotiate to the version the editor requested; advertise a minimal,
    // honest capability set for the skeleton (no auth, no load/fork).
    return Promise.resolve({
      protocolVersion: params.protocolVersion,
      agentCapabilities: {
        loadSession: false,
        promptCapabilities: PROMPT_CAPABILITIES,
      },
      authMethods: [],
    })
  }

  newSession(params: acp.NewSessionRequest): Promise<acp.NewSessionResponse> {
    return this.run(
      "session/new",
      this.port
        .openSession({ cwd: params.cwd })
        .pipe(Effect.map((result) => ({ sessionId: result.sessionId }))),
    )
  }

  prompt(params: acp.PromptRequest): Promise<acp.PromptResponse> {
    return this.run(
      "session/prompt",
      Effect.gen(this, function* () {
        const result = yield* this.port.acceptPrompt({
          sessionId: params.sessionId,
          prompt: params.prompt,
        })
        // Relay runtime-produced updates to the editor verbatim — the conductor
        // forwards, it does not synthesize agent output.
        yield* Effect.forEach(
          result.updates,
          (update) =>
            this.clientChannel.sessionUpdate({
              sessionId: params.sessionId,
              update,
            }),
          { discard: true },
        )
        return { stopReason: result.stopReason }
      }),
    )
  }

  cancel(params: acp.CancelNotification): Promise<void> {
    return this.run(
      "session/cancel",
      this.port.recordCancellation({ sessionId: params.sessionId }),
    )
  }

  authenticate(
    _params: acp.AuthenticateRequest,
  ): Promise<acp.AuthenticateResponse | void> {
    // No auth methods are advertised in `initialize`; accept as a no-op.
    return Promise.resolve()
  }

  /**
   * Cross the Effect → ACP Promise boundary. A `FiregridAcpError` is lowered to
   * an ACP `RequestError` so the editor receives a well-formed JSON-RPC error
   * frame; diagnostics never touch stdout.
   */
  private run<A>(op: string, eff: Effect.Effect<A, FiregridAcpError>): Promise<A> {
    return Effect.runPromiseExit(eff).then((exit) =>
      Exit.match(exit, {
        onSuccess: (value) => value,
        onFailure: (cause) => {
          throw toRequestError(op, cause)
        },
      }),
    )
  }
}

const toRequestError = (op: string, cause: Cause.Cause<FiregridAcpError>): acp.RequestError => {
  const failure = Cause.failureOption(cause)
  if (Option.isSome(failure)) {
    return acp.RequestError.internalError({
      op: failure.value.op,
      message: failure.value.message,
    })
  }
  return acp.RequestError.internalError({ op, message: Cause.pretty(cause) })
}
