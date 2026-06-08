import { Effect } from "effect"
import type { Runtime } from "effect"
import { FluentStore } from "../Store.ts"
import type { FluentRuntimeError } from "../Store.ts"
import { FiregridAcpError } from "./errors.ts"
import type {
  AcceptPromptInput,
  AcceptPromptResult,
  ConductorSessionPort,
  OpenSessionInput,
  OpenSessionResult,
  RecordCancellationInput,
} from "./port.ts"

export interface ConductorRuntimePortOptions {
  /** Agent label recorded on the fluent-runtime session. */
  readonly agent: string
  /** Override session-id minting (defaults to `crypto.randomUUID`). */
  readonly newSessionId?: () => string
}

/**
 * Production {@link ConductorSessionPort} backed by `FluentStore`. Each editor
 * intent becomes a real fluent-runtime write — this is the only place ACP
 * conductor work crosses into Layer 1/Layer 2 facts. The captured `Runtime`
 * carries `FluentStore`, so the returned port methods are `R = never` and the
 * conductor can run them at the ACP Promise boundary.
 *
 * Skeleton mappings (faithful Layer 1 observations; richer turn/wait semantics
 * land in later slices):
 * - openSession        -> FluentStore.createSession
 * - acceptPrompt       -> FluentStore.appendSessionEvent (accepted user intent)
 * - recordCancellation -> FluentStore.appendSessionEvent (cancellation evidence)
 */
const ACCEPTED_END_TURN: AcceptPromptResult = { stopReason: "end_turn", updates: [] }

export const makeConductorSessionPortFromRuntime = (
  runtime: Runtime.Runtime<FluentStore>,
  options: ConductorRuntimePortOptions,
): ConductorSessionPort => {
  // Session-id minting happens at the ACP boundary, outside any Effect, so the
  // platform `crypto` UUID is appropriate here rather than the `Random` service.
  // @effect-diagnostics-next-line effect/cryptoRandomUUID:off
  const mintSessionId = options.newSessionId ?? (() => crypto.randomUUID())

  const run = <A>(
    op: string,
    eff: Effect.Effect<A, FluentRuntimeError, FluentStore>,
  ): Effect.Effect<A, FiregridAcpError> =>
    eff.pipe(
      Effect.provide(runtime),
      Effect.mapError(
        (cause) => new FiregridAcpError({ op, message: `fluent-runtime ${op} failed`, cause }),
      ),
    )

  return {
    openSession: (_input: OpenSessionInput): Effect.Effect<OpenSessionResult, FiregridAcpError> => {
      const sessionId = mintSessionId()
      return run(
        "openSession",
        Effect.flatMap(FluentStore, (store) =>
          store.createSession({ sessionId, agent: options.agent }),
        ),
      ).pipe(Effect.as({ sessionId }))
    },

    acceptPrompt: (input: AcceptPromptInput): Effect.Effect<AcceptPromptResult, FiregridAcpError> =>
      run(
        "acceptPrompt",
        Effect.flatMap(FluentStore, (store) =>
          store.appendSessionEvent({
            sessionId: input.sessionId,
            name: "acp/prompt.accepted",
            payload: { prompt: input.prompt },
          }),
        ),
      ).pipe(Effect.as(ACCEPTED_END_TURN)),

    recordCancellation: (
      input: RecordCancellationInput,
    ): Effect.Effect<void, FiregridAcpError> =>
      run(
        "recordCancellation",
        Effect.flatMap(FluentStore, (store) =>
          store.appendSessionEvent({
            sessionId: input.sessionId,
            name: "acp/session.cancelled",
            payload: {},
          }),
        ),
      ).pipe(Effect.asVoid),
  }
}
