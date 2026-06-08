import { Context, Data, Effect } from "effect"
import type * as acp from "@agentclientprotocol/sdk"
import type { SessionId } from "../Domain.ts"

export class FluentAcpClientError extends Data.TaggedError(
  "FluentAcpClientError",
)<{
  readonly op: string
  readonly message: string
  readonly cause?: unknown
}> {}

export type FluentAcpLayer1Kind =
  | "acp.session_update"
  | "acp.request_permission"
  | "acp.ext_method"

export interface RecordLayer1ObservationInput {
  readonly sessionId: SessionId
  readonly kind: FluentAcpLayer1Kind
  readonly payload: unknown
}

export interface ResolvePermissionInput {
  readonly sessionId: SessionId
  readonly request: acp.RequestPermissionRequest
}

export interface CommitExtMethodInput {
  readonly sessionId: SessionId
  readonly method: string
  readonly params: Record<string, unknown>
}

export interface FluentAcpRuntimePortService {
  readonly recordLayer1Observation: (
    input: RecordLayer1ObservationInput,
  ) => Effect.Effect<void, FluentAcpClientError>
  readonly resolvePermission: (
    input: ResolvePermissionInput,
  ) => Effect.Effect<acp.RequestPermissionResponse, FluentAcpClientError>
  readonly commitExtMethod: (
    input: CommitExtMethodInput,
  ) => Effect.Effect<Record<string, unknown>, FluentAcpClientError>
}

type FiregridAcpEffectRunner = <A>(
  effect: Effect.Effect<A, FluentAcpClientError>,
) => Promise<A>

export class FluentAcpRuntimePort extends Context.Tag(
  "@firegrid/fluent-runtime/acp/client/FluentAcpRuntimePort",
)<FluentAcpRuntimePort, FluentAcpRuntimePortService>() {}

export interface FiregridAcpClientOptions {
  readonly runtime: FluentAcpRuntimePortService
  readonly runEffect?: FiregridAcpEffectRunner
}

export class FiregridAcpClient implements acp.Client {
  readonly #runtime: FluentAcpRuntimePortService
  readonly #runEffect: FiregridAcpEffectRunner

  constructor(options: FiregridAcpClientOptions) {
    this.#runtime = options.runtime
    this.#runEffect = options.runEffect ?? Effect.runPromise
  }

  sessionUpdate(params: acp.SessionNotification): Promise<void> {
    return this.#runEffect(
      this.#runtime.recordLayer1Observation({
        sessionId: params.sessionId,
        kind: "acp.session_update",
        payload: params,
      }),
    )
  }

  requestPermission(
    params: acp.RequestPermissionRequest,
  ): Promise<acp.RequestPermissionResponse> {
    return this.#runEffect(
      this.#runtime.recordLayer1Observation({
        sessionId: params.sessionId,
        kind: "acp.request_permission",
        payload: params,
      }).pipe(
        Effect.zipRight(
          this.#runtime.resolvePermission({
            sessionId: params.sessionId,
            request: params,
          }),
        ),
      ),
    )
  }

  extMethod(
    method: string,
    params: Record<string, unknown>,
  ): Promise<Record<string, unknown>> {
    const sessionId = sessionIdFromExtParams(params)
    return this.#runEffect(
      this.#runtime.recordLayer1Observation({
        sessionId,
        kind: "acp.ext_method",
        payload: { method, params },
      }).pipe(
        Effect.zipRight(
          this.#runtime.commitExtMethod({
            sessionId,
            method,
            params,
          }),
        ),
      ),
    )
  }
}

const sessionIdFromExtParams = (params: Record<string, unknown>): SessionId => {
  const candidate = params.sessionId
  if (typeof candidate === "string" && candidate.length > 0) {
    return candidate
  }
  return "unknown"
}
