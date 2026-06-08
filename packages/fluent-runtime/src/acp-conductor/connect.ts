import * as acp from "@agentclientprotocol/sdk"
import { Effect } from "effect"
import { FiregridAcpConductor } from "./conductor.ts"
import { FiregridAcpError } from "./errors.ts"
import type {
  ConductorClientChannel,
  ConductorDownstream,
  ConductorSessionPort,
} from "./port.ts"

export interface FiregridAcpConductorConnection {
  readonly agent: FiregridAcpConductor
  /** Tear down the ACP connection. */
  readonly close: Effect.Effect<void>
}

export interface ConnectFiregridAcpConductorInput {
  /** A ready ACP message duplex (editor side). How it is built — stdio,
   * in-memory, a socket — is the caller's concern; this package stays
   * transport-agnostic and free of process/Node dependencies. */
  readonly stream: acp.Stream
  /** Fluent-runtime-facing seam. Production composes
   * `makeConductorSessionPortFromRuntime`; tests inject a fake. */
  readonly port: ConductorSessionPort
  /** Optional downstream ACP delegation seam (acp.Client role). */
  readonly downstream?: ConductorDownstream
}

/**
 * Bind a {@link FiregridAcpConductor} to an editor over a ready ACP `Stream`.
 * The outer `AgentSideConnection` owns the frame stream, so the conductor
 * presents as a normal ACP agent and the editor-facing `sessionUpdate` channel
 * is derived from the live connection and handed to the conductor.
 *
 * This is intentionally pure: it takes an `acp.Stream` and never touches
 * `process` / `node:stream`. A CLI/bin edge is responsible for building the
 * stdio stream (ndJsonStream over process stdin/stdout, stdout ACP-only) and
 * calling this.
 */
export const connectFiregridAcpConductor = (
  input: ConnectFiregridAcpConductorInput,
): Effect.Effect<FiregridAcpConductorConnection> =>
  Effect.sync(() => {
    let agent: FiregridAcpConductor | undefined
    const connection = new acp.AgentSideConnection((conn) => {
      const clientChannel: ConductorClientChannel = {
        sessionUpdate: (notification) =>
          Effect.tryPromise({
            try: () => conn.sessionUpdate(notification),
            catch: (cause) =>
              new FiregridAcpError({
                op: "session/update",
                message: "editor sessionUpdate failed",
                cause,
              }),
          }),
      }
      agent = new FiregridAcpConductor({
        port: input.port,
        clientChannel,
        ...(input.downstream !== undefined ? { downstream: input.downstream } : {}),
      })
      return agent
    }, input.stream)
    // `agent` is assigned synchronously by the AgentSideConnection factory above.
    const resolved = agent
    if (resolved === undefined) {
      throw new FiregridAcpError({
        op: "connect",
        message: "AgentSideConnection did not initialize the conductor",
      })
    }
    return {
      agent: resolved,
      close: Effect.sync(() => {
        void connection
      }),
    }
  })
