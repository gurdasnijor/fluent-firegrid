import * as acp from "@agentclientprotocol/sdk"
import { FluentRuntimeLive, type FluentStore } from "@firegrid/fluent-runtime"
import {
  connectFiregridAcpConductor,
  makeConductorSessionPortFromRuntime,
} from "@firegrid/fluent-runtime/acp-conductor"
import { Effect, Layer } from "effect"
import type { FirelabHost, FirelabHostEnv } from "../../types.ts"
import { AGENT_LABEL, SESSION_ID } from "./scenario.ts"

// acp.Stream carries parsed JSON-RPC messages — derive the element type so the
// in-memory editor<->agent duplex matches without a cast.
type AcpStreamMessage = acp.Stream extends { readonly readable: ReadableStream<infer M> } ? M
  : never

const inMemoryStreamPair = (): { editor: acp.Stream; agent: acp.Stream } => {
  const editorToAgent = new TransformStream<AcpStreamMessage, AcpStreamMessage>()
  const agentToEditor = new TransformStream<AcpStreamMessage, AcpStreamMessage>()
  return {
    editor: { writable: editorToAgent.writable, readable: agentToEditor.readable },
    agent: { writable: agentToEditor.writable, readable: editorToAgent.readable },
  }
}

const textBlock = (text: string): acp.ContentBlock => ({ type: "text", text })

/**
 * Drive a REAL ACP SDK editor-side client over an `acp.Stream` into the
 * conductor, backed by the production `makeConductorSessionPortFromRuntime` over
 * `FluentRuntimeLive`. No fakes: each ACP call becomes a real fluent-runtime
 * write against firelab's DurableStreamTestServer. The driver then reads the
 * session stream back to prove the facts landed.
 */
const runBinding = Effect.gen(function*() {
  // Capture the host runtime (carries FluentStore + the firelab tracer) so the
  // conductor's port can discharge `FluentStore` at the ACP Promise boundary.
  const runtime = yield* Effect.runtime<FluentStore>()
  const port = makeConductorSessionPortFromRuntime(runtime, {
    agent: AGENT_LABEL,
    newSessionId: () => SESSION_ID,
  })

  const pair = inMemoryStreamPair()
  // Production conductor (acp.Agent) over the agent side of the duplex.
  yield* connectFiregridAcpConductor({ stream: pair.agent, port })

  // Real ACP SDK editor-side client over the editor side of the duplex.
  const editor = new acp.ClientSideConnection(
    () => ({
      sessionUpdate: () => Promise.resolve(),
      requestPermission: () => Promise.resolve({ outcome: { outcome: "cancelled" } }),
    }),
    pair.editor,
  )

  yield* Effect.promise(() =>
    editor.initialize({
      protocolVersion: 1,
      clientCapabilities: {
        fs: { readTextFile: false, writeTextFile: false },
        terminal: false,
      },
    }),
  )
  const session = yield* Effect.promise(() =>
    editor.newSession({ cwd: "/firelab", mcpServers: [] }),
  )
  yield* Effect.promise(() =>
    editor.prompt({
      sessionId: session.sessionId,
      prompt: [textBlock("drive the conductor binding")],
    }),
  )
  yield* Effect.promise(() => editor.cancel({ sessionId: session.sessionId }))
}).pipe(
  Effect.withSpan("firegrid.sim.fluent_acp_conductor_binding.host.run"),
)

export const host = (
  env: FirelabHostEnv,
): Layer.Layer<FirelabHost, unknown> =>
  Layer.scopedDiscard(
    runBinding.pipe(
      Effect.provide(FluentRuntimeLive({
        durableStreamsBaseUrl: env.durableStreamsBaseUrl,
        namespace: env.namespace,
      })),
    ),
  )
