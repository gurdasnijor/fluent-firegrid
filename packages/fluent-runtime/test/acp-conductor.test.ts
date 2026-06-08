import * as acp from "@agentclientprotocol/sdk"
import { Effect } from "effect"
import { describe, expect, it } from "vitest"
import {
  connectFiregridAcpConductor,
  FiregridAcpConductor,
  FiregridAcpError,
  type AcceptPromptInput,
  type ConductorClientChannel,
  type ConductorSessionPort,
  type OpenSessionInput,
  type RecordCancellationInput,
} from "../src/acp-conductor/index.ts"

// ---------------------------------------------------------------------------
// Fakes — the fluent-runtime-facing seam is faked in this slice (per task scope)
// ---------------------------------------------------------------------------

interface PortSpy {
  readonly port: ConductorSessionPort
  readonly opened: Array<OpenSessionInput>
  readonly prompts: Array<AcceptPromptInput>
  readonly cancels: Array<RecordCancellationInput>
}

const makePortSpy = (opts?: {
  readonly sessionId?: string
  readonly updates?: ReadonlyArray<acp.SessionUpdate>
  readonly fail?: boolean
}): PortSpy => {
  const opened: Array<OpenSessionInput> = []
  const prompts: Array<AcceptPromptInput> = []
  const cancels: Array<RecordCancellationInput> = []
  const port: ConductorSessionPort = {
    openSession: (input) =>
      opts?.fail === true
        ? Effect.fail(new FiregridAcpError({ op: "openSession", message: "boom" }))
        : Effect.sync(() => {
            opened.push(input)
            return { sessionId: opts?.sessionId ?? "sess-1" }
          }),
    acceptPrompt: (input) =>
      opts?.fail === true
        ? Effect.fail(new FiregridAcpError({ op: "acceptPrompt", message: "boom" }))
        : Effect.sync(() => {
            prompts.push(input)
            return { stopReason: "end_turn", updates: opts?.updates ?? [] }
          }),
    recordCancellation: (input) =>
      Effect.sync(() => {
        cancels.push(input)
      }),
  }
  return { port, opened, prompts, cancels }
}

const channelSpy = (sink: Array<acp.SessionNotification>): ConductorClientChannel => ({
  sessionUpdate: (notification) =>
    Effect.sync(() => {
      sink.push(notification)
    }),
})

const textBlock = (text: string): acp.ContentBlock => ({ type: "text", text })
const chunk = (text: string): acp.SessionUpdate => ({
  sessionUpdate: "agent_message_chunk",
  content: textBlock(text),
})

// ---------------------------------------------------------------------------
// Unit: conductor routes ACP Agent calls through the port
// ---------------------------------------------------------------------------

describe("FiregridAcpConductor (acp.Agent)", () => {
  it("initialize negotiates the editor's protocol version and advertises no auth", async () => {
    const spy = makePortSpy()
    const conductor = new FiregridAcpConductor({
      port: spy.port,
      clientChannel: channelSpy([]),
    })
    const res = await conductor.initialize({
      protocolVersion: 1,
      clientCapabilities: { fs: { readTextFile: false, writeTextFile: false }, terminal: false },
    })
    expect(res.protocolVersion).toBe(1)
    expect(res.authMethods).toEqual([])
    expect(res.agentCapabilities?.loadSession).toBe(false)
  })

  it("newSession routes to the port and returns the host-owned session id", async () => {
    const spy = makePortSpy({ sessionId: "sess-42" })
    const conductor = new FiregridAcpConductor({ port: spy.port, clientChannel: channelSpy([]) })
    const res = await conductor.newSession({ cwd: "/work", mcpServers: [] })
    expect(res.sessionId).toBe("sess-42")
    expect(spy.opened).toEqual([{ cwd: "/work" }])
  })

  it("prompt records accepted intent and relays runtime updates to the editor", async () => {
    const sink: Array<acp.SessionNotification> = []
    const spy = makePortSpy({ updates: [chunk("hello")] })
    const conductor = new FiregridAcpConductor({ port: spy.port, clientChannel: channelSpy(sink) })
    const res = await conductor.prompt({ sessionId: "sess-1", prompt: [textBlock("hi")] })
    expect(res.stopReason).toBe("end_turn")
    expect(spy.prompts).toEqual([{ sessionId: "sess-1", prompt: [textBlock("hi")] }])
    // relayed verbatim, tagged with the session id
    expect(sink).toEqual([{ sessionId: "sess-1", update: chunk("hello") }])
  })

  it("cancel records durable cancellation evidence", async () => {
    const spy = makePortSpy()
    const conductor = new FiregridAcpConductor({ port: spy.port, clientChannel: channelSpy([]) })
    await conductor.cancel({ sessionId: "sess-1" })
    expect(spy.cancels).toEqual([{ sessionId: "sess-1" }])
  })

  it("lowers a port failure to an ACP RequestError (no raw throw)", async () => {
    const spy = makePortSpy({ fail: true })
    const conductor = new FiregridAcpConductor({ port: spy.port, clientChannel: channelSpy([]) })
    await expect(conductor.prompt({ sessionId: "s", prompt: [] })).rejects.toBeInstanceOf(
      acp.RequestError,
    )
  })

  it("keeps the downstream delegation seam optional and role-separate", () => {
    const spy = makePortSpy()
    const without = new FiregridAcpConductor({ port: spy.port, clientChannel: channelSpy([]) })
    expect(without.hasDownstream()).toBe(false)
    const withDown = new FiregridAcpConductor({
      port: spy.port,
      clientChannel: channelSpy([]),
      downstream: { delegatePrompt: (i) => spy.port.acceptPrompt(i) },
    })
    expect(withDown.hasDownstream()).toBe(true)
  })
})

// ---------------------------------------------------------------------------
// Integration: drive the conductor over REAL ACP frames through an in-memory
// duplex. Proves the AgentSideConnection wiring + stdout discipline (frames
// flow only through the injected stream) end to end.
// ---------------------------------------------------------------------------

// acp.Stream carries parsed JSON-RPC messages, not bytes — derive the element
// type so the in-memory duplex matches without a cast.
type AcpStreamMessage = acp.Stream extends { readonly readable: ReadableStream<infer M> } ? M : never

const inMemoryStreamPair = (): { client: acp.Stream; agent: acp.Stream } => {
  const clientToAgent = new TransformStream<AcpStreamMessage, AcpStreamMessage>()
  const agentToClient = new TransformStream<AcpStreamMessage, AcpStreamMessage>()
  return {
    client: { writable: clientToAgent.writable, readable: agentToClient.readable },
    agent: { writable: agentToClient.writable, readable: clientToAgent.readable },
  }
}

describe("connectFiregridAcpConductor (end-to-end ACP frames)", () => {
  it("an ACP client drives initialize/newSession/prompt/cancel through the conductor", async () => {
    const spy = makePortSpy({ sessionId: "sess-e2e", updates: [chunk("ack")] })
    const editorUpdates: Array<acp.SessionNotification> = []
    const pair = inMemoryStreamPair()

    const { agent } = await Effect.runPromise(
      connectFiregridAcpConductor({ port: spy.port, stream: pair.agent }),
    )
    expect(agent).toBeInstanceOf(FiregridAcpConductor)

    // The editor side: a minimal ACP client that captures session updates.
    const client = new acp.ClientSideConnection(
      () => ({
        sessionUpdate: (params) => {
          editorUpdates.push(params)
          return Promise.resolve()
        },
        requestPermission: () =>
          Promise.resolve({ outcome: { outcome: "cancelled" } }),
      }),
      pair.client,
    )

    const init = await client.initialize({
      protocolVersion: 1,
      clientCapabilities: { fs: { readTextFile: false, writeTextFile: false }, terminal: false },
    })
    expect(init.protocolVersion).toBe(1)

    const session = await client.newSession({ cwd: "/repo", mcpServers: [] })
    expect(session.sessionId).toBe("sess-e2e")

    const prompt = await client.prompt({
      sessionId: session.sessionId,
      prompt: [textBlock("do it")],
    })
    expect(prompt.stopReason).toBe("end_turn")
    await client.cancel({ sessionId: session.sessionId })

    // Intents reached fluent-runtime (the faked port) and the update reached the editor.
    expect(spy.prompts.map((p) => p.sessionId)).toEqual(["sess-e2e"])
    expect(spy.cancels).toEqual([{ sessionId: "sess-e2e" }])
    expect(editorUpdates).toEqual([{ sessionId: "sess-e2e", update: chunk("ack") }])
  })
})
