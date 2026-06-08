import * as acp from "@agentclientprotocol/sdk"
import { Effect } from "effect"
import { describe, expect, it } from "vitest"
import {
  connectFiregridAcp,
  FiregridAcpClient,
  type FluentAcpLayer1Kind,
  type FluentAcpRuntimePortService,
} from "../src/acp/index.ts"

interface RecordedCall {
  readonly layer: "L1" | "L2"
  readonly kind: FluentAcpLayer1Kind | "permission" | "ext"
  readonly sessionId: string
  readonly payload: unknown
}

interface AcpPair {
  readonly client: acp.Stream
  readonly agent: acp.Stream
}

const makeAcpPair = (): AcpPair => {
  const clientToAgent = new TransformStream<acp.AnyMessage, acp.AnyMessage>()
  const agentToClient = new TransformStream<acp.AnyMessage, acp.AnyMessage>()
  return {
    client: {
      writable: clientToAgent.writable,
      readable: agentToClient.readable,
    },
    agent: {
      writable: agentToClient.writable,
      readable: clientToAgent.readable,
    },
  }
}

const makeAgent = (): acp.Agent => ({
  initialize: (params) => Promise.resolve({
    protocolVersion: params.protocolVersion,
    agentCapabilities: {},
  }),
  newSession: () => Promise.resolve({ sessionId: "agent-session" }),
  authenticate: () => Promise.resolve({}),
  prompt: () => Promise.resolve({ stopReason: "end_turn" }),
  cancel: () => Promise.resolve(),
})

const makeRuntime = (calls: Array<RecordedCall>): FluentAcpRuntimePortService => ({
  recordLayer1Observation: (input) =>
    Effect.sync(() => {
      calls.push({
        layer: "L1",
        kind: input.kind,
        sessionId: input.sessionId,
        payload: input.payload,
      })
    }),
  resolvePermission: (input) =>
    Effect.sync(() => {
      calls.push({
        layer: "L2",
        kind: "permission",
        sessionId: input.sessionId,
        payload: input.request,
      })
      const firstOption = input.request.options[0]
      if (firstOption === undefined) {
        return { outcome: { outcome: "cancelled" } }
      }
      return {
        outcome: {
          outcome: "selected",
          optionId: firstOption.optionId,
        },
      }
    }),
  commitExtMethod: (input) =>
    Effect.sync(() => {
      calls.push({
        layer: "L2",
        kind: "ext",
        sessionId: input.sessionId,
        payload: { method: input.method, params: input.params },
      })
      return { committed: true, method: input.method }
    }),
})

const setup = () => {
  const calls: Array<RecordedCall> = []
  const streams = makeAcpPair()
  let agentClient: acp.AgentSideConnection | undefined
  const agentSide = new acp.AgentSideConnection((connection) => {
    agentClient = connection
    return makeAgent()
  }, streams.agent)
  return Effect.runPromise(
    connectFiregridAcp({
      stream: streams.client,
      runtime: makeRuntime(calls),
    }),
  ).then((connection) => {
    if (agentClient === undefined) {
      throw new Error("AgentSideConnection did not expose an ACP client")
    }
    return { agentSide, agentClient, calls, connection }
  })
}

describe("fluent-firegrid-acp-client FiregridAcpClient", () => {
  it("owns the downstream ACP ClientSideConnection client role", () =>
    setup().then(({ connection }) => {
      expect(connection.client).toBeInstanceOf(FiregridAcpClient)
      return connection.agent.initialize({
        protocolVersion: acp.PROTOCOL_VERSION,
        clientCapabilities: {},
      }).then((initialized) => {
        expect(initialized.protocolVersion).toBe(acp.PROTOCOL_VERSION)
      })
    }))

  it("fluent-firegrid-acp-client records sessionUpdate as Layer 1 observation", () =>
    setup().then(({ agentClient, calls }) =>
      agentClient.sessionUpdate({
        sessionId: "session-1",
        update: {
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: "hello" },
        },
      }).then(() => {
        expect(calls).toEqual([
          {
            layer: "L1",
            kind: "acp.session_update",
            sessionId: "session-1",
            payload: {
              sessionId: "session-1",
              update: {
                sessionUpdate: "agent_message_chunk",
                content: { type: "text", text: "hello" },
              },
            },
          },
        ])
      }),
    ))

  it("fluent-firegrid-acp-client records permission L1 before returning L2 ACP response", () =>
    setup().then(({ agentClient, calls }) =>
      agentClient.requestPermission({
        sessionId: "session-1",
        toolCall: {
          toolCallId: "tool-1",
          title: "Run command",
        },
        options: [
          {
            optionId: "allow-once",
            name: "Allow once",
            kind: "allow_once",
          },
        ],
      }).then((response) => {
        expect(response).toEqual({
          outcome: {
            outcome: "selected",
            optionId: "allow-once",
          },
        })
        expect(calls.map((call) => [call.layer, call.kind])).toEqual([
          ["L1", "acp.request_permission"],
          ["L2", "permission"],
        ])
      }),
    ))

  it("fluent-firegrid-acp-client routes extension tool callbacks through L1 then committed L2 result", () =>
    setup().then(({ agentClient, calls }) =>
      agentClient.extMethod("firegrid/tool/execute", {
        sessionId: "session-1",
        tool: "execute",
        input: { command: "pnpm test" },
      }).then((response) => {
        expect(response).toEqual({
          committed: true,
          method: "firegrid/tool/execute",
        })
        expect(calls.map((call) => [call.layer, call.kind])).toEqual([
          ["L1", "acp.ext_method"],
          ["L2", "ext"],
        ])
        expect(calls[1]?.payload).toEqual({
          method: "firegrid/tool/execute",
          params: {
            sessionId: "session-1",
            tool: "execute",
            input: { command: "pnpm test" },
          },
        })
      }),
    ))
})
