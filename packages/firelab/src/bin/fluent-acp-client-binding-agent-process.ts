#!/usr/bin/env tsx

import * as acp from "@agentclientprotocol/sdk"
import { Readable, Writable } from "node:stream"

class BindingAgent implements acp.Agent {
  readonly #connection: acp.AgentSideConnection

  constructor(connection: acp.AgentSideConnection) {
    this.#connection = connection
  }

  initialize(
    params: acp.InitializeRequest,
  ): Promise<acp.InitializeResponse> {
    return Promise.resolve({
      protocolVersion: params.protocolVersion,
      agentCapabilities: {
        loadSession: false,
      },
    })
  }

  newSession(): Promise<acp.NewSessionResponse> {
    return Promise.resolve({ sessionId: "fluent-acp-client-binding-session" })
  }

  authenticate(): Promise<acp.AuthenticateResponse> {
    return Promise.resolve({})
  }

  prompt(params: acp.PromptRequest): Promise<acp.PromptResponse> {
    return this.#connection.sessionUpdate({
      sessionId: params.sessionId,
      update: {
        sessionUpdate: "agent_message_chunk",
        content: {
          type: "text",
          text: "firelab binding witness",
        },
      },
    }).then(() =>
      this.#connection.requestPermission({
        sessionId: params.sessionId,
        toolCall: {
          toolCallId: "permission-tool",
          title: "Approve firelab binding witness",
          kind: "edit",
          status: "pending",
          rawInput: { witness: "fluent-acp-client-binding" },
        },
        options: [
          {
            optionId: "allow-once",
            name: "Allow once",
            kind: "allow_once",
          },
        ],
      }),
    ).then(() =>
      this.#connection.extMethod("firegrid/tool/execute", {
        sessionId: params.sessionId,
        tool: "execute",
        input: { command: "firelab-binding-witness" },
      }),
    ).then(() => ({ stopReason: "end_turn" }))
  }

  cancel(): Promise<void> {
    return Promise.resolve()
  }
}

const input = Writable.toWeb(process.stdout)
const output = Readable.toWeb(process.stdin) as ReadableStream<Uint8Array>
const stream = acp.ndJsonStream(input, output)

new acp.AgentSideConnection((connection) => new BindingAgent(connection), stream)
