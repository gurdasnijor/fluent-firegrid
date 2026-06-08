import * as acp from "@agentclientprotocol/sdk"
import { NodeContext } from "@effect/platform-node"
import { spawnAcpProcess } from "@firegrid/fluent-acp-process"
import {
  FluentRuntimeLive,
  FluentStore,
} from "@firegrid/fluent-runtime"
import {
  connectFiregridAcp,
  FluentAcpClientError,
  type CommitExtMethodInput,
  type FluentAcpRuntimePortService,
  type RecordLayer1ObservationInput,
  type ResolvePermissionInput,
} from "@firegrid/fluent-runtime/acp"
import { Effect, Layer } from "effect"
import type { Context } from "effect"
import type {
  FirelabHost,
  FirelabHostEnv,
} from "../../types.ts"
import {
  agentName,
  sessionId,
  type SessionFactName,
} from "./scenario.ts"

const agentPath = "src/bin/fluent-acp-client-binding-agent-process.ts"

type FluentStoreService = Context.Tag.Service<typeof FluentStore>

const appendFact = (
  store: FluentStoreService,
  name: SessionFactName,
  input: { readonly sessionId: string; readonly payload: unknown },
) =>
  store.appendSessionEvent({
    sessionId: input.sessionId,
    name,
    payload: input.payload,
  }).pipe(
    Effect.asVoid,
    Effect.mapError((cause) =>
      new FluentAcpClientError({
        op: name,
        message: `failed to persist ${name}`,
        cause,
      }),
    ),
  )

const makeRuntimePort = (
  store: FluentStoreService,
): FluentAcpRuntimePortService => ({
  recordLayer1Observation: (input: RecordLayer1ObservationInput) =>
    appendFact(store, input.kind, {
      sessionId: input.sessionId,
      payload: input.payload,
    }),
  resolvePermission: (input: ResolvePermissionInput) => {
    const firstOption = input.request.options[0]
    const response: acp.RequestPermissionResponse = firstOption === undefined
      ? { outcome: { outcome: "cancelled" } }
      : {
        outcome: {
          outcome: "selected",
          optionId: firstOption.optionId,
        },
      }
    return appendFact(store, "acp.permission_result", {
      sessionId: input.sessionId,
      payload: {
        request: input.request,
        response,
      },
    }).pipe(Effect.as(response))
  },
  commitExtMethod: (input: CommitExtMethodInput) => {
    const result = {
      committed: true,
      method: input.method,
      tool: input.params["tool"],
    }
    return appendFact(store, "acp.ext_method.result", {
      sessionId: input.sessionId,
      payload: {
        request: input,
        result,
      },
    }).pipe(Effect.as(result))
  },
})

const seedAcpBinding = Effect.gen(function*() {
  const store = yield* FluentStore
  yield* store.createSession({
    sessionId,
    agent: agentName,
  })

  const handle = yield* spawnAcpProcess({
    agent: {
      command: "pnpm",
      args: ["exec", "tsx", agentPath],
    },
    cwd: process.cwd(),
  })
  yield* Effect.addFinalizer(() => handle.kill)

  const connection = yield* connectFiregridAcp({
    stream: handle.stream,
    runtime: makeRuntimePort(store),
  })

  yield* Effect.promise(() =>
    connection.agent.initialize({
      protocolVersion: acp.PROTOCOL_VERSION,
      clientCapabilities: {},
    }),
  )
  const session = yield* Effect.promise(() =>
    connection.agent.newSession({ cwd: process.cwd(), mcpServers: [] }),
  )
  if (session.sessionId !== sessionId) {
      return yield* Effect.fail(
        new Error(`unexpected ACP session id ${session.sessionId}`),
      )
  }
  yield* Effect.promise(() =>
    connection.agent.prompt({
      sessionId,
      prompt: [
        {
          type: "text",
          text: "run the firelab fluent ACP client binding witness",
        },
      ],
    }),
  )
}).pipe(
  Effect.withSpan("firegrid.sim.fluent_acp_client_binding.host"),
)

export const host = (
  env: FirelabHostEnv,
): Layer.Layer<FirelabHost, unknown> =>
  Layer.scopedDiscard(
    seedAcpBinding.pipe(
      Effect.provide(FluentRuntimeLive({
        durableStreamsBaseUrl: env.durableStreamsBaseUrl,
        namespace: env.namespace,
      })),
      Effect.provide(NodeContext.layer),
    ),
  )
