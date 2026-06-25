/* oxlint-disable effect/restricted-syntax -- This fixture is an HTTP process boundary that runs fluent Effect clients. */
import { createS2WorkflowRuntimeHost } from "@firegrid/tanstack-workflow-s2"
import { bindFluentDefinitions, object, objectClient, run, state } from "@firegrid/fluent-firegrid"
import { primaryKey, Table } from "@firegrid/fluent-firegrid/state"
import { createS2ObjectRuntimeBinding, s2FluentDefinitionBindingOptions } from "@firegrid/fluent-firegrid-s2"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as http from "node:http"

class CounterState extends Table<CounterState>("counterState")({
  id: Schema.String.pipe(primaryKey),
  value: Schema.Number
}) {}

const counterState = state(CounterState)

const requiredEnv = (name: string): string => {
  const value = process.env[name]
  if (value === undefined || value === "") {
    throw new Error(`${name} is required`)
  }
  return value
}

const s2Endpoint = requiredEnv("S2_ENDPOINT")
const trialId = requiredEnv("FIREGRID_TRIAL_ID")
const hostId = requiredEnv("FIREGRID_HOST_ID")
const port = Number(requiredEnv("HOST_PORT"))

const counter = object({
  name: "cross-host-counter",
  handlers: {
    *add(input: { readonly by: number }) {
      const current = yield* counterState.get("v")
      const value = Option.match(current, {
        onNone: () => 0,
        onSome: (row) => row.value
      })
      const next = value + input.by
      yield* run(() => ({ hostId, next }), { name: `compute-${input.by}` })
      yield* counterState.set({ id: "v", value: next })
      return next
    },
    *value() {
      const current = yield* counterState.get("v")
      return Option.match(current, {
        onNone: () => 0,
        onSome: (row) => row.value
      })
    }
  }
})

const config = {
  namespace: `fluent-object-cross-host-${trialId}`,
  s2Endpoint
}

const host = createS2WorkflowRuntimeHost({
  ...config,
  workflows: bindFluentDefinitions([counter], s2FluentDefinitionBindingOptions(config))
})

const binding = createS2ObjectRuntimeBinding(host, {
  ...config,
  now: () => 1_000,
  objectOwnerLeaseMs: 1_000
})

const client = objectClient(binding, counter)("counter-1")

const sendJson = (response: http.ServerResponse, value: unknown, status = 200) => {
  response.writeHead(status, { "content-type": "application/json" })
  response.end(JSON.stringify(value))
}

const readBy = (request: http.IncomingMessage): number => {
  const url = new URL(request.url ?? "/", `http://${request.headers.host ?? "127.0.0.1"}`)
  return Number(url.searchParams.get("by") ?? "0")
}

const server = http.createServer((request, response) => {
  void (async () => {
    if (request.url === "/ready") {
      sendJson(response, { ok: true })
      return
    }

    if (request.url?.startsWith("/add") === true && request.method === "POST") {
      const value = await Effect.runPromise(client.add({ by: readBy(request) }))
      sendJson(response, { hostId, value })
      return
    }

    if (request.url === "/value") {
      const value = await Effect.runPromise(client.value(undefined))
      sendJson(response, { hostId, value })
      return
    }

    sendJson(response, { error: "not found" }, 404)
  })().catch((cause) => {
    sendJson(response, { error: String(cause) }, 500)
  })
})

server.listen(port, "127.0.0.1")

const close = () => {
  server.close(() => process.exit(0))
}

process.on("SIGTERM", close)
process.on("SIGINT", close)
