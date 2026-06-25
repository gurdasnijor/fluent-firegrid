/* oxlint-disable effect/restricted-syntax -- This fixture is an HTTP process boundary that runs fluent Effect clients. */
import { createS2WorkflowRuntimeHost } from "@firegrid/tanstack-workflow-s2"
import {
  bindFluentDefinitions,
  createTanStackRuntimeBinding,
  run,
  sendClient,
  service,
  sleepUntil,
  workflowIdForHandler
} from "@firegrid/fluent-firegrid"
import * as Effect from "effect/Effect"
import * as http from "node:http"

const runId = "fluent-orders:run-1"

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

const orders = service({
  name: "orders",
  handlers: {
    *submit(input: { readonly orderId: string }) {
      const reserved = yield* run(() => ({ hostId, orderId: input.orderId }), { name: "reserve" })
      yield* sleepUntil(5_000)
      const charged = yield* run(() => ({ hostId, reservedBy: reserved.hostId }), { name: "charge" })
      return {
        chargedBy: charged.hostId,
        orderId: input.orderId,
        reservedBy: reserved.hostId
      }
    }
  }
})

const host = createS2WorkflowRuntimeHost({
  namespace: `fluent-host-${trialId}`,
  s2Endpoint,
  workflows: bindFluentDefinitions([orders])
})
const binding = createTanStackRuntimeBinding(host, { now: () => 1_000 })
const ordersSend = sendClient(binding, orders)
const workflowId = workflowIdForHandler(orders, "submit")

const sendJson = (response: http.ServerResponse, value: unknown, status = 200) => {
  response.writeHead(status, { "content-type": "application/json" })
  response.end(JSON.stringify(value))
}

const readNow = (request: http.IncomingMessage, fallback: number): number => {
  const url = new URL(request.url ?? "/", `http://${request.headers.host ?? "127.0.0.1"}`)
  const now = url.searchParams.get("now")
  return now === null ? fallback : Number(now)
}

const server = http.createServer((request, response) => {
  void (async () => {
    if (request.url === "/ready") {
      sendJson(response, { ok: true })
      return
    }

    if (request.url === "/send" && request.method === "POST") {
      const reference = await Effect.runPromise(ordersSend.submit({ orderId: "order-1" }, { runId }))
      sendJson(response, reference)
      return
    }

    if (request.url?.startsWith("/tick") === true && request.method === "POST") {
      const result = await host.tick({
        includeEvents: true,
        leaseMs: 1_000,
        leaseOwner: `host:${hostId}`,
        maxScheduledRuns: 0,
        maxTimers: 10,
        now: readNow(request, Date.now()),
        recoverStaleRuns: true,
        staleRunLimit: 10
      })
      sendJson(response, result)
      return
    }

    if (request.url === "/execution") {
      const execution = await host.store.loadExecution(runId)
      sendJson(response, {
        execution,
        workflowId
      })
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
