import { createWorkflow } from "@tanstack/workflow-core"
import { createS2WorkflowRuntimeHost } from "@firegrid/tanstack-workflow-s2"
import * as http from "node:http"

const runId = "crash-sleep:run-1"
const workflowId = "crash-sleep-workflow"

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

const workflow = createWorkflow({
  id: workflowId
}).handler(async (ctx) => {
  await ctx.step("before-sleep", async () => ({ hostId }))
  await ctx.sleepUntil(5_000)
  await ctx.step("after-sleep", async () => ({ hostId }))
  return {
    completed: true,
    runId: ctx.runId
  }
})

const host = createS2WorkflowRuntimeHost({
  namespace: `crash-host-${trialId}`,
  s2Endpoint,
  workflows: {
    [workflowId]: {
      load: async () => workflow
    }
  }
})

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

    if (request.url === "/start" && request.method === "POST") {
      const result = await host.runtime.startRun({
        includeEvents: true,
        input: {},
        leaseMs: 1_000,
        leaseOwner: `host:${hostId}`,
        now: 1_000,
        runId,
        workflowId
      })
      sendJson(response, result)
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
      sendJson(response, execution)
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
