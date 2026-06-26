/* oxlint-disable effect/restricted-syntax -- Node HTTP serving and host-loop callbacks are runtime boundaries that must run fluent Effects. */
import {
  type AnyGeneratorHandler,
  bindFluentDefinitions,
  createTanStackExternalSignalBinding,
  type Definition,
  type DefinitionKind
} from "@firegrid/fluent-firegrid"
import { createFluentHttpHandler } from "@firegrid/fluent-firegrid-http"
import {
  createS2ObjectRuntimeBinding,
  s2FluentDefinitionBindingOptions,
  type S2ObjectRuntimeBindingConfig
} from "@firegrid/fluent-firegrid-s2"
import {
  createS2WorkflowRuntimeHost,
  type S2WorkflowRuntimeConfig,
  type S2WorkflowRuntimeHost,
  type S2WorkflowRuntimeHostLoopArgs
} from "@firegrid/tanstack-workflow-s2"
import type { WorkflowRegistrationMap } from "@tanstack/workflow-runtime"
import * as Effect from "effect/Effect"
// @effect-diagnostics-next-line nodeBuiltinImport:off
import * as Http from "node:http"
import type * as Net from "node:net"

type AnyDefinition = Definition<string, DefinitionKind, Record<string, AnyGeneratorHandler>>
type MaybePromise<A> = A | Promise<A>

export interface FluentNodeHttpServerOptions {
  readonly handler: (request: Request) => Promise<Response>
  readonly healthPath?: string
  readonly hostname?: string
  readonly port?: number
  readonly readyPath?: string
  readonly ready?: () => boolean | Promise<boolean>
}

export interface FluentNodeHttpServer {
  readonly address: Net.AddressInfo
  readonly close: () => Promise<void>
  readonly server: Http.Server
  readonly url: string
}

export interface FluentS2NodeRuntimeOptions
  extends Omit<S2WorkflowRuntimeConfig<WorkflowRegistrationMap>, "workflows">, S2ObjectRuntimeBindingConfig
{
  readonly definitions: ReadonlyArray<AnyDefinition>
  readonly webhooks?: FluentWebhookRoutes
}

export interface FluentS2NodeRuntime {
  readonly binding: ReturnType<typeof createS2ObjectRuntimeBinding>
  readonly handler: (request: Request) => Promise<Response>
  readonly host: S2WorkflowRuntimeHost<WorkflowRegistrationMap>
}

export interface FluentS2NodeServerOptions extends FluentS2NodeRuntimeOptions {
  readonly healthPath?: string
  readonly hostLoop?: false | Omit<S2WorkflowRuntimeHostLoopArgs, "signal">
  readonly hostname?: string
  readonly port?: number
  readonly readyPath?: string
}

export interface FluentS2NodeServer extends FluentS2NodeRuntime, FluentNodeHttpServer {
  readonly close: () => Promise<void>
}

export type FluentWebhookRoutes = Readonly<Record<string, FluentWebhookRoute>>

export interface FluentWebhookRoute {
  readonly definition: AnyDefinition
  readonly handler: string
  readonly idempotencyKey?: (request: Request) => MaybePromise<string | undefined>
  readonly key?: string | ((request: Request) => MaybePromise<string | undefined>)
  readonly verify?: (request: Request, body: Uint8Array) => MaybePromise<boolean | Response | void>
}

export const createFluentS2NodeRuntime = (options: FluentS2NodeRuntimeOptions): FluentS2NodeRuntime => {
  let binding: ReturnType<typeof createS2ObjectRuntimeBinding> | undefined
  let externalSignals: ReturnType<typeof createTanStackExternalSignalBinding> | undefined
  const workflows = bindFluentDefinitions(
    options.definitions,
    {
      ...s2FluentDefinitionBindingOptions(options, { invocationBinding: () => binding }),
      externalSignals: () => externalSignals
    }
  )
  const host = createS2WorkflowRuntimeHost({
    ...options,
    workflows
  })
  externalSignals = createTanStackExternalSignalBinding(host)
  binding = createS2ObjectRuntimeBinding(host, options)
  const handler = createFluentWebhookHandler(
    createFluentHttpHandler({ binding, definitions: options.definitions, externalSignals }),
    options.webhooks
  )
  return { binding, handler, host }
}

export const createFluentWebhookHandler = (
  fallback: (request: Request) => Promise<Response>,
  routes: FluentWebhookRoutes | undefined
): (request: Request) => Promise<Response> => {
  if (routes === undefined || Object.keys(routes).length === 0) return fallback
  return async (request) => {
    const route = routes[new URL(request.url).pathname]
    if (route === undefined) return await fallback(request)
    if (request.method !== "POST") return writeWebJson({ error: "method_not_allowed" }, 405)

    const body = new Uint8Array(await request.arrayBuffer())
    const verification = await route.verify?.(request, body)
    if (verification instanceof Response) return verification
    if (verification === false) return writeWebJson({ error: "webhook_verification_failed" }, 401)

    const key = typeof route.key === "function" ? await route.key(request) : route.key
    if (route.definition._kind === "object" && (key === undefined || key === "")) {
      return writeWebJson({ error: "object_key_required" }, 400)
    }
    const runId = await route.idempotencyKey?.(request)
    const targetUrl = webhookInvocationUrl(request, route, key, runId)
    return await fallback(
      new Request(targetUrl, {
        body,
        headers: request.headers,
        method: "POST"
      })
    )
  }
}

export const listenFluentHttp = (options: FluentNodeHttpServerOptions): Promise<FluentNodeHttpServer> =>
  new Promise((resolve, reject) => {
    const healthPath = options.healthPath ?? "/health"
    const readyPath = options.readyPath ?? "/ready"
    const server = Http.createServer((request, response) => {
      void handleNodeRequest(options, request, response, healthPath, readyPath)
    })
    const onError = (cause: Error) => {
      reject(cause)
    }
    server.once("error", onError)
    server.listen(options.port ?? 0, options.hostname ?? "127.0.0.1", () => {
      server.off("error", onError)
      const address = server.address()
      if (address === null || typeof address === "string") {
        reject(new Error("fluent Node server did not bind to a TCP address"))
        return
      }
      resolve({
        address,
        close: () => closeServer(server),
        server,
        url: `http://${address.address}:${address.port}`
      })
    })
  })

export const serveFluentS2 = async (options: FluentS2NodeServerOptions): Promise<FluentS2NodeServer> => {
  const runtime = createFluentS2NodeRuntime(options)
  const loopController = new AbortController()
  let loopFailed = false
  const { onTick, ...hostLoopOptions } = options.hostLoop === false ? {} : options.hostLoop ?? {}
  const hostLoop = options.hostLoop === false
    ? undefined
    : runtime.host.runLoop({
      ...hostLoopOptions,
      onTick: async (result) => {
        await Effect.runPromise(runtime.binding.drainDelayedStarts())
        await onTick?.(result)
      },
      signal: loopController.signal
    }).catch(() => {
      loopFailed = true
    })
  const listener = await listenFluentHttp({
    handler: runtime.handler,
    ...(options.healthPath === undefined ? {} : { healthPath: options.healthPath }),
    ...(options.hostname === undefined ? {} : { hostname: options.hostname }),
    ...(options.port === undefined ? {} : { port: options.port }),
    ready: () => !loopFailed,
    ...(options.readyPath === undefined ? {} : { readyPath: options.readyPath })
  })

  const close = async () => {
    loopController.abort()
    await listener.close()
    await hostLoop
  }

  return {
    ...runtime,
    address: listener.address,
    close,
    server: listener.server,
    url: listener.url
  }
}

const handleNodeRequest = async (
  options: FluentNodeHttpServerOptions,
  request: Http.IncomingMessage,
  response: Http.ServerResponse,
  healthPath: string,
  readyPath: string
) => {
  try {
    const url = requestUrl(request)
    if (url.pathname === healthPath) {
      writeJson(response, { ok: true }, 200)
      return
    }
    if (url.pathname === readyPath) {
      const ready = await options.ready?.() ?? true
      writeJson(response, { ok: ready }, ready ? 200 : 503)
      return
    }
    await writeWebResponse(response, await options.handler(await toWebRequest(request, url)))
  } catch (cause) {
    writeJson(response, { error: cause instanceof Error ? cause.message : String(cause) }, 500)
  }
}

const requestUrl = (request: Http.IncomingMessage): URL =>
  new URL(request.url ?? "/", `http://${request.headers.host ?? "127.0.0.1"}`)

const toWebRequest = async (
  request: Http.IncomingMessage,
  url: URL
): Promise<Request> =>
  new Request(
    url,
    requestInit({
      ...(hasRequestBody(request.method) ? { body: new Uint8Array(await readBody(request)) } : {}),
      headers: headersFrom(request),
      ...(request.method === undefined ? {} : { method: request.method })
    })
  )

const requestInit = (init: RequestInit): RequestInit => init

const hasRequestBody = (method: string | undefined): boolean =>
  method !== undefined && method !== "GET" && method !== "HEAD"

const headersFrom = (request: Http.IncomingMessage): Headers => {
  const headers = new Headers()
  for (let index = 0; index < request.rawHeaders.length; index += 2) {
    const name = request.rawHeaders[index]
    const value = request.rawHeaders[index + 1]
    if (name !== undefined && value !== undefined) headers.append(name, value)
  }
  return headers
}

const readBody = (request: Http.IncomingMessage): Promise<Buffer> =>
  new Promise((resolve, reject) => {
    const chunks = new Array<Buffer>()
    request.on("data", (chunk: Buffer) => {
      chunks.push(chunk)
    })
    request.on("end", () => {
      resolve(Buffer.concat(chunks))
    })
    request.on("error", reject)
  })

const writeWebResponse = async (
  target: Http.ServerResponse,
  source: Response
) => {
  target.statusCode = source.status
  source.headers.forEach((value, key) => {
    target.setHeader(key, value)
  })
  target.end(Buffer.from(await source.arrayBuffer()))
}

const writeJson = (
  response: Http.ServerResponse,
  value: unknown,
  status: number
) => {
  response.writeHead(status, { "content-type": "application/json" })
  response.end(JSON.stringify(value))
}

const writeWebJson = (value: unknown, status: number): Response =>
  new Response(JSON.stringify(value), {
    headers: { "content-type": "application/json" },
    status
  })

const webhookInvocationUrl = (
  request: Request,
  route: FluentWebhookRoute,
  key: string | undefined,
  runId: string | undefined
): URL => {
  const source = new URL(request.url)
  const parts = route.definition._kind === "object"
    ? ["call", route.definition._kind, route.definition.name, key ?? "", route.handler]
    : ["call", route.definition._kind, route.definition.name, route.handler]
  source.pathname = `/${parts.map(encodeURIComponent).join("/")}`
  source.search = ""
  if (runId !== undefined && runId !== "") source.searchParams.set("runId", runId)
  return source
}

const closeServer = (server: Http.Server): Promise<void> =>
  new Promise((resolve, reject) => {
    server.close((cause) => {
      if (cause !== undefined) {
        reject(cause)
        return
      }
      resolve()
    })
  })
