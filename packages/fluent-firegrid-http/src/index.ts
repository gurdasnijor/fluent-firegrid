/* oxlint-disable effect/restricted-syntax -- HTTP Request/Response handling is a runtime boundary that must run fluent Effects. */
import {
  type AnyGeneratorHandler,
  type CallRequest,
  type Definition,
  type DefinitionKind,
  type ExternalSignalBinding,
  type ExternalSignalDelivery,
  FluentFiregridError,
  type HandlerDescriptor,
  type InvocationBinding,
  rejectAwakeable,
  resolveAwakeable,
  type SendRequest
} from "@firegrid/fluent-firegrid"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import type { ConstraintDecoder, ConstraintEncoder } from "effect/Schema"

type AnyDefinition = Definition<string, DefinitionKind, Record<string, AnyGeneratorHandler>>
type TransportMode = "call" | "send"

export interface AwakeableHttpClientOptions {
  readonly baseUrl: string | URL
  readonly fetch?: typeof fetch
  readonly headers?: HeadersInit | (() => HeadersInit | Promise<HeadersInit>)
}

export interface AwakeableHttpClient {
  readonly reject: (id: string, reason: unknown) => Promise<ExternalSignalDelivery>
  readonly resolve: <T>(id: string, value: T) => Promise<ExternalSignalDelivery>
}

export class AwakeableHttpClientError extends Schema.TaggedErrorClass<AwakeableHttpClientError>()(
  "AwakeableHttpClientError",
  {
    body: Schema.Unknown,
    message: Schema.String,
    status: Schema.Number
  }
) {}

export const createAwakeableHttpClient = (
  options: AwakeableHttpClientOptions
): AwakeableHttpClient => {
  const fetchImpl = options.fetch ?? fetch
  const post = async (id: string, action: "reject" | "resolve", body: unknown): Promise<ExternalSignalDelivery> => {
    const response = await fetchImpl(awakeableUrl(options.baseUrl, id, action), {
      body: JSON.stringify(body),
      headers: await awakeableHeaders(options.headers),
      method: "POST"
    })
    const payload = await response.json().catch(() => undefined) as unknown
    if (!response.ok) {
      throw new AwakeableHttpClientError({
        body: payload,
        message: `awakeable HTTP delivery failed with status ${response.status}`,
        status: response.status
      })
    }
    return payload as ExternalSignalDelivery
  }
  return {
    reject: (id, reason) => post(id, "reject", { reason }),
    resolve: (id, value) => post(id, "resolve", { value })
  }
}

export interface FluentHttpHandlerOptions<Error = unknown> {
  readonly binding: InvocationBinding<Error>
  readonly definitions: ReadonlyArray<AnyDefinition>
  readonly externalSignals?: ExternalSignalBinding<FluentFiregridError>
}

interface RouteMatch {
  readonly handler: string
  readonly key?: string
  readonly kind: DefinitionKind
  readonly mode: TransportMode
  readonly name: string
}

interface RouteTarget {
  readonly definition: AnyDefinition
  readonly descriptor?: HandlerDescriptor
  readonly route: RouteMatch
}

export const createFluentHttpHandler = <Error = unknown>(
  options: FluentHttpHandlerOptions<Error>
): (request: Request) => Promise<Response> => {
  const registry = createRegistry(options.definitions)
  return async (request) => {
    if (request.method !== "POST") {
      return jsonResponse({ error: "method_not_allowed" }, 405)
    }

    const url = new URL(request.url)
    const externalRoute = parseExternalEventRoute(url)
    if (externalRoute !== undefined) {
      return await handleExternalEventRoute(options, request, externalRoute)
    }

    const route = parseRoute(url)
    if (route === undefined) {
      return jsonResponse({ error: "not_found" }, 404)
    }

    const target = registry.get(keyFor(route.kind, route.name, route.handler))
    if (target === undefined) {
      return jsonResponse({ error: "not_found" }, 404)
    }
    if (route.kind === "object" && route.key === undefined) {
      return jsonResponse({ error: "object_key_required" }, 400)
    }

    const body = await parseJsonBody(request)
    if (body._tag === "Error") {
      return jsonResponse({ error: "invalid_json", message: body.message }, 400)
    }

    const input = await decodeInput(target, body.value)
    if (input._tag === "Error") {
      return jsonResponse({ error: "invalid_input", message: input.message }, 400)
    }

    const runId = readRunId(request)
    const requestEnvelope: CallRequest = {
      handler: route.handler,
      input: input.value,
      kind: route.kind,
      name: route.name,
      ...(target.descriptor === undefined ? {} : { descriptor: target.descriptor }),
      ...(route.key === undefined ? {} : { key: route.key }),
      ...(runId === undefined ? {} : { runId })
    }

    const result = await invoke(options.binding, route.mode, requestEnvelope)
    if (result._tag === "Error") {
      return jsonResponse({ error: "fluent_invocation_failed", message: result.message }, 500)
    }

    if (route.mode === "send") {
      return jsonResponse(result.value, 202)
    }

    const output = await encodeOutput(target, result.value)
    if (output._tag === "Error") {
      return jsonResponse({ error: "invalid_output", message: output.message }, 500)
    }
    return jsonResponse({ output: output.value }, 200)
  }
}

interface ExternalEventRoute {
  readonly action: "reject" | "resolve"
  readonly id: string
}

const createRegistry = (definitions: ReadonlyArray<AnyDefinition>): ReadonlyMap<string, RouteTarget> => {
  const registry = new Map<string, RouteTarget>()
  definitions.forEach((definition) => {
    Object.keys(definition._handlers).forEach((handler) => {
      registry.set(keyFor(definition._kind, definition.name, handler), {
        definition,
        ...(definition._handlers[handler] === undefined ? {} : { descriptor: definition._handlers[handler] }),
        route: {
          handler,
          kind: definition._kind,
          mode: "call",
          name: definition.name
        }
      })
    })
  })
  return registry
}

const keyFor = (kind: DefinitionKind, name: string, handler: string): string => `${kind}:${name}:${handler}`

const parseExternalEventRoute = (url: URL): ExternalEventRoute | undefined => {
  const parts = url.pathname.split("/").filter((part) => part.length > 0).map(decodeURIComponent)
  if (parts[0] !== "firegrid" || parts[1] !== "awakeables") return undefined
  const id = parts[2]
  const action = parts[3]
  if (id === undefined || (action !== "resolve" && action !== "reject") || parts.length !== 4) return undefined
  return { action, id }
}

const awakeableUrl = (
  baseUrl: string | URL,
  id: string,
  action: "reject" | "resolve"
): URL => {
  const url = new URL(baseUrl)
  const basePath = url.pathname.endsWith("/") ? url.pathname.slice(0, -1) : url.pathname
  url.pathname = `${basePath}/firegrid/awakeables/${encodeURIComponent(id)}/${action}`
  url.search = ""
  return url
}

const awakeableHeaders = async (
  source: AwakeableHttpClientOptions["headers"]
): Promise<Headers> => {
  const headers = new Headers({ "content-type": "application/json" })
  const resolved = typeof source === "function" ? await source() : source
  if (resolved !== undefined) {
    new Headers(resolved).forEach((value, key) => {
      headers.set(key, value)
    })
  }
  return headers
}

const parseRoute = (url: URL): RouteMatch | undefined => {
  const parts = url.pathname.split("/").filter((part) => part.length > 0).map(decodeURIComponent)
  const mode = parts[0]
  if (mode !== "call" && mode !== "send") return undefined
  const kind = parts[1]
  if (!isDefinitionKind(kind)) return undefined
  if (kind === "object") {
    const name = parts[2]
    const key = parts[3]
    const handler = parts[4]
    if (name === undefined || key === undefined || handler === undefined || parts.length !== 5) return undefined
    return { handler, key, kind, mode, name }
  }
  const name = parts[2]
  const handler = parts[3]
  if (name === undefined || handler === undefined || parts.length !== 4) return undefined
  return { handler, kind, mode, name }
}

const handleExternalEventRoute = async <Error>(
  options: FluentHttpHandlerOptions<Error>,
  request: Request,
  route: ExternalEventRoute
): Promise<Response> => {
  if (options.externalSignals === undefined) {
    return jsonResponse({ error: "external_signals_not_configured" }, 500)
  }
  const body = await parseJsonBody(request)
  if (body._tag === "Error") {
    return jsonResponse({ error: "invalid_json", message: body.message }, 400)
  }
  const value = externalEventPayload(route.action, body.value)
  const result = await runEffect(
    route.action === "resolve"
      ? resolveAwakeable(options.externalSignals, route.id, value)
      : rejectAwakeable(options.externalSignals, route.id, value)
  )
  if (result._tag === "Error") {
    return jsonResponse({ error: "external_signal_delivery_failed", message: result.message }, 500)
  }
  return jsonResponse(result.value, 202)
}

const externalEventPayload = (action: ExternalEventRoute["action"], body: unknown): unknown => {
  if (typeof body === "object" && body !== null) {
    if (action === "resolve" && "value" in body) return (body as { readonly value?: unknown }).value
    if (action === "reject" && "reason" in body) return (body as { readonly reason?: unknown }).reason
  }
  return body
}

const isDefinitionKind = (value: string | undefined): value is DefinitionKind =>
  value === "service" || value === "workflow" || value === "object"

const parseJsonBody = async (request: Request): Promise<Result<unknown>> => {
  try {
    return { _tag: "Ok", value: await request.json() }
  } catch (cause) {
    return { _tag: "Error", message: cause instanceof Error ? cause.message : "failed to parse request JSON" }
  }
}

const decodeInput = async (target: RouteTarget, value: unknown): Promise<Result<unknown>> => {
  if (target.descriptor?.input === undefined) return { _tag: "Ok", value }
  try {
    return {
      _tag: "Ok",
      value: await Schema.decodeUnknownPromise(target.descriptor.input as unknown as ConstraintDecoder<unknown>)(value)
    }
  } catch (cause) {
    return {
      _tag: "Error",
      message: new FluentFiregridError({
        cause,
        message: `invalid input for ${target.definition.name}.${target.route.handler}`
      }).message
    }
  }
}

const encodeOutput = async (target: RouteTarget, value: unknown): Promise<Result<unknown>> => {
  if (target.descriptor?.output === undefined) return { _tag: "Ok", value }
  try {
    return {
      _tag: "Ok",
      value: await Schema.encodeUnknownPromise(target.descriptor.output as unknown as ConstraintEncoder<unknown>)(value)
    }
  } catch (cause) {
    return {
      _tag: "Error",
      message: new FluentFiregridError({
        cause,
        message: `invalid output for ${target.definition.name}.${target.route.handler}`
      }).message
    }
  }
}

const invoke = async <Error>(
  binding: InvocationBinding<Error>,
  mode: TransportMode,
  request: CallRequest
): Promise<Result<unknown>> => runEffect(mode === "call" ? binding.call(request) : binding.send(request as SendRequest))

const runEffect = async <A, Error>(
  effect: Effect.Effect<A, Error>
): Promise<Result<A>> => {
  try {
    return { _tag: "Ok", value: await Effect.runPromise(effect) }
  } catch (cause) {
    return { _tag: "Error", message: errorMessage(cause) }
  }
}

const readRunId = (request: Request): string | undefined => {
  const url = new URL(request.url)
  return url.searchParams.get("runId") ?? request.headers.get("x-firegrid-run-id") ?? undefined
}

const jsonResponse = (body: unknown, status: number): Response =>
  new Response(JSON.stringify(body), {
    headers: { "content-type": "application/json" },
    status
  })

type Result<A> =
  | { readonly _tag: "Ok"; readonly value: A }
  | { readonly _tag: "Error"; readonly message: string }

const errorMessage = (cause: unknown): string => {
  if (cause instanceof Error) return cause.message
  return String(cause)
}
