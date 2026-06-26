import {
  awakeable,
  type CallRequest,
  type ExternalSignalBinding,
  FluentDurableContext,
  FluentFiregridError,
  iface,
  implement,
  type InvocationBinding,
  object,
  run,
  schemas,
  type SendReference,
  type SendRequest,
  service
} from "@firegrid/fluent-firegrid"
import {
  type AwakeableHttpClientError,
  createAwakeableHttpClient,
  createFluentHttpHandler
} from "@firegrid/fluent-firegrid-http"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import { describe, expect, it } from "vitest"

const request = (path: string, body: unknown, headers?: HeadersInit): Request =>
  new Request(`http://fluent.test${path}`, {
    body: JSON.stringify(body),
    headers: {
      "content-type": "application/json",
      ...headers
    },
    method: "POST"
  })

const json = async <A>(response: Response): Promise<A> => await response.json() as A

describe("createAwakeableHttpClient", () => {
  it("posts resolve and reject payloads to awakeable endpoints", async () => {
    const requests = new Array<{
      readonly body: unknown
      readonly headers: Record<string, string>
      readonly method: string
      readonly url: string
    }>()
    const client = createAwakeableHttpClient({
      baseUrl: "https://callbacks.test/base/",
      fetch: async (input, init) => {
        requests.push({
          body: JSON.parse(String(init?.body)),
          headers: Object.fromEntries(new Headers(init?.headers).entries()),
          method: init?.method ?? "GET",
          url: String(input)
        })
        return Response.json({ kind: "delivered", runId: "run-1" }, { status: 202 })
      },
      headers: { authorization: "Bearer token" }
    })

    await expect(client.resolve("token/1", "ok")).resolves.toEqual({ kind: "delivered", runId: "run-1" })
    await expect(client.reject("token/1", "no")).resolves.toEqual({ kind: "delivered", runId: "run-1" })

    expect(requests).toEqual([
      {
        body: { value: "ok" },
        headers: {
          authorization: "Bearer token",
          "content-type": "application/json"
        },
        method: "POST",
        url: "https://callbacks.test/base/firegrid/awakeables/token%2F1/resolve"
      },
      {
        body: { reason: "no" },
        headers: {
          authorization: "Bearer token",
          "content-type": "application/json"
        },
        method: "POST",
        url: "https://callbacks.test/base/firegrid/awakeables/token%2F1/reject"
      }
    ])
  })

  it("throws a typed error for failed awakeable HTTP delivery", async () => {
    const client = createAwakeableHttpClient({
      baseUrl: "https://callbacks.test",
      fetch: async () => Response.json({ error: "not_found" }, { status: 404 })
    })

    await expect(client.resolve("missing", "ok")).rejects.toMatchObject(
      {
        _tag: "AwakeableHttpClientError",
        body: { error: "not_found" },
        status: 404
      } satisfies Partial<AwakeableHttpClientError>
    )
  })
})

describe("createFluentHttpHandler", () => {
  it("routes awakeable resolve and reject endpoints through external signal bindings", async () => {
    const testSleep = Effect.fn("testSleep")(function*() {})
    const testSleepUntil = Effect.fn("testSleepUntil")(function*() {})
    const unusedStep = Effect.fn("unusedStep")(function*() {
      return yield* new FluentFiregridError({ message: "step not used" })
    })
    const unusedWaitForSignal = Effect.fn("unusedWaitForSignal")(function*() {
      return yield* new FluentFiregridError({ message: "waitForSignal not used" })
    })
    const id = await Effect.runPromise(
      awakeable<string>({ name: "review" }).pipe(
        Effect.map((created) => created.id),
        Effect.provideService(
          FluentDurableContext,
          FluentDurableContext.of({
            runId: "run-1",
            signalOperationId: ({ kind, name }) => `run-1:signal:0:${kind}:${name}`,
            sleep: testSleep,
            sleepUntil: testSleepUntil,
            step: unusedStep,
            waitForSignal: unusedWaitForSignal
          })
        )
      )
    )
    const deliveries = new Array<unknown>()
    const externalSignals: ExternalSignalBinding<never> = {
      deliverSignal: (delivery) =>
        Effect.sync(() => {
          deliveries.push(delivery)
          return { kind: "delivered", runId: delivery.runId }
        })
    }
    const binding: InvocationBinding<never> = {
      call: <Output>() => Effect.succeed("not-used" as Output),
      send: <Output>() => Effect.succeed({ invocationId: "not-used" } satisfies SendReference<Output>)
    }
    const handler = createFluentHttpHandler({ binding, definitions: [], externalSignals })

    const resolved = await handler(request(`/firegrid/awakeables/${encodeURIComponent(id)}/resolve`, { value: "ok" }))
    const rejected = await handler(request(`/firegrid/awakeables/${encodeURIComponent(id)}/reject`, { reason: "no" }))

    expect(resolved.status).toBe(202)
    expect(rejected.status).toBe(202)
    expect(await json(resolved)).toEqual({ kind: "delivered", runId: "run-1" })
    expect(await json(rejected)).toEqual({ kind: "delivered", runId: "run-1" })
    expect(deliveries).toMatchObject([
      {
        name: "__firegrid_awakeable:run-1:signal:0:awakeable:review",
        payload: { _tag: "AwakeableResolved", value: "ok" },
        runId: "run-1"
      },
      {
        name: "__firegrid_awakeable:run-1:signal:0:awakeable:review",
        payload: { _tag: "AwakeableRejected", reason: "no" },
        runId: "run-1"
      }
    ])
  })

  it("routes descriptor-backed service calls through the invocation binding", async () => {
    const incident = service({
      name: "incident",
      handlers: {
        *triage(input: string) {
          return yield* run(() => `triaged:${input}`, { name: "triage" })
        }
      },
      descriptors: {
        triage: schemas({
          input: Schema.String,
          output: Schema.String
        })
      }
    })
    const calls = new Array<CallRequest>()
    const binding: InvocationBinding<never> = {
      call: <Output>(callRequest: CallRequest) =>
        Effect.sync(() => {
          calls.push(callRequest)
          return `called:${String(callRequest.input)}` as Output
        }),
      send: <Output>() => Effect.succeed({ invocationId: "not-used" } satisfies SendReference<Output>)
    }
    const handler = createFluentHttpHandler({ binding, definitions: [incident] })

    const response = await handler(request("/call/service/incident/triage?runId=run-1", "INC-1"))

    expect(response.status).toBe(200)
    expect(await json(response)).toEqual({ output: "called:INC-1" })
    expect(calls[0]).toMatchObject({
      handler: "triage",
      input: "INC-1",
      kind: "service",
      name: "incident",
      runId: "run-1"
    })
    expect(calls[0]?.descriptor).toBe(incident._handlers.triage)
  })

  it("returns send references with 202 status", async () => {
    const contract = iface.workflow("emails", {
      send: iface.schemas({
        input: Schema.Struct({ id: Schema.String }),
        output: Schema.Struct({ ok: Schema.Boolean })
      })
    })
    const emails = implement(contract, {
      handlers: {
        *send(input) {
          return yield* run(() => ({ ok: input.id.length > 0 }), { name: "send" })
        }
      }
    })
    const sends = new Array<SendRequest>()
    const binding: InvocationBinding<never> = {
      call: <Output>() => Effect.succeed("not-used" as Output),
      send: <Output>(sendRequest: SendRequest) =>
        Effect.sync(() => {
          sends.push(sendRequest)
          return { invocationId: "email-1" } satisfies SendReference<Output>
        })
    }
    const handler = createFluentHttpHandler({ binding, definitions: [emails] })

    const response = await handler(
      request("/send/workflow/emails/send", { id: "email-1" }, { "x-firegrid-run-id": "run-2" })
    )

    expect(response.status).toBe(202)
    expect(await json(response)).toEqual({ invocationId: "email-1" })
    expect(sends[0]).toMatchObject({
      handler: "send",
      input: { id: "email-1" },
      kind: "workflow",
      name: "emails",
      runId: "run-2"
    })
  })

  it("routes keyed object calls", async () => {
    const counter = object({
      name: "counter",
      handlers: {
        *add(input: { readonly by: number }) {
          return yield* run(() => input.by, { name: "add" })
        }
      },
      descriptors: {
        add: schemas({
          input: Schema.Struct({ by: Schema.Number }),
          output: Schema.Number
        })
      }
    })
    const calls = new Array<CallRequest>()
    const binding: InvocationBinding<never> = {
      call: <Output>(callRequest: CallRequest) =>
        Effect.sync(() => {
          calls.push(callRequest)
          return (callRequest.input as { readonly by: number }).by as Output
        }),
      send: <Output>() => Effect.succeed({ invocationId: "not-used" } satisfies SendReference<Output>)
    }
    const handler = createFluentHttpHandler({ binding, definitions: [counter] })

    const response = await handler(request("/call/object/counter/user-1/add", { by: 3 }))

    expect(response.status).toBe(200)
    expect(calls[0]).toMatchObject({
      handler: "add",
      input: { by: 3 },
      key: "user-1",
      kind: "object",
      name: "counter"
    })
  })

  it("rejects invalid descriptor input before invoking the binding", async () => {
    const incident = service({
      name: "incident",
      handlers: {
        *triage(input: string) {
          return yield* run(() => input, { name: "triage" })
        }
      },
      descriptors: {
        triage: schemas({
          input: Schema.String,
          output: Schema.String
        })
      }
    })
    let called = false
    const binding: InvocationBinding<never> = {
      call: <Output>() =>
        Effect.sync(() => {
          called = true
          return "not-called" as Output
        }),
      send: <Output>() => Effect.succeed({ invocationId: "not-used" } satisfies SendReference<Output>)
    }
    const handler = createFluentHttpHandler({ binding, definitions: [incident] })

    const response = await handler(request("/call/service/incident/triage", 42))

    expect(response.status).toBe(400)
    expect((await json<{ readonly error: string }>(response)).error).toBe("invalid_input")
    expect(called).toBeFalsy()
  })
})
