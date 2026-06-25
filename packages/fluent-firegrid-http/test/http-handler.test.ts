import {
  iface,
  implement,
  object,
  run,
  schemas,
  service,
  type CallRequest,
  type InvocationBinding,
  type SendReference,
  type SendRequest
} from "@firegrid/fluent-firegrid"
import { createFluentHttpHandler } from "@firegrid/fluent-firegrid-http"
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

describe("createFluentHttpHandler", () => {
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

    const response = await handler(request("/send/workflow/emails/send", { id: "email-1" }, { "x-firegrid-run-id": "run-2" }))

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
