import { Effect, Schema } from "effect"
import { describe, expect, it } from "vitest"
import * as fluent from "../src/index.ts"
import {
  client,
  iface,
  implement,
  run,
  schemas,
  sendClient,
  service,
} from "../src/index.ts"
import type {
  CallRequest,
  DurableExecutionIngress,
  Operation,
  SendRequest,
} from "../src/index.ts"

describe("durable-execution-public-surface", () => {
  it("durable-execution-public-surface: definitions expose runtime-bindable metadata", () => {
    const incident = service({
      name: "incident",
      handlers: {
        *triage(input: string): Operation<string> {
          return yield* run(() => `triaged:${input}`, { name: "triage" })
        },
      },
      descriptors: {
        triage: schemas({
          input: Schema.String,
          output: Schema.String,
        }),
      },
    })

    expect(incident.name).toBe("incident")
    expect(incident._kind).toBe("service")
    expect(Object.keys(incident.handlers)).toEqual(["triage"])
    expect(Object.keys(incident._handlers)).toEqual(["triage"])
    expect(incident._handlers.triage.input).toBe(Schema.String)
    expect(incident._handlers.triage.output).toBe(Schema.String)
  })

  it("durable-execution-public-surface: interface descriptors implement typed definitions", () => {
    const incidentContract = iface.service("incident", {
      triage: iface.schemas({
        input: Schema.String,
        output: Schema.String,
      }),
    })

    const incident = implement(incidentContract, {
      handlers: {
        *triage(input: string): Operation<string> {
          return yield* run(() => `triaged:${input}`, { name: "triage" })
        },
      },
    })

    expect(incident.name).toBe("incident")
    expect(incident._kind).toBe("service")
    expect(incident._handlers.triage).toBe(incidentContract._handlers.triage)
  })

  it("durable-execution-public-surface: typed clients derive call and send shape from definitions", async () => {
    const incident = service({
      name: "incident",
      handlers: {
        *triage(input: string): Operation<string> {
          return yield* run(() => `triaged:${input}`, { name: "triage" })
        },
      },
      descriptors: {
        triage: schemas({
          input: Schema.String,
          output: Schema.String,
        }),
      },
    })
    const calls: Array<CallRequest> = []
    const sends: Array<SendRequest> = []
    const ingress: DurableExecutionIngress<never> = {
      call: (request) =>
        Effect.sync(() => {
          calls.push(request)
        }).pipe(Effect.as(`called:${String(request.input)}` as never)),
      send: (request) =>
        Effect.sync(() => sends.push(request)).pipe(
          Effect.as({ invocationId: `send:${request.name}:${request.handler}` }),
        ),
    }

    const callResult = await Effect.runPromise(
      client(ingress, incident).triage("INC-1"),
    )
    const sendResult = await Effect.runPromise(
      sendClient(ingress, incident).triage("INC-2"),
    )

    expect(callResult).toBe("called:INC-1")
    expect(sendResult).toEqual({ invocationId: "send:incident:triage" })
    expect(calls).toMatchObject([
      {
        kind: "service",
        name: "incident",
        handler: "triage",
        input: "INC-1",
      },
    ])
    expect(sends).toMatchObject([
      {
        kind: "service",
        name: "incident",
        handler: "triage",
        input: "INC-2",
      },
    ])
    expect(calls[0]?.descriptor).toBe(incident._handlers.triage)
    expect(sends[0]?.descriptor).toBe(incident._handlers.triage)
  })

  it("durable-execution-public-surface: root exports do not expose scheduler internals", () => {
    expect("Scheduler" in fluent).toBe(false)
    expect("AwaitableLib" in fluent).toBe(false)
  })
})
