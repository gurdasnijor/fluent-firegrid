import { Effect, Schema } from "effect"
import { describe, expect, it } from "vitest"
import { defineWorkflowRuntime, inMemoryWorkflowExecutionStore } from "@tanstack/workflow-runtime"

import {
  bindFluentDefinitions,
  client,
  createTanStackRuntimeBinding,
  iface,
  implement,
  run,
  schemas,
  sendClient,
  service,
  serviceClient,
  type CallRequest,
  type FluentFiregridError,
  FluentDurableContext,
  type FluentDurableContextService,
  type InvocationBinding,
  type RunAction,
  type SendReference,
  type SendRequest,
  workflowIdForHandler
} from "../src/index.ts"

describe("fluent-firegrid public surface", () => {
  it("attaches schema descriptors to direct definitions", () => {
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

    expect(incident.name).toBe("incident")
    expect(incident._kind).toBe("service")
    expect(Object.keys(incident.handlers)).toEqual(["triage"])
    expect(incident._handlers.triage.input).toBe(Schema.String)
    expect(incident._handlers.triage.output).toBe(Schema.String)
  })

  it("implements descriptor-only interfaces with generator handlers", () => {
    const incidentContract = iface.service("incident", {
      triage: iface.schemas({
        input: Schema.String,
        output: Schema.String
      })
    })

    const incident = implement(incidentContract, {
      handlers: {
        *triage(input: string) {
          return yield* run(() => `triaged:${input}`, { name: "triage" })
        }
      }
    })

    expect(incident.name).toBe("incident")
    expect(incident._kind).toBe("service")
    expect(incident._handlers.triage).toBe(incidentContract._handlers.triage)
  })

  it("derives typed call/send clients from implemented interfaces", async () => {
    const incidentContract = iface.service("incident", {
      triage: iface.schemas({
        input: Schema.String,
        output: Schema.String
      })
    })
    const incident = implement(incidentContract, {
      handlers: {
        *triage(input: string) {
          return yield* run(() => `triaged:${input}`, { name: "triage" })
        }
      }
    })
    const calls = new Array<CallRequest>()
    const sends = new Array<SendRequest>()
    const binding: InvocationBinding<never> = {
      call: <Output>(request: CallRequest) =>
        Effect.sync(() => {
          calls.push(request)
          return `called:${String(request.input)}` as Output
        }),
      send: <Output>(request: SendRequest) =>
        Effect.sync(() => {
          sends.push(request)
          return { invocationId: `send:${request.name}:${request.handler}` } satisfies SendReference<Output>
        })
    }

    const callResult = await Effect.runPromise(client(binding, incident).triage("INC-1"))
    const sendResult = await Effect.runPromise(sendClient(binding, incident).triage("INC-2"))

    expect(callResult).toBe("called:INC-1")
    expect(sendResult).toEqual({ invocationId: "send:incident:triage" })
    expect(calls[0]).toMatchObject({
      handler: "triage",
      input: "INC-1",
      kind: "service",
      name: "incident"
    })
    expect(sends[0]).toMatchObject({
      handler: "triage",
      input: "INC-2",
      kind: "service",
      name: "incident"
    })
    expect(calls[0]?.descriptor).toBe(incidentContract._handlers.triage)
    expect(sends[0]?.descriptor).toBe(incidentContract._handlers.triage)
  })

  it("resolves ambient handler clients from FluentDurableContext", async () => {
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
      call: <Output>(request: CallRequest) =>
        Effect.sync(() => {
          calls.push(request)
          return `ambient:${String(request.input)}` as Output
        }),
      send: <Output>() => Effect.succeed({ invocationId: "not-used" } satisfies SendReference<Output>)
    }
    const context = FluentDurableContext.of({
      binding,
      sleep: () => Effect.void,
      sleepUntil: () => Effect.void,
      step: <A>(_name: string, action: RunAction<A>) => {
        const value = action({ attempt: 1, id: "step", signal: new AbortController().signal })
        return (Effect.isEffect(value)
          ? value
          : Effect.promise(() => Promise.resolve(value))) as Effect.Effect<A, never>
      },
      waitForSignal: () => Effect.die("not used")
    } satisfies FluentDurableContextService)

    const result = await Effect.runPromise(
      serviceClient(incident).triage("INC-4").pipe(Effect.provideService(FluentDurableContext, context))
    )

    expect(result).toBe("ambient:INC-4")
    expect(calls[0]).toMatchObject({
      handler: "triage",
      input: "INC-4",
      kind: "service",
      name: "incident"
    })
    expect(calls[0]?.descriptor).toBe(incident._handlers.triage)
  })

  it("provides ambient clients through bindFluentDefinitions", async () => {
    const worker = service({
      name: "worker",
      handlers: {
        *review(input: string) {
          return yield* run(() => `reviewed:${input}`, { name: "review" })
        }
      },
      descriptors: {
        review: schemas({
          input: Schema.String,
          output: Schema.String
        })
      }
    })
    const coordinator = service({
      name: "coordinator",
      handlers: {
        *route(input: string) {
          return yield* serviceClient(worker).review(input)
        }
      },
      descriptors: {
        route: schemas({
          input: Schema.String,
          output: Schema.String
        })
      }
    })
    let binding: InvocationBinding<FluentFiregridError> | undefined
    const runtime = defineWorkflowRuntime({
      store: inMemoryWorkflowExecutionStore(),
      workflows: bindFluentDefinitions([worker, coordinator], {
        invocationBinding: () => binding
      })
    })
    binding = createTanStackRuntimeBinding({ runtime })

    const result = await runtime.startRun({
      input: { input: "INC-5" },
      now: 1,
      runId: "ambient-client-host",
      workflowId: workflowIdForHandler(coordinator, "route")
    })

    expect(result.kind).toBe("completed")
    expect(result.run?.output).toBe("reviewed:INC-5")
  })

  it("validates descriptor schemas at the TanStack handler boundary", async () => {
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
    const runtime = defineWorkflowRuntime({
      store: inMemoryWorkflowExecutionStore(),
      workflows: bindFluentDefinitions([incident])
    })
    const workflowId = workflowIdForHandler(incident, "triage")

    const completed = await runtime.startRun({
      input: { input: "INC-3" },
      now: 1,
      runId: "valid-input",
      workflowId
    })
    const errored = await runtime.startRun({
      input: { input: 42 },
      now: 2,
      runId: "invalid-input",
      workflowId
    })

    expect(completed.kind).toBe("completed")
    expect(completed.run?.output).toBe("triaged:INC-3")
    expect(errored.kind).toBe("errored")
    expect(errored.run?.error?.message).toContain("invalid input for fluent handler incident.triage")
  })
})
