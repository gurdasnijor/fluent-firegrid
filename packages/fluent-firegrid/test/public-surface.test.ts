import { Effect, Schema } from "effect"
import { describe, expect, expectTypeOf, it } from "vitest"
import { defineWorkflowRuntime, inMemoryWorkflowExecutionStore } from "@tanstack/workflow-runtime"

import {
  attach,
  bindFluentDefinitions,
  type CallRequest,
  client,
  createTanStackExternalSignalBinding,
  createTanStackRuntimeBinding,
  FluentDurableContext,
  type FluentDurableContextService,
  type FluentFiregridError,
  genericCall,
  genericSend,
  iface,
  implement,
  type InvocationBinding,
  type InvocationHandle,
  objectSendClient,
  rpc,
  run,
  type RunAction,
  schemas,
  sendClient,
  sendObjectClient,
  type SendReference,
  type SendRequest,
  sendServiceClient,
  sendWorkflowClient,
  service,
  serviceClient,
  serviceSendClient,
  workflowIdForHandler,
  workflowSendClient
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
    const sendOutput = await Effect.runPromise(sendResult.outputEffect())

    expect(callResult).toBe("called:INC-1")
    expect(sendResult).toEqual({ invocationId: "send:incident:triage" })
    expect(sendOutput).toBe("called:INC-2")
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
    expect(calls[1]).toMatchObject({
      handler: "triage",
      input: "INC-2",
      kind: "service",
      name: "incident",
      runId: "send:incident:triage"
    })
    expect(calls[0]?.descriptor).toBe(incidentContract._handlers.triage)
    expect(calls[1]?.descriptor).toBe(incidentContract._handlers.triage)
    expect(sends[0]?.descriptor).toBe(incidentContract._handlers.triage)
  })

  it("normalizes invocation options and exposes Restate-like send aliases", async () => {
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
          return {
            handler: request.handler,
            invocationId: request.runId ?? "generated",
            kind: request.kind,
            name: request.name
          } satisfies SendReference<Output>
        })
    }

    await Effect.runPromise(
      client(binding, incident).triage(
        "INC-7",
        rpc.opts({
          idempotencyKey: "incident:INC-7",
          metadata: { source: "test" }
        })
      )
    )
    const handle = await Effect.runPromise(
      serviceSendClient(binding, incident).triage(
        "INC-8",
        rpc.sendOpts({
          delay: rpc.duration({ minutes: 2, seconds: 3 }),
          idempotencyKey: "incident:INC-8"
        })
      )
    )

    expect(sendServiceClient).toBe(serviceSendClient)
    expect(workflowSendClient).toBe(sendWorkflowClient)
    expect(objectSendClient).toBe(sendObjectClient)
    expect(handle.invocationId).toBe("incident:INC-8")
    expect(calls[0]).toMatchObject({
      handler: "triage",
      idempotencyKey: "incident:INC-7",
      metadata: { source: "test" },
      runId: "incident:INC-7"
    })
    expect(sends[0]).toMatchObject({
      delayMs: 123_000,
      handler: "triage",
      idempotencyKey: "incident:INC-8",
      runId: "incident:INC-8"
    })
  })

  it("supports generic call/send and attach-by-reference", async () => {
    const calls = new Array<CallRequest>()
    const sends = new Array<SendRequest>()
    const binding: InvocationBinding<never> = {
      call: <Output>(request: CallRequest) =>
        Effect.sync(() => {
          calls.push(request)
          return `generic:${request.name}:${request.handler}:${String(request.input)}:${request.runId}` as Output
        }),
      send: <Output>(request: SendRequest) =>
        Effect.sync(() => {
          sends.push(request)
          return {
            handler: request.handler,
            invocationId: request.runId ?? "generic-run",
            kind: request.kind,
            name: request.name
          } satisfies SendReference<Output>
        })
    }

    const output = await Effect.runPromise(
      genericCall<string>(binding, {
        handler: "triage",
        idempotencyKey: "incident:INC-9",
        input: "INC-9",
        kind: "service",
        name: "incident"
      })
    )
    const handle = await Effect.runPromise(
      genericSend<string>(binding, {
        handler: "triage",
        input: "INC-10",
        kind: "service",
        name: "incident",
        runId: "manual-run"
      })
    )
    const attached = await Effect.runPromise(attach(binding, {
      handler: "triage",
      input: "INC-11",
      invocationId: "attached-run",
      kind: "service",
      name: "incident"
    }))

    expect(output).toBe("generic:incident:triage:INC-9:incident:INC-9")
    expect(handle.invocationId).toBe("manual-run")
    expect(attached).toBe("generic:incident:triage:INC-11:attached-run")
    expect(sends[0]).toMatchObject({
      handler: "triage",
      input: "INC-10",
      kind: "service",
      name: "incident",
      runId: "manual-run"
    })
  })

  it("adapts runtime deliverSignal for external event helpers", async () => {
    const deliveries = new Array<unknown>()
    const binding = createTanStackExternalSignalBinding({
      runtime: {
        deliverSignal: async (request) => {
          deliveries.push(request)
          return {
            eventCount: 1,
            events: [],
            kind: "completed",
            runId: request.runId,
            workflowId: "service:reviews:complete"
          }
        },
        startRun: async () => ({
          eventCount: 0,
          events: [],
          kind: "completed",
          runId: "unused"
        })
      }
    }, { now: () => 1_234 })

    await expect(
      Effect.runPromise(
        binding.deliverSignal({
          name: "review",
          payload: { ok: true },
          runId: "run-1",
          signalId: "signal-1",
          stepId: "step-1"
        })
      )
    ).resolves.toEqual({
      kind: "completed",
      runId: "run-1",
      workflowId: "service:reviews:complete"
    })
    expect(deliveries).toEqual([
      {
        name: "review",
        now: 1_234,
        payload: { ok: true },
        runId: "run-1",
        signalId: "signal-1",
        stepId: "step-1"
      }
    ])
  })

  it("rejects delayed starts on bindings without durable delayed-send support", async () => {
    const binding = createTanStackRuntimeBinding({
      runtime: {
        startRun: async () => ({
          eventCount: 0,
          events: [],
          kind: "completed",
          runId: "should-not-run"
        })
      }
    })

    await expect(
      Effect.runPromise(
        binding.send({
          delayMs: 1_000,
          handler: "triage",
          input: "INC-9",
          kind: "service",
          name: "incident",
          runId: "delayed"
        })
      )
    ).rejects.toMatchObject({
      _tag: "FluentFiregridError",
      message: "delayed fluent invocations require a binding with durable delayed-send support"
    })
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
    const context = FluentDurableContext.of(
      {
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
      } satisfies FluentDurableContextService
    )

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

  it("resolves ambient send handles through the captured invocation binding", async () => {
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
          return `attached:${String(request.input)}:${request.runId}` as Output
        }),
      send: <Output>(request: SendRequest) =>
        Effect.succeed(
          {
            handler: request.handler,
            invocationId: `send:${request.name}:${request.handler}`,
            kind: request.kind,
            name: request.name
          } satisfies SendReference<Output>
        )
    }
    const context = FluentDurableContext.of(
      {
        binding,
        sleep: () => Effect.void,
        sleepUntil: () => Effect.void,
        step: () => Effect.die("not used"),
        waitForSignal: () => Effect.die("not used")
      } satisfies FluentDurableContextService
    )

    const handle = await Effect.runPromise(
      sendClient(binding, incident).triage("INC-5")
    )
    const ambientHandle = await Effect.runPromise(
      sendServiceClient(incident).triage("INC-6").pipe(Effect.provideService(FluentDurableContext, context))
    )
    const output = await Effect.runPromise(ambientHandle.attach())

    expect(handle).toEqual({
      handler: "triage",
      invocationId: "send:incident:triage",
      kind: "service",
      name: "incident"
    })
    expectTypeOf(handle).toEqualTypeOf<InvocationHandle<string, never>>()
    expect(output).toBe("attached:INC-6:send:incident:triage")
    expect(calls[0]).toMatchObject({
      handler: "triage",
      input: "INC-6",
      kind: "service",
      name: "incident",
      runId: "send:incident:triage"
    })
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
