import { describe, expect, it } from "vitest"
import { Effect } from "effect"

import { type ExternalSignalBinding, FluentDurableContext } from "../src/context.ts"
import {
  awakeable,
  AwakeableRejected,
  decodeAwakeableToken,
  rejectAwakeable,
  resolveAwakeable,
  resolveWorkflowEvent,
  workflowEvent
} from "../src/externalEvents.ts"
import { FluentFiregridError } from "../src/error.ts"

const baseContext = (
  overrides: Partial<Parameters<typeof FluentDurableContext.of>[0]> = {}
) =>
  FluentDurableContext.of({
    runId: "run-1",
    signalOperationId: ({ kind, name }) => `run-1:signal:0:${kind}:${name}`,
    sleep: () => Effect.void,
    sleepUntil: () => Effect.void,
    step: () => Effect.fail(new FluentFiregridError({ message: "step not used" })),
    waitForSignal: () => Effect.fail(new FluentFiregridError({ message: "waitForSignal not configured" })),
    ...overrides
  })

describe("awakeable external events", () => {
  it("creates a stable token and resolves from the recorded signal payload", async () => {
    let captured:
      | {
        readonly id?: string
        readonly name: string
      }
      | undefined
    const created = await Effect.runPromise(
      awakeable<string>({ name: "review" }).pipe(
        Effect.provideService(
          FluentDurableContext,
          baseContext({
            waitForSignal: <Payload>(name: string, options?: { readonly id?: string }) =>
              Effect.sync(() => {
                captured = {
                  ...(options?.id === undefined ? {} : { id: options.id }),
                  name
                }
                return { _tag: "AwakeableResolved", value: "approved" } as Payload
              })
          })
        )
      )
    )

    const token = decodeAwakeableToken(created.id)
    expect(token).toMatchObject({
      _tag: "FiregridAwakeable",
      name: "__firegrid_awakeable:run-1:signal:0:awakeable:review",
      runId: "run-1",
      signalId: "awakeable:run-1:signal:0:awakeable:review",
      stepId: "run-1:signal:0:awakeable:review"
    })

    await expect(Effect.runPromise(created.promise)).resolves.toBe("approved")
    expect(captured).toEqual({
      id: "run-1:signal:0:awakeable:review",
      name: "__firegrid_awakeable:run-1:signal:0:awakeable:review"
    })
  })

  it("maps rejected awakeables to a typed failure", async () => {
    const created = await Effect.runPromise(
      awakeable<string>({ name: "review" }).pipe(
        Effect.provideService(
          FluentDurableContext,
          baseContext({
            waitForSignal: <Payload>() =>
              Effect.succeed({ _tag: "AwakeableRejected", reason: { code: "denied" } } as Payload)
          })
        )
      )
    )

    await expect(Effect.runPromise(created.effect)).rejects.toBeInstanceOf(AwakeableRejected)
  })

  it("delivers resolve and reject payloads through explicit or ambient bindings", async () => {
    const deliveries = new Array<unknown>()
    const binding: ExternalSignalBinding<never> = {
      deliverSignal: (request) =>
        Effect.sync(() => {
          deliveries.push(request)
          return { kind: "delivered", runId: request.runId }
        })
    }
    const id = await Effect.runPromise(
      awakeable<string>({ name: "callback" }).pipe(
        Effect.map((created) => created.id),
        Effect.provideService(FluentDurableContext, baseContext())
      )
    )

    await expect(Effect.runPromise(resolveAwakeable(binding, id, "ok"))).resolves.toEqual({
      kind: "delivered",
      runId: "run-1"
    })
    await expect(
      Effect.runPromise(
        rejectAwakeable(id, "no").pipe(
          Effect.provideService(
            FluentDurableContext,
            baseContext({
              externalSignals: binding
            })
          )
        )
      )
    ).resolves.toEqual({
      kind: "delivered",
      runId: "run-1"
    })

    expect(deliveries).toMatchObject([
      {
        name: "__firegrid_awakeable:run-1:signal:0:awakeable:callback",
        payload: { _tag: "AwakeableResolved", value: "ok" },
        runId: "run-1",
        signalId: "awakeable:run-1:signal:0:awakeable:callback",
        stepId: "run-1:signal:0:awakeable:callback"
      },
      {
        name: "__firegrid_awakeable:run-1:signal:0:awakeable:callback",
        payload: { _tag: "AwakeableRejected", reason: "no" },
        runId: "run-1",
        signalId: "awakeable:run-1:signal:0:awakeable:callback",
        stepId: "run-1:signal:0:awakeable:callback"
      }
    ])
  })
})

describe("workflowEvent external events", () => {
  it("waits on a workflow-scoped signal and exposes a resolvable reference", async () => {
    let captured:
      | {
        readonly id?: string
        readonly name: string
      }
      | undefined
    const event = await Effect.runPromise(
      workflowEvent<string>("decision").pipe(
        Effect.provideService(
          FluentDurableContext,
          baseContext({
            waitForSignal: <Payload>(name: string, options?: { readonly id?: string }) =>
              Effect.sync(() => {
                captured = {
                  ...(options?.id === undefined ? {} : { id: options.id }),
                  name
                }
                return "approved" as Payload
              })
          })
        )
      )
    )

    await expect(Effect.runPromise(event.await)).resolves.toBe("approved")
    expect(event).toMatchObject({
      name: "decision",
      runId: "run-1",
      signalId: "workflow-event:run-1:decision",
      stepId: "run-1:signal:0:workflowEvent:decision"
    })
    expect(captured).toEqual({
      id: "run-1:signal:0:workflowEvent:decision",
      name: "__firegrid_workflow_event:decision"
    })
  })

  it("delivers workflow events through explicit or ambient bindings", async () => {
    const deliveries = new Array<unknown>()
    const binding: ExternalSignalBinding<never> = {
      deliverSignal: (request) =>
        Effect.sync(() => {
          deliveries.push(request)
          return { kind: "delivered", runId: request.runId }
        })
    }
    const reference = {
      name: "decision",
      runId: "run-1",
      signalId: "workflow-event:run-1:decision",
      stepId: "run-1:signal:0:workflowEvent:decision"
    }

    await expect(Effect.runPromise(resolveWorkflowEvent(binding, reference, "approved"))).resolves.toEqual({
      kind: "delivered",
      runId: "run-1"
    })
    await expect(
      Effect.runPromise(
        resolveWorkflowEvent(reference, "rejected").pipe(
          Effect.provideService(
            FluentDurableContext,
            baseContext({
              externalSignals: binding
            })
          )
        )
      )
    ).resolves.toEqual({
      kind: "delivered",
      runId: "run-1"
    })

    expect(deliveries).toMatchObject([
      {
        name: "__firegrid_workflow_event:decision",
        payload: "approved",
        runId: "run-1",
        signalId: "workflow-event:run-1:decision",
        stepId: "run-1:signal:0:workflowEvent:decision"
      },
      {
        name: "__firegrid_workflow_event:decision",
        payload: "rejected",
        runId: "run-1",
        signalId: "workflow-event:run-1:decision",
        stepId: "run-1:signal:0:workflowEvent:decision"
      }
    ])
  })
})
