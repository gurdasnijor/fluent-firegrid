import {
  Clock,
  Duration,
  Effect,
  Exit,
  Fiber,
  FiberMap,
  Layer,
  Option,
  type Scope,
} from "effect"
import { Workflow } from "effect/unstable/workflow"
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine"
import { executionStreamName } from "./names.ts"
import type { S2WorkflowEngineConfigTag } from "./config.ts"
import {
  activityCompleted,
  deferredCompleted,
  executionCompleted,
  executionStarted,
  interruptRequested,
  timerScheduled,
  type WorkflowRecord,
  type WorkflowResult,
} from "./records.ts"
import {
  activityId,
  completedResultOption,
  deferredId,
  foldRecords,
  timerId,
  type FoldedExecution,
} from "./fold.ts"
import { isFencingTokenMismatch, layerStore, S2WorkflowStoreTag } from "./s2.ts"

interface RegisteredWorkflow {
  readonly workflow: Workflow.Any
  readonly execute: (
    payload: object,
    executionId: string,
  ) => Effect.Effect<unknown, unknown, WorkflowEngine.WorkflowInstance | WorkflowEngine.WorkflowEngine>
  readonly scope: Scope.Scope
}

interface RuntimeExecution {
  readonly streamName: string
  readonly workflow: Workflow.Any
  readonly payload: object
  readonly parentExecutionId: string | undefined
  instance: WorkflowEngine.WorkflowInstance["Service"]
  token: string | undefined
  fiber: Fiber.Fiber<WorkflowResult> | undefined
}

const token = (): string => crypto.randomUUID()

const mapFromOption = <A>(value: A | undefined): Option.Option<A> =>
  Option.fromNullishOr(value)

const defect = (message: string): Effect.Effect<never> =>
  Effect.sync(() => {
    throw new Error(message)
  })

export const makeEncoded: Effect.Effect<
  WorkflowEngine.Encoded,
  never,
  S2WorkflowStoreTag | Scope.Scope
> = Effect.gen(function*() {
  const store = yield* S2WorkflowStoreTag
  const scope = yield* Effect.scope
  const workflows = new Map<string, RegisteredWorkflow>()
  const executions = new Map<string, RuntimeExecution>()
  const timers = yield* FiberMap.make<string>()

  const streamNameFor = (workflowName: string, executionId: string): string =>
    executionStreamName({
      workflowName,
      executionId,
      ...(store.config.streamPrefix === undefined
        ? {}
        : { streamPrefix: store.config.streamPrefix }),
    })

  const readFold = (streamName: string): Effect.Effect<FoldedExecution> =>
    Effect.map(store.readAll(streamName), foldRecords)

  const appendOwner = (
    state: RuntimeExecution,
    records: ReadonlyArray<WorkflowRecord>,
  ): Effect.Effect<void> =>
    store.append(
      state.streamName,
      records,
      state.token === undefined ? undefined : { fencingToken: state.token },
    )

  const engineRef: { current: WorkflowEngine.WorkflowEngine["Service"] | undefined } = {
    current: undefined,
  }

  const currentEngine = (): WorkflowEngine.WorkflowEngine["Service"] => {
    if (engineRef.current === undefined) {
      throw new Error("WorkflowEngine was used before initialization")
    }
    return engineRef.current
  }

  const scheduleTimers = (
    streamName: string,
    folded: FoldedExecution,
  ): Effect.Effect<void> =>
    Effect.forEach(folded.timers.values(), (timer) =>
      Effect.gen(function*() {
        if (folded.deferreds.has(deferredId(timer.executionId, timer.deferredName))) {
          return
        }
        const now = yield* Clock.currentTimeMillis
        const delay = Math.max(0, timer.dueAt - now)
        const completeTimer = Effect.gen(function*() {
          const completedAt = yield* Clock.currentTimeMillis
          yield* store.append(streamName, [
            deferredCompleted(
              deferredId(timer.executionId, timer.deferredName),
              Exit.void,
              completedAt,
            ),
          ])
          yield* resumeExecution(timer.executionId)
        })
        yield* completeTimer.pipe(
          Effect.delay(Duration.millis(delay)),
          FiberMap.run(timers, `${streamName}/${timer.timerId}`, { onlyIfMissing: true }),
          Effect.asVoid,
        )
      }), { discard: true })

  const acquire = (state: RuntimeExecution): Effect.Effect<void> =>
    Effect.gen(function*() {
      while (state.token === undefined) {
        const next = token()
        const result = yield* store.appendFence(state.streamName, next)
        if (result === "acquired") {
          state.token = next
          return
        }
        yield* Effect.yieldNow
      }
    })

  const ensureExecution = (
    workflow: Workflow.Any,
    options: {
      readonly executionId: string
      readonly payload: object
      readonly parent?: WorkflowEngine.WorkflowInstance["Service"] | undefined
    },
  ): Effect.Effect<RuntimeExecution> =>
    Effect.gen(function*() {
      const streamName = streamNameFor(workflow._tag, options.executionId)
      let folded = yield* readFold(streamName)

      if (folded.started === undefined) {
        const createdAt = yield* Clock.currentTimeMillis
        yield* store.append(streamName, [
          executionStarted({
            workflowName: workflow._tag,
            executionId: options.executionId,
            payload: options.payload,
            parentExecutionId: options.parent?.executionId,
            createdAt,
          }),
        ]).pipe(Effect.ignore)
        folded = yield* readFold(streamName)
      }

      const started = folded.started
      if (started === undefined) {
        return yield* defect(`Execution ${options.executionId} was not persisted`)
      }

      const existing = executions.get(options.executionId)
      if (existing !== undefined) {
        existing.instance.interrupted = folded.interrupted
        yield* scheduleTimers(streamName, folded)
        return existing
      }

      const state: RuntimeExecution = {
        streamName,
        workflow,
        payload: started.payload,
        parentExecutionId: started.parentExecutionId,
        instance: WorkflowEngine.WorkflowInstance.initial(workflow, options.executionId),
        token: undefined,
        fiber: undefined,
      }
      state.instance.interrupted = folded.interrupted
      executions.set(options.executionId, state)
      yield* scheduleTimers(streamName, folded)
      return state
    })

  const restoreExecution = (
    workflow: Workflow.Any,
    executionId: string,
  ): Effect.Effect<RuntimeExecution | undefined> =>
    Effect.gen(function*() {
      const existing = executions.get(executionId)
      if (existing !== undefined) return existing

      const streamName = streamNameFor(workflow._tag, executionId)
      const folded = yield* readFold(streamName)
      const started = folded.started
      if (started === undefined || started.workflowName !== workflow._tag) {
        return undefined
      }

      const state: RuntimeExecution = {
        streamName,
        workflow,
        payload: started.payload,
        parentExecutionId: started.parentExecutionId,
        instance: WorkflowEngine.WorkflowInstance.initial(workflow, executionId),
        token: undefined,
        fiber: undefined,
      }
      state.instance.interrupted = folded.interrupted
      executions.set(executionId, state)
      yield* scheduleTimers(streamName, folded)
      return state
    })

  const resumeExecution = (
    executionId: string,
    workflow?: Workflow.Any,
  ): Effect.Effect<void> =>
    Effect.gen(function*() {
      const state = executions.get(executionId)
        ?? (workflow === undefined ? undefined : yield* restoreExecution(workflow, executionId))
      if (state === undefined) return

      const current = state.fiber?.pollUnsafe()
      if (current !== undefined && current._tag === "Success" && current.value._tag === "Complete") {
        return
      }
      if (state.fiber !== undefined && current === undefined) {
        return
      }

      const folded = yield* readFold(state.streamName)
      if (folded.completed !== undefined) return
      yield* acquire(state)

      const registered = workflows.get(state.workflow._tag)
      if (registered === undefined) return

      const instance = WorkflowEngine.WorkflowInstance.initial(
        state.workflow,
        state.instance.executionId,
      )
      instance.interrupted = folded.interrupted || state.instance.interrupted
      state.instance = instance

      state.fiber = yield* registered.execute(state.payload, state.instance.executionId).pipe(
        Effect.onExit(() => {
          if (!instance.interrupted) return Effect.void
          instance.suspended = false
          return Effect.withFiber((fiber) => Effect.interruptible(Fiber.interrupt(fiber)))
        }),
        Workflow.intoResult,
        Effect.tap((result) =>
          result._tag === "Complete"
            ? Effect.gen(function*() {
              const completedAt = yield* Clock.currentTimeMillis
              yield* appendOwner(state, [
                executionCompleted(executionId, result, completedAt),
              ])
            }).pipe(Effect.catchIf(isFencingTokenMismatch, () => Effect.void))
            : Effect.void,
        ),
        Effect.provideService(WorkflowEngine.WorkflowInstance, instance),
        Effect.provideService(WorkflowEngine.WorkflowEngine, currentEngine()),
        Effect.tap((result) =>
          state.parentExecutionId === undefined || result._tag !== "Complete"
            ? Effect.void
            : Effect.forkIn(resumeExecution(state.parentExecutionId), scope),
        ),
        Effect.forkIn(registered.scope),
      )
    })

  const persistInterrupt = (
    workflow: Workflow.Any,
    executionId: string,
    unsafe: boolean,
  ): Effect.Effect<RuntimeExecution | undefined> =>
    Effect.gen(function*() {
      const state = executions.get(executionId)
      const requestedAt = yield* Clock.currentTimeMillis
      yield* store.append(
        state?.streamName ?? streamNameFor(workflow._tag, executionId),
        [interruptRequested(unsafe, requestedAt)],
      )
      if (state !== undefined) {
        state.instance.interrupted = true
      }
      return state
    })

  const encoded: WorkflowEngine.Encoded = {
    register: Effect.fnUntraced(function*(workflow, execute) {
      workflows.set(workflow._tag, {
        workflow,
        execute,
        scope: yield* Effect.scope,
      })
    }),

    execute: Effect.fnUntraced(function*(workflow, options) {
      const registered = workflows.get(workflow._tag)
      if (registered === undefined) {
        return yield* defect(`Workflow ${workflow._tag} is not registered`)
      }

      const state = yield* ensureExecution(workflow, options)
      const folded = yield* readFold(state.streamName)
      if (folded.completed !== undefined) {
        return folded.completed
      }

      yield* resumeExecution(options.executionId)
      if (options.discard) return
      const fiber = state.fiber
      if (fiber === undefined) {
        return yield* defect(`Workflow ${workflow._tag}/${options.executionId} did not start`)
      }
      return yield* Fiber.join(fiber)
    }) as WorkflowEngine.Encoded["execute"],

    poll: (workflow, executionId) =>
      Effect.gen(function*() {
        const folded = yield* readFold(streamNameFor(workflow._tag, executionId))
        return completedResultOption(folded)
      }),

    interrupt: (workflow, executionId) =>
      Effect.gen(function*() {
        yield* persistInterrupt(workflow, executionId, false)
        yield* resumeExecution(executionId, workflow)
      }),

    interruptUnsafe: (workflow, executionId) =>
      Effect.gen(function*() {
        const state = yield* persistInterrupt(workflow, executionId, true)
        if (state === undefined) return
        const fiber = state.fiber
        if (fiber !== undefined) {
          yield* Fiber.interrupt(fiber)
        }
      }),

    resume: (workflow, executionId) => resumeExecution(executionId, workflow),

    activityExecute: Effect.fnUntraced(function*(activity, attempt) {
      const instance = yield* WorkflowEngine.WorkflowInstance
      const state = executions.get(instance.executionId)
      if (state === undefined) {
        return yield* defect(`Execution ${instance.executionId} is not active`)
      }
      const id = activityId(instance.executionId, activity.name, attempt)
      const folded = yield* readFold(state.streamName)
      const existing = folded.activities.get(id)
      if (existing !== undefined && existing._tag !== "Suspended") {
        return existing
      }

      const activityInstance = WorkflowEngine.WorkflowInstance.initial(
        instance.workflow,
        instance.executionId,
      )
      activityInstance.interrupted = instance.interrupted
      const result = yield* activity.executeEncoded.pipe(
        Workflow.intoResult,
        Effect.provideService(WorkflowEngine.WorkflowInstance, activityInstance),
      )
      if (result._tag === "Complete") {
        const completedAt = yield* Clock.currentTimeMillis
        yield* appendOwner(state, [activityCompleted(id, result, completedAt)]).pipe(
          Effect.catchIf(isFencingTokenMismatch, () => Effect.void),
        )
      }
      return result
    }),

    deferredResult: Effect.fnUntraced(function*(deferred) {
      const instance = yield* WorkflowEngine.WorkflowInstance
      const state = executions.get(instance.executionId)
      if (state === undefined) return Option.none()
      const folded = yield* readFold(state.streamName)
      return mapFromOption(folded.deferreds.get(deferredId(instance.executionId, deferred.name)))
    }),

    deferredDone: (options) =>
      Effect.gen(function*() {
        const streamName = streamNameFor(options.workflowName, options.executionId)
        const folded = yield* readFold(streamName)
        const id = deferredId(options.executionId, options.deferredName)
        if (!folded.deferreds.has(id)) {
          const completedAt = yield* Clock.currentTimeMillis
          yield* store.append(streamName, [
            deferredCompleted(id, options.exit, completedAt),
          ])
        }
        const state = executions.get(options.executionId)
        if (state !== undefined) {
          yield* resumeExecution(options.executionId)
        }
      }),

    scheduleClock: (workflow, options) =>
      Effect.gen(function*() {
        const streamName = streamNameFor(workflow._tag, options.executionId)
        const id = timerId(options.executionId, options.clock.name)
        const scheduledAt = yield* Clock.currentTimeMillis
        const dueAt = scheduledAt + Duration.toMillis(options.clock.duration)
        const folded = yield* readFold(streamName)
        if (!folded.timers.has(id)) {
          yield* store.append(streamName, [
            timerScheduled({
              timerId: id,
              executionId: options.executionId,
              deferredName: options.clock.deferred.name,
              dueAt,
              scheduledAt,
            }),
          ])
        }
        yield* scheduleTimers(streamName, yield* readFold(streamName))
      }),
  }

  engineRef.current = WorkflowEngine.makeUnsafe(encoded)

  yield* Effect.forEach(
    yield* store.listExecutionStreams(),
    (streamName) => readFold(streamName).pipe(
      Effect.flatMap((folded) => scheduleTimers(streamName, folded)),
    ),
    { discard: true },
  )

  return encoded
})

export const make: Effect.Effect<
  WorkflowEngine.WorkflowEngine["Service"],
  never,
  S2WorkflowStoreTag | Scope.Scope
> = Effect.map(makeEncoded, WorkflowEngine.makeUnsafe)

export const layer: Layer.Layer<
  WorkflowEngine.WorkflowEngine,
  never,
  S2WorkflowStoreTag
> = Layer.effect(WorkflowEngine.WorkflowEngine, make)

export const layerFromConfig: Layer.Layer<
  WorkflowEngine.WorkflowEngine,
  never,
  S2WorkflowEngineConfigTag
> = layer.pipe(Layer.provide(layerStore))
