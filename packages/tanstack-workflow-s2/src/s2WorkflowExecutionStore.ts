/* oxlint-disable effect/restricted-syntax -- This adapter implements TanStack Workflow's Promise-based store boundary. */
import {
  AppendInput,
  AppendRecord,
  basin,
  basins,
  layer as S2Layer,
  type S2Client,
  SeqNumMismatchError,
  stream as s2Stream,
  type StreamApi
} from "effect-s2"
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"

import {
  type AppendEventsArgs,
  type AppendEventsResult,
  type ApprovalResult,
  type ClaimDueScheduleBucketsArgs,
  type ClaimDueTimersArgs,
  type ClaimRunArgs,
  type ClaimRunResult,
  type ClaimStaleRunsArgs,
  type CreateRunArgs,
  type CreateRunResult,
  type DeleteReason,
  type DeliverApprovalArgs,
  type DeliverApprovalResult,
  type DeliverSignalArgs,
  type DeliverSignalResult,
  type HeartbeatRunLeaseArgs,
  type LeaseOwner,
  type ListRunsArgs,
  type LoadedExecution,
  LogConflictError,
  type MarkScheduleBucketStartedArgs,
  type ReadEventsArgs,
  type ReleaseRunLeaseArgs,
  type RunClaim,
  type RunId,
  type RunState,
  type RunSummary,
  type RunTimeline,
  type ScheduleBucket,
  type ScheduleBucketId,
  type ScheduleId,
  type ScheduleTimerArgs,
  type SerializedError,
  type SignalDelivery,
  type StoredWorkflowEvent,
  type TimerWakeup,
  type UpsertScheduleArgs,
  type WorkflowEvent,
  type WorkflowExecution,
  type WorkflowExecutionStore,
  type WorkflowLease,
  type WorkflowOverlapPolicy,
  type WorkflowScheduleSpec
} from "./types.ts"

export interface S2WorkflowExecutionStoreConfig {
  readonly s2Endpoint: string
  readonly basin?: string
  readonly namespace?: string
  readonly accessToken?: string
}

interface S2StoreRuntime {
  readonly basinName: string
  readonly namespace: string
  readonly layer: ReturnType<typeof S2Layer>
}

interface EventEnvelope {
  readonly eventIndex: number
  readonly event: WorkflowEvent
}

type RunMetaFact =
  | { readonly _tag: "RunCreated"; readonly run: WorkflowExecution }
  | { readonly _tag: "RunStateSaved"; readonly state: RunState }
  | {
    readonly _tag: "RunPaused"
    readonly awaiting?: RunState["awaiting"]
    readonly waitingFor?: RunState["waitingFor"]
    readonly pendingApproval?: RunState["pendingApproval"]
    readonly wakeAt?: number
    readonly now: number
  }
  | { readonly _tag: "RunFinished"; readonly output: unknown; readonly now: number }
  | { readonly _tag: "RunErrored"; readonly error: SerializedError; readonly now: number }
  | { readonly _tag: "RunDeleted"; readonly reason: DeleteReason; readonly now: number }
  | { readonly _tag: "LeaseClaimed"; readonly lease: WorkflowLease; readonly now: number }
  | { readonly _tag: "LeaseReleased"; readonly leaseOwner: LeaseOwner }
  | { readonly _tag: "TimerScheduled"; readonly wakeAt: number; readonly now: number }
  | { readonly _tag: "SignalDelivered"; readonly delivery: SignalDelivery; readonly now: number }
  | { readonly _tag: "ApprovalDelivered"; readonly approval: ApprovalResult; readonly now: number }

interface RunMetaProjection {
  readonly run?: WorkflowExecution
  readonly state?: RunState
  readonly deleted: boolean
  readonly signalDeliveries: ReadonlySet<string>
  readonly nextSeqNum: number
}

type RunIndexFact = { readonly _tag: "RunIndexed"; readonly runId: RunId }

type TimerFact =
  | { readonly _tag: "TimerScheduled"; readonly timer: TimerWakeup }
  | { readonly _tag: "TimerClaimed"; readonly key: string; readonly lease: WorkflowLease }
  | { readonly _tag: "TimerConsumed"; readonly key: string }

interface TimerProjection {
  readonly timers: ReadonlyMap<string, TimerWakeup & { readonly lease?: WorkflowLease }>
  readonly nextSeqNum: number
}

type ScheduleFact =
  | {
    readonly _tag: "ScheduleUpserted"
    readonly scheduleId: ScheduleId
    readonly workflowId: string
    readonly workflowVersion?: string
    readonly schedule: WorkflowScheduleSpec
    readonly overlapPolicy: WorkflowOverlapPolicy
    readonly input?: unknown
    readonly nextFireAt?: number
    readonly enabled: boolean
  }
  | {
    readonly _tag: "ScheduleBucketClaimed"
    readonly scheduleId: ScheduleId
    readonly bucketId: ScheduleBucketId
    readonly runId: RunId
    readonly lease: WorkflowLease
    readonly fireAt: number
  }
  | {
    readonly _tag: "ScheduleBucketStarted"
    readonly scheduleId: ScheduleId
    readonly bucketId: ScheduleBucketId
    readonly runId: RunId
    readonly now: number
  }

interface ScheduleRecord {
  readonly scheduleId: ScheduleId
  readonly workflowId: string
  readonly workflowVersion?: string
  readonly schedule: WorkflowScheduleSpec
  readonly overlapPolicy: WorkflowOverlapPolicy
  readonly input?: unknown
  readonly nextFireAt?: number
  readonly enabled: boolean
}

interface ScheduleBucketRecord extends ScheduleBucket {
  readonly status: "claimed" | "started"
  readonly lease?: WorkflowLease
}

interface ScheduleProjection {
  readonly schedules: ReadonlyMap<ScheduleId, ScheduleRecord>
  readonly buckets: ReadonlyMap<string, ScheduleBucketRecord>
  readonly nextSeqNum: number
}

export const s2WorkflowExecutionStore = (config: S2WorkflowExecutionStoreConfig): WorkflowExecutionStore => {
  const runtime = makeRuntime(config)

  const readRun = (runId: RunId) => run(readRunMeta(runtime, runId))

  return {
    appendEvents: (args) => run(appendEvents(runtime, args)),
    readEvents: (args) => run(readEvents(runtime, args)),
    loadRunState: async (runId) => (await readRun(runId)).state,
    saveRunState: (args) =>
      run(
        updateRunMeta(runtime, args.state.runId, (projection) => ({
          fact: { _tag: "RunStateSaved", state: cloneRunState(args.state) },
          matchSeqNum: projection.nextSeqNum
        }))
      ),
    deleteRun: (runId, reason) =>
      run(
        appendFact(
          runtime,
          streamNames(runtime).runMeta(runId),
          { _tag: "RunDeleted", now: Date.now(), reason } satisfies RunMetaFact
        ).pipe(
          Effect.asVoid
        )
      ),
    createRun: (args) => run(createRun(runtime, args)),
    loadRun: async (runId) => (await readRun(runId)).run,
    loadExecution: (runId) => run(loadExecution(runtime, runId)),
    claimRun: (args) => run(claimRun(runtime, args)),
    heartbeatRunLease: (args) => run(heartbeatRunLease(runtime, args)),
    releaseRunLease: (args) => run(releaseRunLease(runtime, args)),
    markRunPaused: (args) =>
      run(
        updateRunMeta(runtime, args.runId, (projection) =>
          projection.run === undefined
            ? undefined
            : {
              fact: {
                _tag: "RunPaused",
                ...(args.awaiting === undefined ? {} : { awaiting: clone(args.awaiting) }),
                ...(args.waitingFor === undefined ? {} : { waitingFor: clone(args.waitingFor) }),
                ...(args.pendingApproval === undefined ? {} : { pendingApproval: clone(args.pendingApproval) }),
                ...(args.wakeAt === undefined ? {} : { wakeAt: args.wakeAt }),
                now: args.now
              },
              matchSeqNum: projection.nextSeqNum
            })
      ),
    markRunFinished: (args) =>
      run(
        updateRunMeta(runtime, args.runId, (projection) =>
          projection.run === undefined
            ? undefined
            : {
              fact: { _tag: "RunFinished", now: args.now, output: clone(args.output) },
              matchSeqNum: projection.nextSeqNum
            })
      ),
    markRunErrored: (args) =>
      run(
        updateRunMeta(runtime, args.runId, (projection) =>
          projection.run === undefined
            ? undefined
            : {
              fact: { _tag: "RunErrored", error: clone(args.error), now: args.now },
              matchSeqNum: projection.nextSeqNum
            })
      ),
    scheduleTimer: (args) => run(scheduleTimer(runtime, args)),
    claimDueTimers: (args) => run(claimDueTimers(runtime, args)),
    deliverSignal: (args) => run(deliverSignal(runtime, args)),
    deliverApproval: (args) => run(deliverApproval(runtime, args)),
    upsertSchedule: (args) => run(upsertSchedule(runtime, args)),
    claimDueScheduleBuckets: (args) => run(claimDueScheduleBuckets(runtime, args)),
    markScheduleBucketStarted: (args) => run(markScheduleBucketStarted(runtime, args)),
    claimStaleRuns: (args) => run(claimStaleRuns(runtime, args)),
    listRuns: (args) => run(listRuns(runtime, args)),
    getRunTimeline: (runId) => run(getRunTimeline(runtime, runId))
  }
}

const makeRuntime = (config: S2WorkflowExecutionStoreConfig): S2StoreRuntime => ({
  basinName: config.basin ?? "tanstack-workflow",
  namespace: sanitize(config.namespace ?? "default"),
  layer: S2Layer({
    accessToken: config.accessToken ?? "s2_access_token",
    endpoints: {
      account: config.s2Endpoint,
      basin: config.s2Endpoint
    },
    retry: { maxAttempts: 1 }
  })
})

const run = <A>(effect: Effect.Effect<A, unknown>): Promise<A> => Effect.runPromise(effect)

const streamNames = (runtime: S2StoreRuntime) => ({
  events: (runId: RunId) => `${runtime.namespace}.runs.${sanitize(runId)}.events`,
  runMeta: (runId: RunId) => `${runtime.namespace}.runs.${sanitize(runId)}.meta`,
  runIndex: `${runtime.namespace}.runs.index`,
  timers: `${runtime.namespace}.timers`,
  schedules: `${runtime.namespace}.schedules`
})

const withS2 = <A, E>(runtime: S2StoreRuntime, effect: Effect.Effect<A, E, S2Client>): Effect.Effect<A, E> =>
  effect.pipe(Effect.provide(runtime.layer))

const getStream = (runtime: S2StoreRuntime, name: string): Effect.Effect<StreamApi, unknown> =>
  withS2(
    runtime,
    Effect.gen(function*() {
      yield* basins.ensure({ basin: runtime.basinName })
      const basinApi = yield* basin(runtime.basinName)
      yield* basinApi.streams.ensure({ stream: name })
      return yield* s2Stream(runtime.basinName, name)
    })
  )

const appendFact = <A>(
  runtime: S2StoreRuntime,
  streamName: string,
  fact: A,
  matchSeqNum?: number
) =>
  Effect.gen(function*() {
    const stream = yield* getStream(runtime, streamName)
    const ack = yield* stream.append(
      AppendInput.create([jsonRecord(fact)], matchSeqNum === undefined ? undefined : { matchSeqNum })
    )
    return ack
  })

const readJsonRecords = <A>(
  runtime: S2StoreRuntime,
  streamName: string,
  fromSeqNum = 0
): Effect.Effect<{ readonly nextSeqNum: number; readonly records: ReadonlyArray<A> }, unknown> =>
  Effect.gen(function*() {
    const stream = yield* getStream(runtime, streamName)
    const tail = yield* stream.checkTail()
    if (tail.tail.seqNum <= fromSeqNum) {
      return { nextSeqNum: tail.tail.seqNum, records: [] }
    }
    const records = yield* stream.readSession({
      start: { from: { seqNum: fromSeqNum } },
      stop: { limits: { count: tail.tail.seqNum - fromSeqNum } }
    }).pipe(
      Stream.runCollect,
      Effect.map((chunk) => Array.from(chunk, (record) => JSON.parse(record.body) as A))
    )
    return { nextSeqNum: tail.tail.seqNum, records }
  })

const appendEvents = (runtime: S2StoreRuntime, args: AppendEventsArgs): Effect.Effect<AppendEventsResult, unknown> =>
  Effect.gen(function*() {
    const streamName = streamNames(runtime).events(args.runId)
    const stream = yield* getStream(runtime, streamName)
    const envelopes = args.events.map((event, index) =>
      ({
        event: clone(event),
        eventIndex: args.expectedNextIndex + index
      }) satisfies EventEnvelope
    )
    const records = envelopes.map((envelope) =>
      jsonRecord(envelope, [
        ["tanstack.workflow.run_id", args.runId],
        ["tanstack.workflow.event_index", String(envelope.eventIndex)],
        ["tanstack.workflow.event_type", envelope.event.type],
        ["tanstack.workflow.step_id", eventStepId(envelope.event) ?? ""]
      ])
    )
    const ack = yield* stream.append(AppendInput.create(records, { matchSeqNum: args.expectedNextIndex })).pipe(
      Effect.catch((error) =>
        error instanceof SeqNumMismatchError
          ? readEvents(runtime, { fromIndex: args.expectedNextIndex, runId: args.runId }).pipe(
            Effect.flatMap((events) =>
              Effect.fail(new LogConflictError(args.runId, args.expectedNextIndex, events[0]?.event))
            )
          )
          : Effect.fail(error)
      )
    )
    return { nextIndex: ack.end.seqNum }
  })

const readEvents = (
  runtime: S2StoreRuntime,
  args: ReadEventsArgs
): Effect.Effect<ReadonlyArray<StoredWorkflowEvent>, unknown> =>
  readJsonRecords<EventEnvelope>(runtime, streamNames(runtime).events(args.runId), args.fromIndex ?? 0).pipe(
    Effect.map((result) => result.records.map((envelope) => storedWorkflowEvent(args.runId, envelope)))
  )

const storedWorkflowEvent = (runId: RunId, envelope: EventEnvelope): StoredWorkflowEvent => {
  const stepId = eventStepId(envelope.event)
  return {
    createdAt: envelope.event.ts,
    event: envelope.event,
    eventIndex: envelope.eventIndex,
    eventType: envelope.event.type,
    runId,
    ...(stepId === undefined ? {} : { stepId })
  }
}

const eventStepId = (event: WorkflowEvent): string | undefined => "stepId" in event ? event.stepId : undefined

const createRun = (runtime: S2StoreRuntime, args: CreateRunArgs): Effect.Effect<CreateRunResult, unknown> =>
  Effect.gen(function*() {
    const runValue = {
      createdAt: args.now,
      input: clone(args.input),
      runId: args.runId,
      status: "queued",
      updatedAt: args.now,
      workflowId: args.workflowId,
      ...(args.workflowVersion === undefined ? {} : { workflowVersion: args.workflowVersion })
    } satisfies WorkflowExecution
    const created = yield* appendFact(
      runtime,
      streamNames(runtime).runMeta(args.runId),
      { _tag: "RunCreated", run: runValue } satisfies RunMetaFact,
      0
    ).pipe(
      Effect.as(true),
      Effect.catch((error) => error instanceof SeqNumMismatchError ? Effect.succeed(false) : Effect.fail(error))
    )
    if (!created) {
      const existing = yield* readRunMeta(runtime, args.runId)
      return existing.run === undefined
        ? { kind: "created" as const, run: runValue }
        : { kind: "existing" as const, run: existing.run }
    }
    yield* appendFact(
      runtime,
      streamNames(runtime).runIndex,
      { _tag: "RunIndexed", runId: args.runId } satisfies RunIndexFact
    )
    return { kind: "created" as const, run: runValue }
  })

const loadExecution = (runtime: S2StoreRuntime, runId: RunId): Effect.Effect<LoadedExecution | undefined, unknown> =>
  Effect.gen(function*() {
    const projection = yield* readRunMeta(runtime, runId)
    if (projection.run === undefined) return undefined
    const events = yield* readEvents(runtime, { runId })
    return { events, run: projection.run }
  })

const updateRunMeta = (
  runtime: S2StoreRuntime,
  runId: RunId,
  make: (projection: RunMetaProjection) =>
    | { readonly fact: RunMetaFact; readonly matchSeqNum?: number }
    | undefined,
  retries = 5
): Effect.Effect<void, unknown> =>
  Effect.gen(function*() {
    const projection = yield* readRunMeta(runtime, runId)
    const update = make(projection)
    if (update === undefined) return
    const result = yield* appendFact(runtime, streamNames(runtime).runMeta(runId), update.fact, update.matchSeqNum)
      .pipe(
        Effect.exit
      )
    if (result._tag === "Success") return
    if (isSeqNumMismatch(result.cause) && retries > 0) {
      return yield* updateRunMeta(runtime, runId, make, retries - 1)
    }
    return yield* Effect.failCause(result.cause)
  })

const claimRun = (runtime: S2StoreRuntime, args: ClaimRunArgs): Effect.Effect<ClaimRunResult, unknown> =>
  Effect.gen(function*() {
    let claimedRun: WorkflowExecution | undefined
    let notClaimable: WorkflowExecution | undefined
    yield* updateRunMeta(runtime, args.runId, (projection) => {
      const current = projection.run
      if (current === undefined) return undefined
      if (isTerminal(current.status) || !canClaim(current.lease, args.leaseOwner, args.now)) {
        notClaimable = current
        return undefined
      }
      const leaseValue = lease(args.leaseOwner, args.leaseMs, args.now)
      claimedRun = { ...current, lease: leaseValue, status: "running", updatedAt: args.now }
      return {
        fact: { _tag: "LeaseClaimed", lease: leaseValue, now: args.now },
        matchSeqNum: projection.nextSeqNum
      }
    })
    if (claimedRun !== undefined) return { kind: "claimed" as const, run: claimedRun }
    if (notClaimable !== undefined) return { kind: "not-claimable" as const, run: notClaimable }
    return { kind: "not-found" as const }
  })

const heartbeatRunLease = (runtime: S2StoreRuntime, args: HeartbeatRunLeaseArgs): Effect.Effect<void, unknown> =>
  updateRunMeta(runtime, args.runId, (projection) => {
    if (projection.run?.lease?.owner !== args.leaseOwner) return undefined
    return {
      fact: { _tag: "LeaseClaimed", lease: lease(args.leaseOwner, args.leaseMs, args.now), now: args.now },
      matchSeqNum: projection.nextSeqNum
    }
  })

const releaseRunLease = (runtime: S2StoreRuntime, args: ReleaseRunLeaseArgs): Effect.Effect<void, unknown> =>
  updateRunMeta(runtime, args.runId, (projection) => {
    if (projection.run?.lease?.owner !== args.leaseOwner) return undefined
    return {
      fact: { _tag: "LeaseReleased", leaseOwner: args.leaseOwner },
      matchSeqNum: projection.nextSeqNum
    }
  })

const scheduleTimer = (runtime: S2StoreRuntime, args: ScheduleTimerArgs): Effect.Effect<void, unknown> =>
  Effect.gen(function*() {
    const timer = {
      runId: args.runId,
      signalId: args.signalId,
      ...(args.signalName === undefined ? {} : { signalName: args.signalName }),
      wakeAt: args.wakeAt,
      workflowId: args.workflowId,
      ...(args.workflowVersion === undefined ? {} : { workflowVersion: args.workflowVersion })
    } satisfies TimerWakeup
    yield* appendFact(
      runtime,
      streamNames(runtime).timers,
      {
        _tag: "TimerScheduled",
        timer
      } satisfies TimerFact
    )
    yield* updateRunMeta(runtime, args.runId, (projection) =>
      projection.run === undefined
        ? undefined
        : {
          fact: { _tag: "TimerScheduled", now: args.now, wakeAt: args.wakeAt },
          matchSeqNum: projection.nextSeqNum
        })
  })

const claimDueTimers = (
  runtime: S2StoreRuntime,
  args: ClaimDueTimersArgs
): Effect.Effect<ReadonlyArray<TimerWakeup>, unknown> =>
  Effect.gen(function*() {
    const projection = yield* readTimers(runtime)
    const due: Array<TimerWakeup> = []
    let nextSeqNum = projection.nextSeqNum
    for (const [key, timer] of projection.timers) {
      if (due.length >= args.limit) break
      if (timer.wakeAt > args.now || !canClaim(timer.lease, args.leaseOwner, args.now)) continue
      const runProjection = yield* readRunMeta(runtime, timer.runId)
      const delivery = {
        name: timer.signalName ?? "__timer",
        payload: undefined,
        signalId: timer.signalId
      } satisfies SignalDelivery
      if (
        runProjection.run === undefined
        || isTerminal(runProjection.run.status)
        || runProjection.signalDeliveries.has(signalKey(timer.runId, timer.signalId))
        || !isRunWaitingForSignal(runProjection.run, delivery)
      ) {
        const consumed = yield* consumeTimer(runtime, key, nextSeqNum).pipe(Effect.exit)
        if (consumed._tag === "Success") nextSeqNum = consumed.value.end.seqNum
        continue
      }
      const leaseValue = lease(args.leaseOwner, args.leaseMs, args.now)
      const result = yield* appendFact(
        runtime,
        streamNames(runtime).timers,
        {
          _tag: "TimerClaimed",
          key,
          lease: leaseValue
        } satisfies TimerFact,
        nextSeqNum
      ).pipe(Effect.exit)
      if (result._tag === "Success") {
        nextSeqNum = result.value.end.seqNum
        due.push(timerWakeup(timer))
      }
    }
    return due
  })

const consumeTimer = (
  runtime: S2StoreRuntime,
  key: string,
  matchSeqNum?: number
) =>
  appendFact(
    runtime,
    streamNames(runtime).timers,
    {
      _tag: "TimerConsumed",
      key
    } satisfies TimerFact,
    matchSeqNum
  )

const deliverSignal = <TPayload>(
  runtime: S2StoreRuntime,
  args: DeliverSignalArgs<TPayload>
): Effect.Effect<DeliverSignalResult, unknown> =>
  Effect.gen(function*() {
    const projection = yield* readRunMeta(runtime, args.runId)
    const current = projection.run
    if (current === undefined) return { kind: "not-found" as const }
    const key = signalKey(args.runId, args.delivery.signalId)
    if (projection.signalDeliveries.has(key)) return { kind: "duplicate" as const, run: current }
    if (!isRunWaitingForSignal(current, args.delivery)) return { kind: "not-waiting" as const, run: current }
    const updated = queuedAfterDelivery(current, args.now)
    yield* appendFact(
      runtime,
      streamNames(runtime).runMeta(args.runId),
      {
        _tag: "SignalDelivered",
        delivery: args.delivery,
        now: args.now
      } satisfies RunMetaFact,
      projection.nextSeqNum
    )
    if (args.delivery.name === "__timer") {
      yield* consumeTimer(runtime, key)
    }
    return { kind: "delivered" as const, run: updated }
  })

const deliverApproval = (
  runtime: S2StoreRuntime,
  args: DeliverApprovalArgs
): Effect.Effect<DeliverApprovalResult, unknown> =>
  Effect.gen(function*() {
    const projection = yield* readRunMeta(runtime, args.runId)
    const current = projection.run
    if (current === undefined) return { kind: "not-found" as const }
    const key = signalKey(args.runId, `approval:${args.approval.approvalId}`)
    if (projection.signalDeliveries.has(key)) return { kind: "duplicate" as const, run: current }
    if (!isRunWaitingForApproval(current, args.approval)) return { kind: "not-waiting" as const, run: current }
    const updated = queuedAfterDelivery(current, args.now)
    yield* appendFact(
      runtime,
      streamNames(runtime).runMeta(args.runId),
      {
        _tag: "ApprovalDelivered",
        approval: args.approval,
        now: args.now
      } satisfies RunMetaFact,
      projection.nextSeqNum
    )
    return { kind: "delivered" as const, run: updated }
  })

const upsertSchedule = (runtime: S2StoreRuntime, args: UpsertScheduleArgs): Effect.Effect<void, unknown> =>
  appendFact(
    runtime,
    streamNames(runtime).schedules,
    {
      _tag: "ScheduleUpserted",
      enabled: args.enabled,
      overlapPolicy: args.overlapPolicy,
      schedule: clone(args.schedule),
      scheduleId: args.scheduleId,
      workflowId: args.workflowId,
      ...(args.input === undefined ? {} : { input: clone(args.input) }),
      ...(args.nextFireAt === undefined ? {} : { nextFireAt: args.nextFireAt }),
      ...(args.workflowVersion === undefined ? {} : { workflowVersion: args.workflowVersion })
    } satisfies ScheduleFact
  ).pipe(Effect.asVoid)

const claimDueScheduleBuckets = (
  runtime: S2StoreRuntime,
  args: ClaimDueScheduleBucketsArgs
): Effect.Effect<ReadonlyArray<ScheduleBucket>, unknown> =>
  Effect.gen(function*() {
    const projection = yield* readSchedules(runtime)
    const due: Array<ScheduleBucket> = []
    for (const schedule of projection.schedules.values()) {
      if (due.length >= args.limit) break
      if (!schedule.enabled || schedule.nextFireAt === undefined || schedule.nextFireAt > args.now) continue
      const bucketId = String(schedule.nextFireAt)
      const key = scheduleBucketKey(schedule.scheduleId, bucketId)
      const existing = projection.buckets.get(key)
      if (existing?.status === "started") continue
      if (existing !== undefined && !canClaim(existing.lease, args.leaseOwner, args.now)) continue
      const bucket = scheduleBucket(
        schedule,
        bucketId,
        schedule.nextFireAt,
        lease(args.leaseOwner, args.leaseMs, args.now)
      )
      const result = yield* appendFact(
        runtime,
        streamNames(runtime).schedules,
        {
          _tag: "ScheduleBucketClaimed",
          bucketId,
          fireAt: schedule.nextFireAt,
          lease: bucket.lease!,
          runId: bucket.runId,
          scheduleId: schedule.scheduleId
        } satisfies ScheduleFact,
        projection.nextSeqNum
      ).pipe(Effect.exit)
      if (result._tag === "Success") due.push(scheduleBucketWakeup(bucket))
    }
    return due
  })

const markScheduleBucketStarted = (
  runtime: S2StoreRuntime,
  args: MarkScheduleBucketStartedArgs
): Effect.Effect<void, unknown> =>
  appendFact(
    runtime,
    streamNames(runtime).schedules,
    {
      _tag: "ScheduleBucketStarted",
      bucketId: args.bucketId,
      now: args.now,
      runId: args.runId,
      scheduleId: args.scheduleId
    } satisfies ScheduleFact
  ).pipe(Effect.asVoid)

const claimStaleRuns = (
  runtime: S2StoreRuntime,
  args: ClaimStaleRunsArgs
): Effect.Effect<ReadonlyArray<RunClaim>, unknown> =>
  Effect.gen(function*() {
    const runs = yield* listRuns(runtime, { limit: Number.MAX_SAFE_INTEGER, status: "running" })
    const claims: Array<RunClaim> = []
    for (const summary of runs) {
      if (claims.length >= args.limit) break
      const projection = yield* readRunMeta(runtime, summary.runId)
      const runValue = projection.run
      if (runValue?.status !== "running" || runValue.lease === undefined || runValue.lease.expiresAt > args.now) {
        continue
      }
      const claimed = yield* claimRun(runtime, {
        leaseMs: args.leaseMs,
        leaseOwner: args.leaseOwner,
        now: args.now,
        runId: summary.runId
      })
      if (claimed.kind === "claimed" && claimed.run.lease !== undefined) {
        claims.push({ lease: claimed.run.lease, run: claimed.run })
      }
    }
    return claims
  })

const listRuns = (runtime: S2StoreRuntime, args: ListRunsArgs): Effect.Effect<ReadonlyArray<RunSummary>, unknown> =>
  Effect.gen(function*() {
    const index = yield* readJsonRecords<RunIndexFact>(runtime, streamNames(runtime).runIndex)
    const seen = new Set<RunId>()
    const summaries: Array<RunSummary> = []
    for (const fact of index.records) {
      if (fact._tag !== "RunIndexed" || seen.has(fact.runId)) continue
      seen.add(fact.runId)
      const projection = yield* readRunMeta(runtime, fact.runId)
      const runValue = projection.run
      if (runValue === undefined) continue
      if (args.workflowId !== undefined && runValue.workflowId !== args.workflowId) continue
      if (args.status !== undefined && runValue.status !== args.status) continue
      summaries.push(toRunSummary(runValue))
    }
    const offset = args.cursor === undefined ? 0 : Number(args.cursor)
    const start = Number.isFinite(offset) && offset > 0 ? offset : 0
    return summaries.sort((a, b) => b.updatedAt - a.updatedAt).slice(start, start + args.limit)
  })

const getRunTimeline = (runtime: S2StoreRuntime, runId: RunId): Effect.Effect<RunTimeline | undefined, unknown> =>
  Effect.gen(function*() {
    const loaded = yield* loadExecution(runtime, runId)
    return loaded === undefined ? undefined : { events: loaded.events, run: loaded.run }
  })

const readRunMeta = (runtime: S2StoreRuntime, runId: RunId): Effect.Effect<RunMetaProjection, unknown> =>
  readJsonRecords<RunMetaFact>(runtime, streamNames(runtime).runMeta(runId)).pipe(
    Effect.map((result) => foldRunMeta(runId, result.records, result.nextSeqNum))
  )

const foldRunMeta = (
  runId: RunId,
  facts: ReadonlyArray<RunMetaFact>,
  nextSeqNum: number
): RunMetaProjection => {
  let runValue: WorkflowExecution | undefined
  let state: RunState | undefined
  let deleted = false
  const signalDeliveries = new Set<string>()
  for (const fact of facts) {
    switch (fact._tag) {
      case "RunCreated": {
        runValue = cloneRun(fact.run)
        deleted = false
        break
      }
      case "RunStateSaved": {
        state = cloneRunState(fact.state)
        runValue = executionFromRunState(state, runValue?.lease)
        break
      }
      case "RunPaused": {
        if (runValue !== undefined) {
          const cleared = clearRunWaitFields(runValue)
          runValue = {
            ...cleared,
            status: "paused",
            updatedAt: fact.now,
            ...(fact.awaiting === undefined ? {} : { awaiting: clone(fact.awaiting) }),
            ...(fact.pendingApproval === undefined ? {} : { pendingApproval: clone(fact.pendingApproval) }),
            ...(fact.waitingFor === undefined ? {} : { waitingFor: clone(fact.waitingFor) }),
            ...(fact.wakeAt === undefined ? {} : { wakeAt: fact.wakeAt })
          }
        }
        break
      }
      case "RunFinished": {
        if (runValue !== undefined) {
          const cleared = clearRunWaitFields(runValue)
          runValue = {
            ...cleared,
            output: clone(fact.output),
            status: "finished",
            updatedAt: fact.now
          }
        }
        break
      }
      case "RunErrored": {
        if (runValue !== undefined) {
          const cleared = clearRunWaitFields(runValue)
          runValue = {
            ...cleared,
            error: clone(fact.error),
            status: "errored",
            updatedAt: fact.now
          }
        }
        break
      }
      case "RunDeleted": {
        runValue = undefined
        state = undefined
        deleted = true
        break
      }
      case "LeaseClaimed": {
        if (runValue !== undefined) {
          runValue = { ...runValue, lease: cloneLease(fact.lease), status: "running", updatedAt: fact.now }
        }
        break
      }
      case "LeaseReleased": {
        if (runValue?.lease?.owner === fact.leaseOwner) {
          const { lease: _lease, ...withoutLease } = runValue
          runValue = withoutLease
        }
        break
      }
      case "TimerScheduled": {
        if (runValue !== undefined) {
          runValue = { ...runValue, updatedAt: fact.now, wakeAt: fact.wakeAt }
        }
        break
      }
      case "SignalDelivered": {
        signalDeliveries.add(signalKey(runId, fact.delivery.signalId))
        if (runValue !== undefined) runValue = queuedAfterDelivery(runValue, fact.now)
        break
      }
      case "ApprovalDelivered": {
        signalDeliveries.add(signalKey(runId, `approval:${fact.approval.approvalId}`))
        if (runValue !== undefined) runValue = queuedAfterDelivery(runValue, fact.now)
        break
      }
    }
  }
  return {
    deleted,
    nextSeqNum,
    signalDeliveries,
    ...(runValue === undefined ? {} : { run: runValue }),
    ...(state === undefined ? {} : { state })
  }
}

const readTimers = (runtime: S2StoreRuntime): Effect.Effect<TimerProjection, unknown> =>
  readJsonRecords<TimerFact>(runtime, streamNames(runtime).timers).pipe(
    Effect.map((result) => {
      const timers = new Map<string, TimerWakeup & { readonly lease?: WorkflowLease }>()
      for (const fact of result.records) {
        if (fact._tag === "TimerScheduled") {
          timers.set(timerKey(fact.timer.runId, fact.timer.signalId), fact.timer)
        } else if (fact._tag === "TimerClaimed") {
          const existing = timers.get(fact.key)
          if (existing !== undefined) {
            timers.set(fact.key, { ...existing, lease: fact.lease })
          }
        } else {
          timers.delete(fact.key)
        }
      }
      return { nextSeqNum: result.nextSeqNum, timers }
    })
  )

const readSchedules = (runtime: S2StoreRuntime): Effect.Effect<ScheduleProjection, unknown> =>
  readJsonRecords<ScheduleFact>(runtime, streamNames(runtime).schedules).pipe(
    Effect.map((result) => {
      const schedules = new Map<ScheduleId, ScheduleRecord>()
      const buckets = new Map<string, ScheduleBucketRecord>()
      for (const fact of result.records) {
        if (fact._tag === "ScheduleUpserted") {
          schedules.set(fact.scheduleId, {
            enabled: fact.enabled,
            overlapPolicy: fact.overlapPolicy,
            schedule: clone(fact.schedule),
            scheduleId: fact.scheduleId,
            workflowId: fact.workflowId,
            ...(fact.input === undefined ? {} : { input: clone(fact.input) }),
            ...(fact.nextFireAt === undefined ? {} : { nextFireAt: fact.nextFireAt }),
            ...(fact.workflowVersion === undefined ? {} : { workflowVersion: fact.workflowVersion })
          })
        } else if (fact._tag === "ScheduleBucketClaimed") {
          const schedule = schedules.get(fact.scheduleId)
          if (schedule !== undefined) {
            const bucket = scheduleBucket(schedule, fact.bucketId, fact.fireAt, fact.lease)
            buckets.set(scheduleBucketKey(fact.scheduleId, fact.bucketId), bucket)
          }
        } else {
          const key = scheduleBucketKey(fact.scheduleId, fact.bucketId)
          const existing = buckets.get(key)
          if (existing !== undefined) {
            buckets.set(key, { ...existing, runId: fact.runId, status: "started" })
          }
        }
      }
      return { buckets, nextSeqNum: result.nextSeqNum, schedules }
    })
  )

const jsonRecord = (value: unknown, headers: ReadonlyArray<readonly [string, string]> = []): AppendRecord =>
  AppendRecord.string({ body: JSON.stringify(value), headers })

const sanitize = (value: string): string => encodeURIComponent(value).replace(/%/g, "_")

const clone = <A>(value: A): A => value === undefined ? value : JSON.parse(JSON.stringify(value)) as A

const cloneRun = (run: WorkflowExecution): WorkflowExecution => clone(run)

const cloneRunState = (state: RunState): RunState => clone(state)

const cloneLease = (value: WorkflowLease): WorkflowLease => ({ ...value })

const lease = (owner: LeaseOwner, leaseMs: number, now: number): WorkflowLease => ({ expiresAt: now + leaseMs, owner })

const canClaim = (existing: WorkflowLease | undefined, owner: LeaseOwner, now: number): boolean =>
  existing === undefined || existing.owner === owner || existing.expiresAt <= now

const isTerminal = (status: WorkflowExecution["status"]): boolean =>
  status === "finished" || status === "errored" || status === "aborted"

const executionFromRunState = (state: RunState, leaseValue?: WorkflowLease): WorkflowExecution => ({
  createdAt: state.createdAt,
  input: clone(state.input),
  runId: state.runId,
  status: state.status,
  updatedAt: state.updatedAt,
  workflowId: state.workflowId,
  ...(state.workflowVersion === undefined ? {} : { workflowVersion: state.workflowVersion }),
  ...(state.output === undefined ? {} : { output: clone(state.output) }),
  ...(state.error === undefined ? {} : { error: clone(state.error) }),
  ...(state.awaiting === undefined ? {} : { awaiting: clone(state.awaiting) }),
  ...(state.waitingFor === undefined ? {} : { waitingFor: clone(state.waitingFor) }),
  ...(state.pendingApproval === undefined ? {} : { pendingApproval: clone(state.pendingApproval) }),
  ...(state.waitingFor?.deadline !== undefined
    ? { wakeAt: state.waitingFor.deadline }
    : {}),
  ...(leaseValue === undefined ? {} : { lease: cloneLease(leaseValue) })
})

const queuedAfterDelivery = (run: WorkflowExecution, now: number): WorkflowExecution => ({
  ...clearRunWaitFields(run),
  status: "queued",
  updatedAt: now
})

const clearRunWaitFields = (run: WorkflowExecution): WorkflowExecution => {
  const {
    awaiting: _awaiting,
    lease: _lease,
    pendingApproval: _pendingApproval,
    waitingFor: _waitingFor,
    wakeAt: _wakeAt,
    ...rest
  } = run
  return rest
}

const isRunWaitingForSignal = (run: WorkflowExecution, delivery: SignalDelivery): boolean =>
  signalAwaitableMatches(run.waitingFor, delivery)
  || run.awaiting?.some((awaitable) =>
      awaitable.type === "signal"
      && awaitable.signalName === delivery.name
      && (delivery.stepId === undefined || awaitable.stepId === undefined || awaitable.stepId === delivery.stepId)
    ) === true

const signalAwaitableMatches = (
  awaitable: WorkflowExecution["waitingFor"] | undefined,
  delivery: SignalDelivery
): boolean =>
  awaitable?.signalName === delivery.name
  && (delivery.stepId === undefined || awaitable.stepId === undefined || awaitable.stepId === delivery.stepId)

const isRunWaitingForApproval = (run: WorkflowExecution, approval: ApprovalResult): boolean =>
  run.pendingApproval?.approvalId === approval.approvalId
  || run.awaiting?.some((awaitable) =>
      awaitable.type === "approval" && awaitable.approvalId === approval.approvalId
    ) === true

const timerKey = (runId: RunId, signalId: string): string => `${runId}:${signalId}`

const signalKey = (runId: RunId, signalId: string): string => `${runId}:${signalId}`

const scheduleBucketKey = (scheduleId: ScheduleId, bucketId: ScheduleBucketId): string => `${scheduleId}:${bucketId}`

const timerWakeup = (timer: TimerWakeup): TimerWakeup => ({
  runId: timer.runId,
  signalId: timer.signalId,
  ...(timer.signalName === undefined ? {} : { signalName: timer.signalName }),
  wakeAt: timer.wakeAt,
  workflowId: timer.workflowId,
  ...(timer.workflowVersion === undefined ? {} : { workflowVersion: timer.workflowVersion })
})

const scheduleBucket = (
  schedule: ScheduleRecord,
  bucketId: ScheduleBucketId,
  fireAt: number,
  leaseValue: WorkflowLease
): ScheduleBucketRecord => ({
  bucketId,
  fireAt,
  input: clone(schedule.input),
  lease: leaseValue,
  overlapPolicy: schedule.overlapPolicy,
  runId: `${schedule.workflowId}:${schedule.scheduleId}:${bucketId}`,
  scheduleId: schedule.scheduleId,
  status: "claimed",
  workflowId: schedule.workflowId,
  ...(schedule.workflowVersion === undefined ? {} : { workflowVersion: schedule.workflowVersion })
})

const scheduleBucketWakeup = (bucket: ScheduleBucketRecord): ScheduleBucket => ({
  bucketId: bucket.bucketId,
  fireAt: bucket.fireAt,
  input: clone(bucket.input),
  overlapPolicy: bucket.overlapPolicy,
  runId: bucket.runId,
  scheduleId: bucket.scheduleId,
  workflowId: bucket.workflowId,
  ...(bucket.workflowVersion === undefined ? {} : { workflowVersion: bucket.workflowVersion })
})

const toRunSummary = (run: WorkflowExecution): RunSummary => ({
  createdAt: run.createdAt,
  runId: run.runId,
  status: run.status,
  updatedAt: run.updatedAt,
  workflowId: run.workflowId,
  ...(run.workflowVersion === undefined ? {} : { workflowVersion: run.workflowVersion }),
  ...(run.awaiting === undefined ? {} : { awaiting: clone(run.awaiting) }),
  ...(run.waitingFor === undefined ? {} : { waitingFor: clone(run.waitingFor) }),
  ...(run.pendingApproval === undefined ? {} : { pendingApproval: clone(run.pendingApproval) }),
  ...(run.wakeAt === undefined ? {} : { wakeAt: run.wakeAt })
})

const isSeqNumMismatch = (cause: unknown): boolean =>
  String(cause).includes("SeqNumMismatchError") || cause instanceof SeqNumMismatchError
