import { Context, Effect, Layer } from "effect"
import { FluentStore, type FireTurnTimerResult, type FluentRuntimeError, type MatchTurnWaitResult } from "./Store.ts"
import type {
  TimerId,
  TurnEvent,
  TurnHandle,
  TurnId,
  TurnTimerFiredEvent,
  TurnTimerScheduledEvent,
  TurnWaitMatchedEvent,
  TurnWaitRegisteredEvent,
  SessionId,
  WaitId,
} from "./Domain.ts"

export interface FireDueTurnTimersInput {
  readonly sessionId: SessionId
  readonly turnId: TurnId
  readonly nowEpochMs: number
}

export interface FiredDueTurnTimer {
  readonly timerId: TimerId
  readonly fireAtEpochMs: number
  readonly firedAtEpochMs: number
  readonly write: FireTurnTimerResult["write"]
}

export interface PendingTurnTimer {
  readonly timerId: TimerId
  readonly fireAtEpochMs: number
}

export interface AlreadyFiredTurnTimer {
  readonly timerId: TimerId
  readonly fireAtEpochMs: number
  readonly firedAtEpochMs: number
}

export interface FireDueTurnTimersResult {
  readonly turn: TurnHandle
  readonly fired: ReadonlyArray<FiredDueTurnTimer>
  readonly pending: ReadonlyArray<PendingTurnTimer>
  readonly alreadyFired: ReadonlyArray<AlreadyFiredTurnTimer>
}

export interface MatchPendingTurnWaitsInput {
  readonly sessionId: SessionId
  readonly turnId: TurnId
  readonly matchedOffset: string
  readonly event: unknown
}

export interface MatchedTurnWait {
  readonly waitId: WaitId
  readonly write: Extract<MatchTurnWaitResult, { readonly _tag: "Matched" }>["write"]
}

export interface UnmatchedTurnWait {
  readonly waitId: WaitId
}

export interface AlreadyMatchedTurnWait {
  readonly waitId: WaitId
  readonly matchedOffset: string
}

export interface MatchPendingTurnWaitsResult {
  readonly turn: TurnHandle
  readonly matched: ReadonlyArray<MatchedTurnWait>
  readonly notMatched: ReadonlyArray<UnmatchedTurnWait>
  readonly alreadyMatched: ReadonlyArray<AlreadyMatchedTurnWait>
}

interface TimerState {
  readonly scheduled: TurnTimerScheduledEvent
  readonly fired?: TurnTimerFiredEvent
}

interface WaitState {
  readonly registered: TurnWaitRegisteredEvent
  readonly matched?: TurnWaitMatchedEvent
}

export class FluentSources extends Context.Tag("@firegrid/fluent-runtime/Sources/FluentSources")<
  FluentSources,
  {
    readonly fireDueTurnTimers: (
      input: FireDueTurnTimersInput,
    ) => Effect.Effect<FireDueTurnTimersResult, FluentRuntimeError>
    readonly matchPendingTurnWaits: (
      input: MatchPendingTurnWaitsInput,
    ) => Effect.Effect<MatchPendingTurnWaitsResult, FluentRuntimeError>
  }
>() {}

const timerStatesFromEvents = (
  events: ReadonlyArray<TurnEvent>,
): ReadonlyMap<TimerId, TimerState> => {
  const states = new Map<TimerId, TimerState>()
  for (let index = 0; index < events.length; index += 1) {
    const event = events[index]
    if (event === undefined) continue
    if (event.type === "turn.timer_scheduled") {
      states.set(event.timerId, { scheduled: event })
      continue
    }
    if (event.type === "turn.timer_fired") {
      const state = states.get(event.timerId)
      if (state !== undefined) {
        states.set(event.timerId, { ...state, fired: event })
      }
    }
  }
  return states
}

const waitStatesFromEvents = (
  events: ReadonlyArray<TurnEvent>,
): ReadonlyMap<WaitId, WaitState> => {
  const states = new Map<WaitId, WaitState>()
  for (let index = 0; index < events.length; index += 1) {
    const event = events[index]
    if (event === undefined) continue
    if (event.type === "turn.wait_registered") {
      states.set(event.waitId, { registered: event })
      continue
    }
    if (event.type === "turn.wait_matched") {
      const state = states.get(event.waitId)
      if (state !== undefined) {
        states.set(event.waitId, { ...state, matched: event })
      }
    }
  }
  return states
}

export const FluentSourcesLive = Layer.effect(
  FluentSources,
  Effect.gen(function* () {
    const store = yield* FluentStore

    return {
      fireDueTurnTimers: (input) =>
        Effect.gen(function* () {
          const read = yield* store.readTurn(input.sessionId, input.turnId)
          const fired: Array<FiredDueTurnTimer> = []
          const pending: Array<PendingTurnTimer> = []
          const alreadyFired: Array<AlreadyFiredTurnTimer> = []
          const timerStates = Array.from(timerStatesFromEvents(read.events).values())

          for (let index = 0; index < timerStates.length; index += 1) {
            const state = timerStates[index]
            if (state === undefined) continue
            if (state.fired !== undefined) {
              alreadyFired.push({
                timerId: state.scheduled.timerId,
                fireAtEpochMs: state.scheduled.fireAtEpochMs,
                firedAtEpochMs: state.fired.firedAtEpochMs,
              })
              continue
            }
            if (state.scheduled.fireAtEpochMs > input.nowEpochMs) {
              pending.push({
                timerId: state.scheduled.timerId,
                fireAtEpochMs: state.scheduled.fireAtEpochMs,
              })
              continue
            }
            const result = yield* store.fireTurnTimer({
              sessionId: input.sessionId,
              turnId: input.turnId,
              timerId: state.scheduled.timerId,
              firedAtEpochMs: input.nowEpochMs,
            })
            fired.push({
              timerId: state.scheduled.timerId,
              fireAtEpochMs: state.scheduled.fireAtEpochMs,
              firedAtEpochMs: input.nowEpochMs,
              write: result.write,
            })
          }

          return { turn: read.turn, fired, pending, alreadyFired }
        }).pipe(
          Effect.withSpan("fluent_runtime.sources.timer.fire_due", {
            attributes: {
              "firegrid.session.id": input.sessionId,
              "firegrid.turn.id": input.turnId,
              "fluent_runtime.timer.now_epoch_ms": input.nowEpochMs,
            },
          }),
        ),
      matchPendingTurnWaits: (input) =>
        Effect.gen(function* () {
          const read = yield* store.readTurn(input.sessionId, input.turnId)
          const matched: Array<MatchedTurnWait> = []
          const notMatched: Array<UnmatchedTurnWait> = []
          const alreadyMatched: Array<AlreadyMatchedTurnWait> = []
          const waitStates = Array.from(waitStatesFromEvents(read.events).values())

          for (let index = 0; index < waitStates.length; index += 1) {
            const state = waitStates[index]
            if (state === undefined) continue
            if (state.matched !== undefined) {
              alreadyMatched.push({
                waitId: state.registered.waitId,
                matchedOffset: state.matched.matchedOffset,
              })
              continue
            }
            const result = yield* store.matchTurnWait({
              sessionId: input.sessionId,
              turnId: input.turnId,
              waitId: state.registered.waitId,
              matchedOffset: input.matchedOffset,
              event: input.event,
            })
            if (result._tag === "Matched") {
              matched.push({ waitId: state.registered.waitId, write: result.write })
            } else {
              notMatched.push({ waitId: state.registered.waitId })
            }
          }

          return { turn: read.turn, matched, notMatched, alreadyMatched }
        }).pipe(
          Effect.withSpan("fluent_runtime.sources.wait.match_pending", {
            attributes: {
              "firegrid.session.id": input.sessionId,
              "firegrid.turn.id": input.turnId,
              "fluent_runtime.wait.matched_offset": input.matchedOffset,
            },
          }),
        ),
    }
  }),
)
