import { Schema } from "effect"

export const SessionIdSchema = Schema.String
export const TurnIdSchema = Schema.String
export const TimerIdSchema = Schema.String
export const WaitIdSchema = Schema.String

export type SessionId = Schema.Schema.Type<typeof SessionIdSchema>
export type TurnId = Schema.Schema.Type<typeof TurnIdSchema>
export type TimerId = Schema.Schema.Type<typeof TimerIdSchema>
export type WaitId = Schema.Schema.Type<typeof WaitIdSchema>

const SessionCreatedEventSchema = Schema.Struct({
  type: Schema.Literal("session.created"),
  sessionId: SessionIdSchema,
  agent: Schema.String,
})

const SessionEventAppendedSchema = Schema.Struct({
  type: Schema.Literal("session.event_appended"),
  sessionId: SessionIdSchema,
  name: Schema.String,
  payload: Schema.Unknown,
})

export const StateChangeMessageSchema = Schema.Struct({
  type: Schema.String,
  key: Schema.String,
  value: Schema.Unknown,
  old_value: Schema.optional(Schema.Unknown),
  headers: Schema.Record({ key: Schema.String, value: Schema.String }),
})

export const SessionEventSchema = Schema.Union(
  SessionCreatedEventSchema,
  SessionEventAppendedSchema,
  StateChangeMessageSchema,
)

const TurnStartedEventSchema = Schema.Struct({
  type: Schema.Literal("turn.started"),
  sessionId: SessionIdSchema,
  turnId: TurnIdSchema,
  prompt: Schema.String,
})

const TurnCompletedEventSchema = Schema.Struct({
  type: Schema.Literal("turn.completed"),
  sessionId: SessionIdSchema,
  turnId: TurnIdSchema,
  result: Schema.Unknown,
})

const TurnFailedEventSchema = Schema.Struct({
  type: Schema.Literal("turn.failed"),
  sessionId: SessionIdSchema,
  turnId: TurnIdSchema,
  message: Schema.String,
})

const TurnTimerScheduledEventSchema = Schema.Struct({
  type: Schema.Literal("turn.timer_scheduled"),
  sessionId: SessionIdSchema,
  turnId: TurnIdSchema,
  timerId: TimerIdSchema,
  fireAtEpochMs: Schema.Number,
})

const TurnTimerFiredEventSchema = Schema.Struct({
  type: Schema.Literal("turn.timer_fired"),
  sessionId: SessionIdSchema,
  turnId: TurnIdSchema,
  timerId: TimerIdSchema,
  firedAtEpochMs: Schema.Number,
})

const TurnWaitRegisteredEventSchema = Schema.Struct({
  type: Schema.Literal("turn.wait_registered"),
  sessionId: SessionIdSchema,
  turnId: TurnIdSchema,
  waitId: WaitIdSchema,
  predicate: Schema.String,
  afterOffset: Schema.String,
  self: Schema.optional(Schema.Unknown),
})

const TurnWaitMatchedEventSchema = Schema.Struct({
  type: Schema.Literal("turn.wait_matched"),
  sessionId: SessionIdSchema,
  turnId: TurnIdSchema,
  waitId: WaitIdSchema,
  matchedOffset: Schema.String,
  event: Schema.Unknown,
})

export const TurnEventSchema = Schema.Union(
  TurnStartedEventSchema,
  TurnCompletedEventSchema,
  TurnFailedEventSchema,
  TurnTimerScheduledEventSchema,
  TurnTimerFiredEventSchema,
  TurnWaitRegisteredEventSchema,
  TurnWaitMatchedEventSchema,
)

export type SessionEventAppended = Schema.Schema.Type<typeof SessionEventAppendedSchema>
export type StateChangeMessage = Schema.Schema.Type<typeof StateChangeMessageSchema>
export type SessionEvent = Schema.Schema.Type<typeof SessionEventSchema>

export type TurnStartedEvent = Schema.Schema.Type<typeof TurnStartedEventSchema>
export type TurnCompletedEvent = Schema.Schema.Type<typeof TurnCompletedEventSchema>
export type TurnFailedEvent = Schema.Schema.Type<typeof TurnFailedEventSchema>
export type TurnTimerScheduledEvent = Schema.Schema.Type<typeof TurnTimerScheduledEventSchema>
export type TurnTimerFiredEvent = Schema.Schema.Type<typeof TurnTimerFiredEventSchema>
export type TurnWaitRegisteredEvent = Schema.Schema.Type<typeof TurnWaitRegisteredEventSchema>
export type TurnWaitMatchedEvent = Schema.Schema.Type<typeof TurnWaitMatchedEventSchema>
export type TurnEvent = Schema.Schema.Type<typeof TurnEventSchema>

export interface SessionHandle {
  readonly sessionId: SessionId
  readonly eventsUrl: string
}

export interface TurnHandle {
  readonly sessionId: SessionId
  readonly turnId: TurnId
  readonly eventsUrl: string
}
