import type { HttpClient } from "@effect/platform"
import { Schema } from "effect"
import type { Endpoint } from "effect-durable-streams"

const StepSucceededEventSchema = Schema.Struct({
  type: Schema.Literal("StepSucceeded"),
  stepKey: Schema.String,
  name: Schema.String,
  value: Schema.Unknown,
})

const StepFailedEventSchema = Schema.Struct({
  type: Schema.Literal("StepFailed"),
  stepKey: Schema.String,
  name: Schema.String,
  message: Schema.String,
  error: Schema.optional(Schema.Unknown),
})

export const JournalEventSchema = Schema.Union(
  StepSucceededEventSchema,
  StepFailedEventSchema,
)

export type JournalEvent = Schema.Schema.Type<typeof JournalEventSchema>
export type StepSucceededEvent = Schema.Schema.Type<typeof StepSucceededEventSchema>
export type StepFailedEvent = Schema.Schema.Type<typeof StepFailedEventSchema>
export type FluentRequirements = HttpClient.HttpClient

export interface ExecutionContext {
  readonly journal: {
    readonly endpoint: Endpoint
    readonly producerId?: string
    readonly producerEpoch?: number
  }
}
