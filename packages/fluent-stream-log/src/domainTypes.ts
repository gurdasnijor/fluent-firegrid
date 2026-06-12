import { Schema, pipe } from "effect"

export const StreamPath = pipe(
  Schema.String,
  Schema.check(Schema.isNonEmpty()),
  Schema.brand("StreamPath"),
)
export type StreamPath = typeof StreamPath.Type

export const StreamId = pipe(
  Schema.String,
  Schema.check(Schema.isNonEmpty()),
  Schema.brand("StreamId"),
)
export type StreamId = typeof StreamId.Type

export const Offset = Schema.String.pipe(
  Schema.check(Schema.isPattern(/^(?!-1$)(?!now$)[^,&=?/]+$/u)),
  Schema.brand("Offset"),
)
export type Offset = typeof Offset.Type

export const BeginningOffset = "-1"
export type BeginningOffset = typeof BeginningOffset

export const NowOffset = "now"
export type NowOffset = typeof NowOffset

export type ReadOffset = Offset | BeginningOffset | NowOffset

export const decodeStreamPath = Schema.decodeEffect(StreamPath)
export const decodeOffset = Schema.decodeEffect(Offset)

export const initialOffset = "00000000000000000000" as Offset

export const makeOffset = (sequence: number): Offset =>
  String(sequence).padStart(20, "0") as Offset
