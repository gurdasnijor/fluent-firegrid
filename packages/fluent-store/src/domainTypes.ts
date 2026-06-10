import { Schema, pipe } from "effect"

export const StreamPath = pipe(
  Schema.String,
  Schema.nonEmptyString(),
  Schema.brand("StreamPath"),
)
export type StreamPath = typeof StreamPath.Type

export const Offset = Schema.String.pipe(Schema.pattern(/^(?!-1$)(?!now$)[^,&=?/]+$/u), Schema.brand("Offset"))
export type Offset = typeof Offset.Type

export const BeginningOffset = "-1"
export type BeginningOffset = typeof BeginningOffset

export const NowOffset = "now"
export type NowOffset = typeof NowOffset

export type ReadOffset = Offset | BeginningOffset | NowOffset

export const decodeStreamPath = Schema.decode(StreamPath)
export const decodeOffset = Schema.decode(Offset)

export const initialOffset = "00000000000000000000" as Offset

export const makeOffset = (sequence: number): Offset =>
  String(sequence).padStart(20, "0") as Offset
