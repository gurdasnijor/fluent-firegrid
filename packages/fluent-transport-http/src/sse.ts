import { Sse } from "@effect/experimental"
import { Effect, Either, Encoding, Schema, Stream } from "effect"

export interface SseEvent {
  readonly event?: string
  readonly id?: string
  readonly retry?: number
  readonly data: string
}

export const SseControlEvent = Schema.Struct({
  streamNextOffset: Schema.String,
  streamCursor: Schema.optional(Schema.String),
  upToDate: Schema.optional(Schema.Boolean),
  streamClosed: Schema.optional(Schema.Boolean),
})
export type SseControlEvent = typeof SseControlEvent.Type

export const encodeSseEvent = (event: SseEvent): string => {
  const lines = [
    ...(event.id === undefined ? [] : [`id: ${event.id}`]),
    ...(event.event === undefined ? [] : [`event: ${event.event}`]),
    ...(event.retry === undefined ? [] : [`retry: ${event.retry}`]),
    ...event.data.split(/\r?\n/u).map((line) => `data: ${line}`),
    "",
    "",
  ]
  return lines.join("\n")
}

export const encodeSseDataEvent = (data: string): string =>
  encodeSseEvent({
    event: "data",
    data,
  })

export const encodeSseControlEvent = (control: SseControlEvent): string =>
  encodeSseEvent({
    event: "control",
    data: JSON.stringify(control),
  })

export const encodeBase64Bytes = (bytes: Uint8Array): string =>
  Encoding.encodeBase64(bytes)

export const encodeBase64DataEvent = (bytes: Uint8Array): string =>
  encodeSseEvent({
    event: "data",
    data: encodeBase64Bytes(bytes),
  })

export const normalizeBase64SseData = (data: string): string => data.replace(/[\n\r]/gu, "")

export const decodeBase64SseData = (data: string): Effect.Effect<Uint8Array, Encoding.DecodeException> =>
  Either.match(Encoding.decodeBase64(normalizeBase64SseData(data)), {
    onLeft: Effect.fail,
    onRight: Effect.succeed,
  })

export const decodeSseControlEvent = Schema.decode(Schema.parseJson(SseControlEvent))

export const parseSseText = (
  text: string,
): Stream.Stream<Sse.Event, never> =>
  Stream.make(text).pipe(
    Stream.pipeThroughChannel(Sse.makeChannel()),
  )
