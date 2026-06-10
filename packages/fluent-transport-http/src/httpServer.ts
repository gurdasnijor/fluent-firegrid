import type { Stream } from "effect"
import type { SseControlEvent } from "./sse.ts"

export interface HttpStreamRecord {
  readonly bytes: Uint8Array
  readonly contentType: string
  readonly streamNextOffset: string
  readonly streamCursor?: string
  readonly upToDate?: boolean
  readonly streamClosed?: boolean
}

export type HttpReadResponse =
  | {
      readonly _tag: "CatchUp"
      readonly record: HttpStreamRecord
    }
  | {
      readonly _tag: "LongPollTimeout"
      readonly streamNextOffset: string
      readonly streamCursor?: string
      readonly streamClosed?: boolean
    }
  | {
      readonly _tag: "Sse"
      readonly events: Stream.Stream<HttpSseEvent>
    }

export type HttpSseEvent =
  | {
      readonly _tag: "Data"
      readonly data: string
      readonly encoded: boolean
    }
  | {
      readonly _tag: "Control"
      readonly control: SseControlEvent
    }

export const dataEvent = (data: string, options: { readonly encoded?: boolean } = {}): HttpSseEvent => ({
  _tag: "Data",
  data,
  encoded: options.encoded === true,
})

export const controlEvent = (control: SseControlEvent): HttpSseEvent => ({
  _tag: "Control",
  control,
})
