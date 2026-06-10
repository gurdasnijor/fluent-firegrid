import type { Effect } from "effect"
import type { DurableStreamsClientError, DurableStreamsProtocolFailure } from "./Errors.ts"

export interface StreamHandle {
  readonly path: string
  readonly append: (
    bytes: Uint8Array,
    options?: {
      readonly close?: boolean
      readonly expectedTailOffset?: string
    },
  ) => Effect.Effect<
    {
      readonly tailOffset: string
      readonly closed: boolean
    },
    DurableStreamsClientError | DurableStreamsProtocolFailure
  >
  readonly read: (
    offset?: string,
  ) => Effect.Effect<
    readonly {
      readonly bytes: Uint8Array
      readonly fromOffset: string
      readonly nextOffset: string
      readonly contentType: string
      readonly closed: boolean
    }[],
    DurableStreamsClientError | DurableStreamsProtocolFailure
  >
}
