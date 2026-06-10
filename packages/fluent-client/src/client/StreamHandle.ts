import type { Effect } from "effect"
import type {
  Append,
  AppendResponse,
  Read,
  ReadResponse,
  TransportError,
} from "@firegrid/fluent-protocol"

export interface StreamHandle {
  readonly path: string
  readonly append: (
    bytes: Uint8Array,
    options?: {
      readonly close?: boolean
      readonly expectedTailOffset?: Append["expectedTailOffset"]
    },
  ) => Effect.Effect<AppendResponse, TransportError>
  readonly read: (
    offset?: Read["offset"],
  ) => Effect.Effect<ReadResponse, TransportError>
}
