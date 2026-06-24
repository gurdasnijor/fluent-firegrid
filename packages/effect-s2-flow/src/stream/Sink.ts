import type * as Effect from "effect/Effect"
import type * as Option from "effect/Option"
import type * as Scope from "effect/Scope"

import type { FlowError } from "../runtime/FlowError.ts"
import type { EventStreamDef } from "./EventStream.ts"
import type { EventCursor } from "./Record.ts"
import type { EventSource } from "./Source.ts"

export type Guarantee = "atMostOnce" | "atLeastOnce" | "effectivelyOnce"

export interface CheckpointStore {
  readonly load: Effect.Effect<Option.Option<EventCursor>, FlowError>
  readonly save: (cursor: EventCursor) => Effect.Effect<void, FlowError>
}

export interface SinkDef<K, A> {
  readonly name: string
  readonly input: EventStreamDef<K, A>
  readonly guarantee: Guarantee
  readonly run: (source: EventSource<K, A>, checkpoint: CheckpointStore) => Effect.Effect<void, FlowError, Scope.Scope>
}
