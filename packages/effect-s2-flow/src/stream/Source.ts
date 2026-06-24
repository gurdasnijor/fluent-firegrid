import type * as Effect from "effect/Effect"
import type * as Scope from "effect/Scope"
import type * as Stream from "effect/Stream"

import type { FlowError } from "../runtime/FlowError.ts"
import type { EventStreamDef } from "./EventStream.ts"
import type { EventCursor, EventRecord } from "./Record.ts"

export interface EventSink<K, A> {
  readonly emit: (key: K, value: A) => Effect.Effect<EventCursor, FlowError>
}

export interface EventSource<K, A> {
  readonly records: Stream.Stream<EventRecord<K, A>, FlowError>
}

export interface SourceDef<K, A> {
  readonly name: string
  readonly output: EventStreamDef<K, A>
  readonly run: (sink: EventSink<K, A>) => Effect.Effect<void, FlowError, Scope.Scope>
}
