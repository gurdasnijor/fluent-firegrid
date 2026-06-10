import { Context, type Effect, type Stream } from "effect"
import type { DurableStreamLog, DurableStreamLogError, TailAdvanced } from "@firegrid/fluent-store"

export interface EventBus {
  readonly tailAdvanced: () => Effect.Effect<Stream.Stream<TailAdvanced, DurableStreamLogError>, DurableStreamLogError>
}

export class EventBusTag extends Context.Tag("@firegrid/fluent-server/EventBus")<
  EventBusTag,
  EventBus
>() {}

export const makeEventBus = (log: DurableStreamLog): EventBus => ({
  tailAdvanced: log.subscribeAll,
})
