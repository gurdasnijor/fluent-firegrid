import { Context, type Effect, type Scope, type Stream } from "effect"
import type { DurableStreamLog, DurableStreamLogError, TailAdvanced } from "@firegrid/fluent-store"

export interface EventBus {
  readonly tailAdvanced: () => Effect.Effect<
    Stream.Stream<TailAdvanced, DurableStreamLogError>,
    DurableStreamLogError,
    Scope.Scope
  >
}

export class EventBusTag extends Context.Service<EventBusTag, EventBus>()(
  "@firegrid/fluent-server/EventBus",
) {}

export const makeEventBus = (log: DurableStreamLog): EventBus => ({
  tailAdvanced: log.subscribeAll,
})
