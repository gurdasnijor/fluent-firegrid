import { Effect, Layer } from "effect"
import { DurableStreamLogTag } from "@firegrid/fluent-store"
import { EventBusTag, makeEventBus } from "./eventBus.ts"
import { StreamServerTag, makeStreamServer } from "./streamServer.ts"

export const streamServerLayer: Layer.Layer<StreamServerTag, never, DurableStreamLogTag> = Layer.effect(
  StreamServerTag,
  Effect.map(DurableStreamLogTag, makeStreamServer),
)

export const eventBusLayer: Layer.Layer<EventBusTag, never, DurableStreamLogTag> = Layer.effect(
  EventBusTag,
  Effect.map(DurableStreamLogTag, makeEventBus),
)

export const layer = Layer.merge(streamServerLayer, eventBusLayer)
