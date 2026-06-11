import { Effect, Layer } from "effect"
import { DurableStreamLogTag } from "@firegrid/fluent-stream-log"
import * as InMemoryDurableStreamLog from "./inMemoryDurableStreamLog.ts"

export const layer: Layer.Layer<DurableStreamLogTag> = Layer.effect(
  DurableStreamLogTag,
  InMemoryDurableStreamLog.make(),
)

export const scoped = Effect.acquireRelease(InMemoryDurableStreamLog.make(), () => Effect.void)
