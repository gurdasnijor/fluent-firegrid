import { Effect, Layer } from "effect"
import { DurableStreamLogTag } from "@firegrid/fluent-store"
import * as InMemoryStore from "./InMemoryStore.ts"

export const make = InMemoryStore.makeDurableStreamLog

export const layer: Layer.Layer<DurableStreamLogTag> = Layer.effect(DurableStreamLogTag, make())

export const scoped = Effect.acquireRelease(make(), () => Effect.void)
