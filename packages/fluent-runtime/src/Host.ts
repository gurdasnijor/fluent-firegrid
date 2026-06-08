import { FetchHttpClient } from "@effect/platform"
import { Layer } from "effect"
import {
  FluentEventIngressLive,
  type FluentEventIngress,
} from "./EventIngress.ts"
import {
  FluentSourcesLive,
  type FluentSources,
} from "./Sources.ts"
import {
  FluentStoreLive,
  type FluentStore,
  type StoreConfig,
} from "./Store.ts"

export type FluentRuntimeConfig = StoreConfig

export type FluentRuntimeServices =
  | FluentStore
  | FluentSources
  | FluentEventIngress

export const FluentRuntimeLive = (
  config: FluentRuntimeConfig,
): Layer.Layer<FluentRuntimeServices> => {
  const store = FluentStoreLive(config).pipe(
    Layer.provide(FetchHttpClient.layer),
  )
  const sources = FluentSourcesLive.pipe(
    Layer.provide(store),
  )
  const ingress = FluentEventIngressLive.pipe(
    Layer.provide(Layer.mergeAll(store, sources)),
  )
  return Layer.mergeAll(store, sources, ingress)
}
