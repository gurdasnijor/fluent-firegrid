import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"

import { greeter } from "./CapabilityAGreeter.ts"
import { flowRuntimeLayerFromEnv, hostTraceLayerFromEnv, serve } from "./runtime.ts"

NodeRuntime.runMain(
  serve({ services: [greeter] }).pipe(
    Effect.provide(Layer.mergeAll(flowRuntimeLayerFromEnv(), hostTraceLayerFromEnv()))
  )
)
