import { layer as ChdbLayer } from "@firegrid/observability"
import * as Layer from "effect/Layer"

import { VerificationRuntime } from "./Property.ts"

export const layer = Layer.mergeAll(
  ChdbLayer({}),
  VerificationRuntime.layer
)
