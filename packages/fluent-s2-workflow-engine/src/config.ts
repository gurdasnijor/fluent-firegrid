import { Context, Layer } from "effect"
import type { S2ClientOptions } from "@s2-dev/streamstore"

export interface S2WorkflowEngineConfig {
  readonly basin: string
  readonly accessToken: string
  readonly endpoints?: S2ClientOptions["endpoints"]
  readonly streamPrefix?: string
  readonly requestTimeoutMillis?: number
  readonly connectionTimeoutMillis?: number
}

export class S2WorkflowEngineConfigTag extends Context.Service<
  S2WorkflowEngineConfigTag,
  S2WorkflowEngineConfig
>()("@firegrid/fluent-s2-workflow-engine/S2WorkflowEngineConfig") {}

export const layerConfig = (
  config: S2WorkflowEngineConfig,
): Layer.Layer<S2WorkflowEngineConfigTag> =>
  Layer.succeed(S2WorkflowEngineConfigTag, config)

export const defaultStreamPrefix = "workflow"
