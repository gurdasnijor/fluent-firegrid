import * as CEL from "./CEL.ts"
import type { CelExpression } from "./CEL.ts"

export interface SubscriptionFilter {
  readonly language: "cel"
  readonly expression: string
  readonly self?: unknown
}

export interface PullWakeSubscriptionConfig {
  readonly type: "pull-wake"
  readonly streams?: ReadonlyArray<string>
  readonly pattern?: string
  readonly wake_stream: string
  readonly filter?: SubscriptionFilter
  readonly lease_ttl_ms?: number
  readonly description?: string
}

export interface FilteredPullWakeOptions {
  readonly streamPath: string
  readonly wakeStream: string
  readonly filter: CelExpression
  readonly self?: unknown
  readonly leaseTtlMs?: number
  readonly description?: string
}

export const filteredPullWakeConfig = (
  opts: FilteredPullWakeOptions,
): PullWakeSubscriptionConfig => ({
  type: "pull-wake",
  streams: [opts.streamPath],
  wake_stream: opts.wakeStream,
  filter: {
    language: "cel",
    expression: CEL.expression(opts.filter),
    ...(opts.self !== undefined ? { self: opts.self } : {}),
  },
  ...(opts.leaseTtlMs !== undefined ? { lease_ttl_ms: opts.leaseTtlMs } : {}),
  ...(opts.description !== undefined ? { description: opts.description } : {}),
})

export interface SubscriptionClient {
  readonly filteredPullWakeConfig: (
    opts: FilteredPullWakeOptions,
  ) => PullWakeSubscriptionConfig
}

export const makeSubscriptionClient = (): SubscriptionClient => ({
  filteredPullWakeConfig,
})
