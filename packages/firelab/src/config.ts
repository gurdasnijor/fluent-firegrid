import { Context } from "effect"

/**
 * Client/consumer configuration — the durable-streams endpoint a simulation
 * driver talks to. Inlined into firelab (formerly `@firegrid/client-sdk/config`)
 * so the package depends only on the fluent/substrate surface. Sims read
 * `durableStreamsBaseUrl` / `namespace`; the runner provides it via
 * `Effect.provideService(FiregridConfig, { ... })`.
 */
export interface ClientOptions {
  readonly durableStreamsBaseUrl?: string
  readonly namespace?: string
}

export class FiregridConfig extends Context.Tag("firelab/config/FiregridConfig")<
  FiregridConfig,
  ClientOptions
>() {}
