import type { FiregridConfig } from "./config.ts"
import type { Effect, Layer } from "effect"
import type { CoverageSpec } from "./runner/coverage.ts"

export type { CoverageSpec } from "./runner/coverage.ts"

export type FirelabHost = never

export interface FirelabStopSignal {
  readonly complete: Effect.Effect<void>
}

export interface FirelabHostEnv {
  readonly simulationId: string
  readonly runId: string
  readonly namespace: string
  readonly durableStreamsBaseUrl: string
  readonly processEnv: NodeJS.ProcessEnv
  readonly stopSignal: FirelabStopSignal
}

export interface FirelabSimulationDefinition<A, E = unknown> {
  readonly id: string
  readonly description: string
  readonly host?: (
    env: FirelabHostEnv,
  ) => Layer.Layer<FirelabHost, E>
  readonly launchHost?: boolean
  readonly driver: Effect.Effect<A, E, FiregridConfig>
  /**
   * The trace-coverage oracle for this simulation. The verdict is computed from
   * the run's host-substrate OTel spans (runner/coverage.ts), not asserted by the
   * driver. `gates` decide the verdict and are lint-restricted to forge-proof
   * host-substrate span names; `corroborations` are report-only. Optional during
   * migration — a sim without a spec runs but produces no computed verdict.
   */
  readonly coverage?: CoverageSpec
}

declare const FirelabSimulationBrand: unique symbol

export type FirelabSimulation<A, E = unknown> =
  FirelabSimulationDefinition<A, E> & {
    readonly [FirelabSimulationBrand]: typeof FirelabSimulationBrand
  }

export const defineSimulation = <A, E = unknown>(
  simulation: FirelabSimulationDefinition<A, E>,
): FirelabSimulation<A, E> =>
  simulation as FirelabSimulation<A, E>
