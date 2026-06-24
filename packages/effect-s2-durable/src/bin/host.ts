#!/usr/bin/env node
/**
 * The `effect-s2-durable` host process entrypoint (SDD §10.2, build step 1).
 *
 * Reads its configuration from the environment (`S2_BASIN` for the namespace,
 * `INGRESS_PORT` to serve the HTTP ingress) and runs forever as a single host.
 * Boot recovery happens on layer build; there is no fenced ownership / claim
 * sweep yet (step 4) — this is N=1.
 *
 * The package ships this with an **empty catalog**: it is a "does it boot"
 * reference binary. Real deployments compose their own bin that calls
 * `startHost`/`DurableHostFromConfig` from `effect-s2-durable/host` with their
 * compile-time catalog (Model A).
 */
import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { pathToFileURL } from "node:url"
import type { AnyDef } from "../authoring/definition.ts"
// oxlint-disable-next-line effect/no-import-from-barrel-package -- host module is intentionally exported from index.ts
import { DurableHostFromConfig } from "../host/index.ts"

/** The package reference bin serves no app definitions. */
const catalog: ReadonlyArray<AnyDef> = []

export const runHostMain = (): void =>
  NodeRuntime.runMain(
    Effect.andThen(
      Console.error("effect-s2-durable host starting"),
      Layer.launch(DurableHostFromConfig(catalog))
    )
  )

const isDirectRun = process.argv[1] !== undefined
  && pathToFileURL(process.argv[1]).href === import.meta.url

if (isDirectRun) {
  runHostMain()
}
