import { FileSystem, Path } from "@effect/platform"
import { Data, Effect, Option } from "effect"
import type { FirelabSimulation } from "../types.ts"

// Simulations live exactly one level deep: `simulations/<id>/index.ts`, where the
// folder name IS the `id` (enforced by scripts/firelab-layout-check.mjs).
// No recursion, no per-folder denylist: discovery lists folder names only (never
// imports), and loading a sim is isolated per-id, so one sim's bad import can no
// longer sink the whole runner.
const simulationsDirUrl = new URL("../simulations/", import.meta.url)
const moduleUrlFor = (id: string): string =>
  new URL(`${id}/index.ts`, simulationsDirUrl).href

class SimulationFolderInvalid extends Data.TaggedClass("SimulationFolderInvalid")<{
  readonly id: string
  readonly reason: string
}> {}

class UnknownSimulation extends Data.TaggedClass("UnknownSimulation")<{
  readonly id: string
  readonly available: ReadonlyArray<string>
}> {}

const isSimulation = (
  value: unknown,
): value is FirelabSimulation<unknown> => {
  if (typeof value !== "object" || value === null) return false
  const candidate = value as Record<string, unknown>
  return typeof candidate["id"] === "string" &&
    typeof candidate["description"] === "string" &&
    (typeof candidate["host"] === "function" || candidate["launchHost"] === false) &&
    candidate["driver"] !== undefined
}

// The available simulation ids: immediate subdirectories of `simulations/` that
// contain an `index.ts`. Pure directory listing — imports nothing, so it cannot
// be broken by any single sim's import error. `_`/`.`-prefixed folders are
// skipped by convention (scaffolding / hidden).
const simulationIds = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const dir = yield* path.fromFileUrl(simulationsDirUrl)
  const names = yield* fs.readDirectory(dir)
  const candidates = names
    .filter(name => !name.startsWith("_") && !name.startsWith("."))
    .sort()
  return yield* Effect.filter(candidates, name =>
    fs.exists(path.join(dir, name, "index.ts")))
})

// Import + validate a single sim by id. `Effect.tryPromise` (not `Effect.promise`)
// so a failing module import surfaces as a typed, catchable `SimulationFolderInvalid`
// rather than an uncatchable defect — that is what lets `listSimulations` skip a
// broken sim and `selectedSimulation` import only the one requested.
const loadSimulationById = (
  id: string,
): Effect.Effect<FirelabSimulation<unknown>, SimulationFolderInvalid> =>
  Effect.gen(function*() {
    const module = yield* Effect.tryPromise({
      try: () =>
        import(moduleUrlFor(id)) as Promise<{ readonly default?: unknown }>,
      catch: cause =>
        new SimulationFolderInvalid({
          id,
          reason: `import failed: ${cause instanceof Error ? cause.message : String(cause)}`,
        }),
    })
    if (!isSimulation(module.default)) {
      return yield* Effect.fail(new SimulationFolderInvalid({
        id,
        reason: "missing default export of simulation shape",
      }))
    }
    if (module.default.id !== id) {
      return yield* Effect.fail(new SimulationFolderInvalid({
        id,
        reason: `id "${module.default.id}" does not match folder "${id}"`,
      }))
    }
    return module.default
  })

// `simulate list`: load every discovered sim, but isolate failures — a sim whose
// import or shape is broken is skipped with a warning, not fatal to the listing.
export const listSimulations = Effect.gen(function*() {
  const ids = yield* simulationIds
  const loaded = yield* Effect.forEach(
    ids,
    id =>
      loadSimulationById(id).pipe(
        Effect.map(Option.some),
        Effect.catchAll(error =>
          Effect.logWarning(
            `firelab: skipping simulation "${id}" — ${error.reason}`,
          ).pipe(Effect.as(Option.none<FirelabSimulation<unknown>>())),
        ),
      ),
    { concurrency: "unbounded" },
  )
  return loaded.filter(Option.isSome).map(some => some.value)
})

// Resolve a simulation by id. Imports ONLY the requested sim — running one sim
// never loads (and so never trips over) any other. On miss, fail with the
// available ids (a pure folder listing) so the CLI lists them. There is no
// "default simulation": running without an explicit id must error rather than
// silently picking the alphabetically-first folder.
export const selectedSimulation = (
  simulationId: string,
) =>
  Effect.gen(function*() {
    const ids = yield* simulationIds
    if (!ids.includes(simulationId)) {
      return yield* Effect.fail(
        new UnknownSimulation({ id: simulationId, available: ids }),
      )
    }
    return yield* loadSimulationById(simulationId)
  })
