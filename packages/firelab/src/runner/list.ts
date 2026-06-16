import { FileSystem, Path } from "effect"
import { Data, Effect, Option } from "effect"
import type { FirelabValidation } from "../types.ts"

// Validations live exactly one level deep: `validations/<id>/index.ts`, where the
// folder name IS the `id` (enforced by scripts/firelab-layout-check.mjs).
// No recursion, no per-folder denylist: discovery lists folder names only (never
// imports), and loading a validation is isolated per-id, so one broken validation can no
// longer sink the whole runner.
const validationsDirUrl = new URL("../validations/", import.meta.url)
const moduleUrlFor = (id: string): string =>
  new URL(`${id}/index.ts`, validationsDirUrl).href

class ValidationFolderInvalid extends Data.TaggedClass("ValidationFolderInvalid")<{
  readonly id: string
  readonly reason: string
}> {}

class UnknownValidation extends Data.TaggedClass("UnknownValidation")<{
  readonly id: string
  readonly available: ReadonlyArray<string>
}> {}

const isValidation = (
  value: unknown,
): value is FirelabValidation<unknown> => {
  if (typeof value !== "object" || value === null) return false
  const candidate = value as Record<string, unknown>
  return typeof candidate["id"] === "string" &&
    typeof candidate["description"] === "string" &&
    typeof candidate["component"] === "function"
}

// The available validation ids: immediate subdirectories of `validations/` that
// contain an `index.ts`. Pure directory listing — imports nothing, so it cannot
// be broken by any single validation's import error. `_`/`.`-prefixed folders are
// skipped by convention (scaffolding / hidden).
const validationIds = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const dir = yield* path.fromFileUrl(validationsDirUrl)
  const names = yield* fs.readDirectory(dir)
  const candidates = names
    .filter(name => !name.startsWith("_") && !name.startsWith("."))
    .sort()
  return yield* Effect.filter(candidates, name =>
    fs.exists(path.join(dir, name, "index.ts")))
})

// Import + validate a single validation by id. `Effect.tryPromise` (not
// `Effect.promise`) so a failing module import surfaces as a typed, catchable
// `ValidationFolderInvalid` rather than an uncatchable defect.
const loadValidationById = (
  id: string,
): Effect.Effect<FirelabValidation<unknown>, ValidationFolderInvalid> =>
  Effect.gen(function*() {
    const module = yield* Effect.tryPromise({
      try: () =>
        import(moduleUrlFor(id)) as Promise<{ readonly default?: unknown }>,
      catch: cause =>
        new ValidationFolderInvalid({
          id,
          reason: `import failed: ${cause instanceof Error ? cause.message : String(cause)}`,
        }),
    })
    if (!isValidation(module.default)) {
      return yield* Effect.fail(new ValidationFolderInvalid({
        id,
        reason: "missing default export of validation shape",
      }))
    }
    if (module.default.id !== id) {
      return yield* Effect.fail(new ValidationFolderInvalid({
        id,
        reason: `id "${module.default.id}" does not match folder "${id}"`,
      }))
    }
    return module.default
  })

// `list`: load every discovered validation, but isolate failures — a validation
// whose import or shape is broken is skipped with a warning, not fatal.
export const listValidations = Effect.gen(function*() {
  const ids = yield* validationIds
  const loaded = yield* Effect.forEach(
    ids,
    id =>
      loadValidationById(id).pipe(
        Effect.map(Option.some),
        Effect.catch((error: ValidationFolderInvalid) =>
          Effect.logWarning(
            `firelab: skipping validation "${id}" — ${error.reason}`,
          ).pipe(Effect.as(Option.none<FirelabValidation<unknown>>())),
        ),
      ),
    { concurrency: "unbounded" },
  )
  return loaded.filter(Option.isSome).map(some => some.value)
})

// Resolve a validation by id. Imports ONLY the requested validation — one
// validation never loads, and so never trips over, any other. On miss, fail with
// available ids. There is no default validation.
export const selectedValidation = (
  validationId: string,
) =>
  Effect.gen(function*() {
    const ids = yield* validationIds
    if (!ids.includes(validationId)) {
      return yield* Effect.fail(
        new UnknownValidation({ id: validationId, available: ids }),
      )
    }
    return yield* loadValidationById(validationId)
  })
