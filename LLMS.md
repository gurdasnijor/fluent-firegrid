# LLM Guidance

This repository uses Effect heavily. Agents should treat this file as the
working Effect style guide for changes in this repo.

This guidance is adapted from the Effect team documentation at
<https://github.com/Effect-TS/effect-smol/blob/main/LLMS.md>. When in doubt,
prefer the exact Effect version pinned in this repo and verify APIs with
TypeScript instead of guessing from outdated examples.

## Writing Effect Code

- Prefer `Effect.gen(function*() { ... })` for readable imperative Effect code.
- Use `yield*` to access the value produced by an effect.
- Attach cross-cutting behavior with combinators such as `Effect.catch`,
  `Effect.withSpan`, `Effect.annotateLogs`, `Effect.retry`, or `Effect.provide`.
- When raising an Effect error inside a generator, use `return yield* ...` so
  TypeScript understands control flow.

```ts
import { Effect, Schema } from "effect"

export class FileProcessingError extends Schema.TaggedErrorClass<FileProcessingError>()(
  "FileProcessingError",
  { message: Schema.String },
) {}

export const program = Effect.gen(function*() {
  yield* Effect.logInfo("reading file")
  return yield* new FileProcessingError({ message: "failed to read file" })
}).pipe(
  Effect.catchTag("FileProcessingError", (error) => Effect.logError(error.message)),
)
```

## Functions Returning Effects

Use `Effect.fn("name")` for named functions that return an Effect. The name
should match the exported function name or be a clearly qualified operation
name used for tracing.

Avoid this shape for new named functions:

```ts
export const loadUser = (id: string) =>
  Effect.gen(function*() {
    // ...
  })
```

Prefer:

```ts
export const loadUser = Effect.fn("loadUser")(function*(id: string) {
  // ...
})
```

Pass additional behavior as extra arguments to `Effect.fn`; do not add a
`.pipe(...)` around the `Effect.fn` declaration unless an existing local pattern
requires it.

## Services And Layers

- Define services with `Context.Service`.
- Put construction in a `Layer`.
- Define service methods with `Effect.fn` when they return effects.
- Use `Service["Service"]` when the service interface type is needed.

```ts
import { Context, Effect, Layer, Schema } from "effect"

export class DatabaseError extends Schema.TaggedErrorClass<DatabaseError>()(
  "DatabaseError",
  { cause: Schema.Defect() },
) {}

export class Database extends Context.Service<Database, {
  readonly query: (sql: string) => Effect.Effect<ReadonlyArray<unknown>, DatabaseError>
}>()("fluent-firegrid/Database") {
  static readonly layer = Layer.effect(
    Database,
    Effect.gen(function*() {
      const query = Effect.fn("Database.query")(function*(sql: string) {
        yield* Effect.logDebug("executing SQL", sql)
        return []
      })

      return Database.of({ query })
    }),
  )
}

export type DatabaseService = Database["Service"]
```

## Errors

- Prefer typed tagged errors for expected failures.
- Use `Schema.TaggedErrorClass` when schema support is useful or already local.
- Use `Data.TaggedError` where the package already uses that lighter pattern.
- Recover with `Effect.catchTag`, `Effect.catchTags`, or package-local helpers.
- Reserve thrown JavaScript errors for programmer errors, test assertions, or
  non-Effect integration boundaries.

## Resources

- Use `Effect.acquireRelease` for resource lifecycles.
- Use `Layer.effect`, `Layer.scoped`, `Layer.unwrap`, and `Layer.provideMerge`
  to wire services.
- Use `ManagedRuntime` only at integration boundaries where non-Effect code must
  run Effect programs.
- Do not hide run-level mutable state behind ad hoc globals when Cucumber,
  Vitest, or Effect scopes already provide the lifecycle.

## Streams, Time, And Observability

- Use `Stream` for pull-based effectful sequences.
- Use `PubSub` when one producer fans out to multiple consumers.
- Use `Schedule` for retry, repeat, polling, and backoff behavior.
- Prefer Effect `DateTime`/`Clock` APIs over raw `Date.now()` in Effect programs
  that need testable time.
- Use Effect logging/tracing APIs and existing package trace utilities
  instead of bespoke side channels.

## Testing

- Keep Effect tests in the style already used by the package.
- Prefer shared layers for service tests.
- If adding new Effect test infrastructure, prefer `@effect/vitest` patterns
  unless the package already has a simpler local convention.
