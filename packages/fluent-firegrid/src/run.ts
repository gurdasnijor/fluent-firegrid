import { Effect } from "effect"
import { Journal, type RunOptions } from "./journal.ts"
import { FluentFiregridError } from "./error.ts"
import type { FluentRequirements } from "./schema.ts"

type RunAction<A, E, R> = () =>
  | A
  | PromiseLike<A>
  | Effect.Effect<A, E, R>

interface RunClosureOptions<E, Encoded = unknown> extends RunOptions<E | FluentFiregridError, Encoded> {
  readonly name?: string
}

const isPromiseLike = <A>(
  value: unknown,
): value is PromiseLike<A> =>
  typeof value === "object" &&
  value !== null &&
  "then" in value &&
  typeof value.then === "function"

const effectFromAction = <A, E, R>(
  action: RunAction<A, E, R>,
): Effect.Effect<A, E | FluentFiregridError, R> =>
  Effect.suspend((): Effect.Effect<A, E | FluentFiregridError, R> => {
    const value = action()
    if (Effect.isEffect(value)) return value
    if (isPromiseLike<A>(value)) {
      return Effect.tryPromise({
        try: () => value,
        catch: (cause) =>
          new FluentFiregridError({
            message: "run action promise rejected",
            cause,
          }),
      })
    }
    return Effect.succeed(value)
  })

const missingStepName = Effect.fail(new FluentFiregridError({
  message: "run(action, options) requires options.name or a named action",
}))

type RunStep = {
  <A, E, R, Encoded = unknown>(
    key: string,
    action: Effect.Effect<A, E, R>,
    options?: RunOptions<E, Encoded>,
  ): Effect.Effect<A, E | FluentFiregridError, R | Journal | FluentRequirements>
}

const runStep: RunStep = (
  key,
  action,
  options,
) =>
  Journal.pipe(Effect.flatMap((journal) => journal.step(key, action, options)))

const runAction = <A, E, R, Encoded = unknown>(
  action: RunAction<A, E, R>,
  options?: RunClosureOptions<E, Encoded>,
): Effect.Effect<A, E | FluentFiregridError, R | Journal | FluentRequirements> => {
  const name = options?.name ?? action.name
  if (name === "") return missingStepName
  return runStep(
    name,
    effectFromAction(action),
    options,
  )
}

interface Run extends RunStep {
  <A, E = unknown, R = never, Encoded = unknown>(
    action: RunAction<A, E, R>,
    options?: RunClosureOptions<E, Encoded>,
  ): Effect.Effect<A, E | FluentFiregridError, R | Journal | FluentRequirements>
}

const runImpl = (
  keyOrAction: string | RunAction<unknown, unknown, unknown>,
  actionOrOptions?: Effect.Effect<unknown, unknown, unknown> | RunClosureOptions<unknown>,
  options?: RunOptions<unknown>,
) => {
  if (typeof keyOrAction === "string") {
    if (!Effect.isEffect(actionOrOptions)) {
      return Effect.fail(new FluentFiregridError({
        message: "run(key, action) requires an Effect action",
      }))
    }
    return runStep(
      keyOrAction,
      actionOrOptions,
      options,
    )
  }
  return runAction(
    keyOrAction,
    Effect.isEffect(actionOrOptions)
      ? undefined
      : actionOrOptions,
  )
}

export const run: Run = runImpl
