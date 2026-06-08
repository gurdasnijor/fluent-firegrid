import { Effect, Fiber } from "effect"
import { FluentFiregridError } from "./error.ts"

export const all: typeof Effect.all = (arg, options) =>
  Effect.all(arg, {
    concurrency: "unbounded",
    ...(options ?? {}),
  }) as never

export const race = <A, E, R>(
  effects: ReadonlyArray<Effect.Effect<A, E, R>>,
): Effect.Effect<A, E | FluentFiregridError, R> => {
  const [first, ...rest] = effects
  if (first === undefined) {
    return Effect.fail(new FluentFiregridError({
      message: "race requires at least one effect",
    }))
  }
  return rest.reduce((winner, effect) => Effect.race(winner, effect), first)
}

interface SelectResult<Tag extends PropertyKey, A> {
  readonly tag: Tag
  readonly future: Effect.Effect<A>
}

export const select = <
  const Branches extends Record<PropertyKey, Effect.Effect<unknown, unknown, unknown>>,
>(
  branches: Branches,
): Effect.Effect<
  {
    readonly [Tag in keyof Branches]: SelectResult<
      Tag,
      Effect.Effect.Success<Branches[Tag]>
    >
  }[keyof Branches],
  Effect.Effect.Error<Branches[keyof Branches]>,
  Effect.Effect.Context<Branches[keyof Branches]>
> => {
  const effects = Reflect.ownKeys(branches).flatMap((tag) => {
    const effect = branches[tag]
    if (effect === undefined) return []
    return [effect.pipe(
      Effect.map((value) => ({
        tag,
        future: Effect.succeed(value),
      })),
    )]
  })
  const [first, ...rest] = effects
  if (first === undefined) {
    return Effect.fail(new FluentFiregridError({
      message: "select requires at least one branch",
    })) as never
  }
  return rest.reduce((winner, effect) => Effect.race(winner, effect), first) as never
}

export const spawn = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> =>
  Effect.gen(function* () {
    const fiber = yield* Effect.fork(effect)
    return yield* Fiber.join(fiber)
  })
