import { Context, Effect, Option, Schema } from "effect"
import { FluentFiregridError } from "./error.ts"
import type { FluentRequirements, JournalEvent, StepFailedEvent, StepSucceededEvent } from "./schema.ts"

export interface RunOptions<E, Encoded = unknown> {
  readonly errorSchema?: Schema.Schema<E, Encoded>
}

export class SessionStream extends Context.Tag("@firegrid/fluent-firegrid/journal/SessionStream")<
  SessionStream,
  string
>() {}

export interface FencedWriterService {
  readonly collect: Effect.Effect<ReadonlyArray<JournalEvent>, FluentFiregridError>
  readonly append: (
    event: JournalEvent,
  ) => Effect.Effect<void, FluentFiregridError, FluentRequirements>
}

export class FencedWriter extends Context.Tag("@firegrid/fluent-firegrid/journal/FencedWriter")<
  FencedWriter,
  FencedWriterService
>() {}

export interface JournalService {
  readonly step: <A, E, R, Encoded = unknown>(
    key: string,
    action: Effect.Effect<A, E, R>,
    options?: RunOptions<E, Encoded>,
  ) => Effect.Effect<A, E | FluentFiregridError, R | FluentRequirements>
  readonly find: (
    key: string,
  ) => Option.Option<JournalEvent>
  readonly append: (
    event: JournalEvent,
  ) => Effect.Effect<void, FluentFiregridError, FluentRequirements>
  readonly stream: string
}

export class Journal extends Context.Tag("@firegrid/fluent-firegrid/journal")<
  Journal,
  JournalService
>() {}

const succeededFor = (
  event: JournalEvent,
  key: string,
): event is StepSucceededEvent =>
  event.type === "StepSucceeded" && event.stepKey === key

const failedFor = (
  event: JournalEvent,
  key: string,
): event is StepFailedEvent =>
  event.type === "StepFailed" && event.stepKey === key

const decodeFailure = <E, Encoded>(
  key: string,
  failed: StepFailedEvent,
  options?: RunOptions<E, Encoded>,
): Effect.Effect<never, E | FluentFiregridError> => {
  if (options?.errorSchema === undefined) {
    return Effect.fail(new FluentFiregridError({
      message: `Journaled step failed: ${key}: ${failed.message}`,
      ...(failed.error === undefined ? {} : { cause: failed.error }),
    }))
  }
  return Schema.decodeUnknown(options.errorSchema)(failed.error).pipe(
    Effect.mapError((cause) =>
      new FluentFiregridError({
        message: `Failed to decode journaled error for ${key}`,
        cause,
      }),
    ),
    Effect.flatMap((error) => Effect.fail(error)),
  )
}

const encodeFailure = <E, Encoded>(
  error: E,
  options?: RunOptions<E, Encoded>,
): unknown =>
  options?.errorSchema === undefined
    ? error
    : Schema.encodeUnknownSync(options.errorSchema)(error)

export const makeJournal = (
  stream: string,
  writer: FencedWriterService,
  events: ReadonlyArray<JournalEvent>,
): JournalService => {
  const seen = new Map(events.map((event) => {
    switch (event.type) {
      case "StepSucceeded":
      case "StepFailed": {
        return [event.stepKey, event] as const
      }
    }
  }))

  const find = (key: string): Option.Option<JournalEvent> =>
    Option.fromNullable(seen.get(key))

  const step = <A, E, R, Encoded = unknown>(
    key: string,
    action: Effect.Effect<A, E, R>,
    options?: RunOptions<E, Encoded>,
  ): Effect.Effect<A, E | FluentFiregridError, R | FluentRequirements> =>
    Effect.gen(function* () {
      const hit = seen.get(key)
      yield* Effect.annotateCurrentSpan({
        "fluent.step.key": key,
        "fluent.step.replayed": hit === undefined ? "false" : "true",
      })
      if (hit !== undefined && succeededFor(hit, key)) {
        yield* Effect.annotateCurrentSpan("fluent.step.served", "journal")
        return hit.value as A
      }
      if (hit !== undefined && failedFor(hit, key)) {
        yield* Effect.annotateCurrentSpan("fluent.step.served", "journal")
        return yield* decodeFailure(key, hit, options)
      }

      yield* Effect.annotateCurrentSpan("fluent.step.served", "executed")
      const result = yield* Effect.either(action.pipe(Effect.withSpan("step.action")))
      if (result._tag === "Right") {
        const event: JournalEvent = {
          type: "StepSucceeded",
          stepKey: key,
          name: key,
          value: result.right,
        }
        yield* writer.append(event)
        seen.set(key, event)
        return result.right
      }

      const event: JournalEvent = {
        type: "StepFailed",
        stepKey: key,
        name: key,
        message: String(result.left),
        error: encodeFailure(result.left, options),
      }
      yield* writer.append(event)
      seen.set(key, event)
      return yield* Effect.fail(result.left)
    }).pipe(Effect.withSpan("journal.step"))

  return {
    step,
    find,
    append: writer.append,
    stream,
  }
}
