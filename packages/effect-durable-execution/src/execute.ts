import { Effect } from "effect"
import { durableJournalLayer } from "./durable-journal.ts"
import type { DurableExecutionError } from "./error.ts"
import type { FencedWriter, Journal, SessionStream } from "./journal.ts"
import type {
  DurableExecutionRequirements,
  ExecutionContext,
} from "./schema.ts"

type JournalRequirements = Journal | FencedWriter | SessionStream

export const execute = <A, E, R>(
  ctx: ExecutionContext,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<
  A,
  E | DurableExecutionError,
  Exclude<R, JournalRequirements> | DurableExecutionRequirements
> =>
  effect.pipe(
    Effect.provide(
      durableJournalLayer({
        endpoint: ctx.journal.endpoint,
        ...(ctx.journal.producerId === undefined
          ? {}
          : { producerId: ctx.journal.producerId }),
        ...(ctx.journal.producerEpoch === undefined
          ? {}
          : { producerEpoch: ctx.journal.producerEpoch }),
      }),
    ),
  )
