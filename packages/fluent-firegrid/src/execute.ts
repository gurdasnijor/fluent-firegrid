import { Effect } from "effect"
import { durableJournalLayer } from "./durable-journal.ts"
import type { FluentFiregridError } from "./error.ts"
import type { FencedWriter, Journal, SessionStream } from "./journal.ts"
import type { ExecutionContext, FluentRequirements } from "./schema.ts"

type JournalRequirements = Journal | FencedWriter | SessionStream

export const execute = <A, E, R>(
  ctx: ExecutionContext,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E | FluentFiregridError, Exclude<R, JournalRequirements> | FluentRequirements> =>
  effect.pipe(
    Effect.provide(durableJournalLayer({
      endpoint: ctx.journal.endpoint,
      ...(ctx.journal.producerId === undefined ? {} : { producerId: ctx.journal.producerId }),
      ...(ctx.journal.producerEpoch === undefined ? {} : { producerEpoch: ctx.journal.producerEpoch }),
    })),
  )
