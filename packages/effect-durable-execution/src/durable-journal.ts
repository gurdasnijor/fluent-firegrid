import { Effect, Layer, Ref } from "effect"
import { DurableStream } from "effect-durable-client"
import { DurableExecutionError } from "./error.ts"
import { FencedWriter, Journal, SessionStream, makeJournal } from "./journal.ts"
import { JournalEventSchema } from "./schema.ts"
import type { DurableExecutionRequirements, JournalEvent } from "./schema.ts"
import type { Endpoint } from "effect-durable-client"

interface DurableJournalOptions {
  readonly endpoint: Endpoint
  readonly producerId?: string
  readonly producerEpoch?: number
}

const producerIdFor = (
  endpoint: Endpoint,
  producerId: string | undefined,
): string => producerId ?? `durable-execution:journal:${String(endpoint.url)}`

const toJournalError =
  (message: string) =>
  (cause: unknown): DurableExecutionError =>
    new DurableExecutionError({ message, cause })

export const durableJournalLayer = (
  options: DurableJournalOptions,
): Layer.Layer<
  Journal | FencedWriter | SessionStream,
  DurableExecutionError,
  DurableExecutionRequirements
> => {
  const stream = DurableStream.define({
    endpoint: options.endpoint,
    schema: JournalEventSchema,
  })
  const sessionLayer = Layer.effect(
    SessionStream,
    Effect.succeed(String(options.endpoint.url)),
  )
  const writerLayer = Layer.effect(
    FencedWriter,
    Effect.gen(function* () {
      yield* stream
        .create({ contentType: "application/json" })
        .pipe(
          Effect.mapError(toJournalError("Failed to create journal stream")),
        )
      const events = yield* stream.collect.pipe(
        Effect.mapError(toJournalError("Failed to collect journal stream")),
      )
      const nextSeq = yield* Ref.make(events.length)
      const append = (event: JournalEvent) =>
        Effect.gen(function* () {
          const seq = yield* Ref.getAndUpdate(nextSeq, (current) => current + 1)
          yield* DurableStream.appendWithProducer({
            endpoint: options.endpoint,
            schema: JournalEventSchema,
            event,
            producerId: producerIdFor(options.endpoint, options.producerId),
            producerEpoch: options.producerEpoch ?? 0,
            producerSeq: seq,
          }).pipe(
            Effect.asVoid,
            Effect.mapError(toJournalError("Failed to append journal event")),
          )
        })
      return { collect: Effect.succeed(events), append }
    }),
  )
  const baseLayer = Layer.merge(sessionLayer, writerLayer)
  return Layer.effect(
    Journal,
    Effect.gen(function* () {
      const streamName = yield* SessionStream
      const writer = yield* FencedWriter
      const events = yield* writer.collect
      return makeJournal(streamName, writer, events)
    }),
  ).pipe(Layer.provideMerge(baseLayer))
}
