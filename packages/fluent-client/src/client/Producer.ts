import { Data, Effect, SynchronizedRef } from "effect"
import * as Protocol from "@firegrid/fluent-protocol"

export class ProducerFenced extends Data.TaggedError("ProducerFenced")<
  Readonly<{
    readonly currentEpoch: number
  }>
> {}

export class ProducerSequenceGap extends Data.TaggedError("ProducerSequenceGap")<
  Readonly<{
    readonly expectedSeq: number
    readonly receivedSeq: number
  }>
> {}

export class ProducerStreamClosed extends Data.TaggedError("ProducerStreamClosed")<
  Readonly<{
    readonly finalOffset: string
  }>
> {}

export class ProducerContentMismatch extends Data.TaggedError("ProducerContentMismatch")<
  Readonly<{
    readonly expected: string
    readonly actual: string
  }>
> {}

export class ProducerOffsetConflict extends Data.TaggedError("ProducerOffsetConflict")<
  Readonly<{
    readonly expectedTailOffset: string
    readonly actualTailOffset: string
  }>
> {}

export class ProducerStreamNotFound extends Data.TaggedError("ProducerStreamNotFound")<
  Readonly<Record<string, never>>
> {}

export class ProducerStreamGone extends Data.TaggedError("ProducerStreamGone")<
  Readonly<Record<string, never>>
> {}

export class ProducerTransportFailure extends Data.TaggedError("ProducerTransportFailure")<
  Readonly<{
    readonly cause: Protocol.TransportError
  }>
> {}

export type ProducerError =
  | ProducerFenced
  | ProducerSequenceGap
  | ProducerStreamClosed
  | ProducerContentMismatch
  | ProducerOffsetConflict
  | ProducerStreamNotFound
  | ProducerStreamGone
  | ProducerTransportFailure

export interface ProducerState {
  readonly epoch: number
  readonly seq: number
}

export interface Producer {
  readonly append: (
    bytes: Uint8Array,
    options?: { readonly close?: boolean },
  ) => Effect.Effect<Protocol.Appended | Protocol.AppendDuplicate, ProducerError>
  readonly state: Effect.Effect<ProducerState>
}

export interface ProducerConfig {
  readonly path: Protocol.Append["path"]
  readonly contentType: string
  readonly producerId: string
  readonly startEpoch?: number
  readonly autoClaim?: boolean
  readonly maxTransportRetries?: number
}

const producerError = (response: Protocol.AppendResponse): ProducerError => {
  switch (response._tag) {
    case "EpochFenced":
      return new ProducerFenced({ currentEpoch: response.currentEpoch })
    case "SequenceGap":
      return new ProducerSequenceGap({
        expectedSeq: response.expectedSeq,
        receivedSeq: response.receivedSeq,
      })
    case "WriteToClosed":
      return new ProducerStreamClosed({ finalOffset: response.finalOffset })
    case "ContentMismatch":
      return new ProducerContentMismatch({
        expected: response.expected,
        actual: response.actual,
      })
    case "OffsetConflict":
      return new ProducerOffsetConflict({
        expectedTailOffset: response.expectedTailOffset,
        actualTailOffset: response.actualTailOffset,
      })
    case "StreamNotFound":
      return new ProducerStreamNotFound({})
    case "StreamGone":
      return new ProducerStreamGone({})
    case "Appended":
    case "AppendDuplicate":
      return new ProducerStreamGone({})
  }
}

export const makeProducer = (
  transport: Protocol.DurableTransportService,
  config: ProducerConfig,
): Effect.Effect<Producer> =>
  SynchronizedRef.make<ProducerState>({
    epoch: config.startEpoch ?? 0,
    seq: 0,
  }).pipe(
    Effect.map((ref): Producer => {
      const sendAt = (
        state: ProducerState,
        bytes: Uint8Array,
        close: boolean,
      ): Effect.Effect<Protocol.AppendResponse, ProducerTransportFailure> =>
        Effect.suspend(() =>
          transport.call(
            new Protocol.Append({
              path: config.path,
              contentType: config.contentType,
              bytes,
              close,
              producer: new Protocol.ProducerFence({
                producerId: config.producerId,
                epoch: state.epoch,
                seq: state.seq,
              }),
            }),
          ),
        ).pipe(
          Effect.retry({
            times: config.maxTransportRetries ?? 5,
            while: (error) => error._tag === "TransportError",
          }),
          Effect.catchAll((cause) => Effect.fail(new ProducerTransportFailure({ cause }))),
        )

      const handleResponse = (
        state: ProducerState,
        bytes: Uint8Array,
        close: boolean,
        response: Protocol.AppendResponse,
      ): Effect.Effect<
        readonly [Protocol.Appended | Protocol.AppendDuplicate, ProducerState],
        ProducerError
      > => {
        switch (response._tag) {
          case "Appended":
          case "AppendDuplicate":
            return Effect.succeed([response, { epoch: state.epoch, seq: state.seq + 1 }] as const)
          case "EpochFenced":
            if (config.autoClaim !== true) {
              return Effect.fail(new ProducerFenced({ currentEpoch: response.currentEpoch }))
            }
            return sendAt({ epoch: response.currentEpoch + 1, seq: 0 }, bytes, close).pipe(
              Effect.flatMap((claimed) =>
                claimed._tag === "Appended" || claimed._tag === "AppendDuplicate"
                  ? Effect.succeed([
                      claimed,
                      { epoch: response.currentEpoch + 1, seq: 1 },
                    ] as const)
                  : Effect.fail(producerError(claimed)),
              ),
            )
          case "SequenceGap":
          case "WriteToClosed":
          case "ContentMismatch":
          case "OffsetConflict":
          case "StreamNotFound":
          case "StreamGone":
            return Effect.fail(producerError(response))
        }
      }

      return {
        append: (bytes, options) =>
          SynchronizedRef.modifyEffect(ref, (state) =>
            sendAt(state, bytes, options?.close ?? false).pipe(
              Effect.flatMap((response) => handleResponse(state, bytes, options?.close ?? false, response)),
            ),
          ),
        state: SynchronizedRef.get(ref),
      }
    }),
  )
