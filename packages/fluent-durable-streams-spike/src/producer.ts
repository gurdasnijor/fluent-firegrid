import { Effect, SynchronizedRef } from "effect"
import type { StreamPath } from "@firegrid/fluent-stream-log"
import type { StreamBody } from "./content.ts"
import type { AppendStreamOutcome, DurableStreamsChannel } from "./model.ts"

export type ProducerError =
  | {
      readonly _tag: "ProducerFenced"
      readonly currentEpoch: number
    }
  | {
      readonly _tag: "ProducerSequenceGap"
      readonly expectedSeq: number
      readonly receivedSeq: number
    }
  | {
      readonly _tag: "ProducerAppendRejected"
      readonly outcome: Exclude<AppendStreamOutcome, { readonly _tag: "Appended" | "Duplicate" }>
    }

export interface ProducerState {
  readonly epoch: number
  readonly seq: number
}

export interface ProducerConfig {
  readonly path: StreamPath
  readonly contentType: string
  readonly producerId: string
  readonly startEpoch?: number
  readonly autoClaim?: boolean
}

export interface Producer {
  readonly append: (
    body: StreamBody,
    options?: { readonly close?: boolean },
  ) => Effect.Effect<Extract<AppendStreamOutcome, { readonly _tag: "Appended" | "Duplicate" }>, ProducerError>
  readonly state: Effect.Effect<ProducerState>
}

const success = (outcome: AppendStreamOutcome) =>
  outcome._tag === "Appended" || outcome._tag === "Duplicate"

const producerFailure = (
  outcome: Exclude<AppendStreamOutcome, { readonly _tag: "Appended" | "Duplicate" }>,
): ProducerError => {
  switch (outcome._tag) {
    case "Fenced":
      return { _tag: "ProducerFenced", currentEpoch: outcome.currentEpoch }
    case "SequenceGap":
      return {
        _tag: "ProducerSequenceGap",
        expectedSeq: outcome.expectedSeq,
        receivedSeq: outcome.receivedSeq,
      }
    case "Noop":
    case "AlreadyClosed":
    case "WriteToClosed":
    case "ContentMismatch":
    case "OffsetConflict":
    case "BadRequest":
    case "Conflict":
    case "NotFound":
    case "Gone":
      return { _tag: "ProducerAppendRejected", outcome }
  }
}

export function makeProducer(
  channel: DurableStreamsChannel,
  config: ProducerConfig,
): Effect.Effect<Producer> {
  const initial: ProducerState = {
    epoch: config.startEpoch ?? 0,
    seq: 0,
  }

  return SynchronizedRef.make(initial).pipe(
    Effect.map((ref): Producer => {
      const producer = (state: ProducerState) => ({
        producerId: config.producerId,
        epoch: state.epoch,
        seq: state.seq,
      })

      const send = (state: ProducerState, body: StreamBody, close: boolean) =>
        channel.append({
          path: config.path,
          contentType: config.contentType,
          body,
          close,
          producer: producer(state),
        })

      const handle = (
        state: ProducerState,
        body: StreamBody,
        close: boolean,
        outcome: AppendStreamOutcome,
      ): Effect.Effect<
        readonly [Extract<AppendStreamOutcome, { readonly _tag: "Appended" | "Duplicate" }>, ProducerState],
        ProducerError
      > => {
        if (success(outcome)) {
          return Effect.succeed([outcome, { ...state, seq: state.seq + 1 }] as const)
        }
        if (outcome._tag === "Fenced" && config.autoClaim === true) {
          const claimedState = { epoch: outcome.currentEpoch + 1, seq: 0 }
          return send(claimedState, body, close).pipe(
            Effect.flatMap((claimed) =>
              success(claimed)
                ? Effect.succeed([claimed, { epoch: claimedState.epoch, seq: 1 }] as const)
                : Effect.fail(producerFailure(claimed)),
            ),
          )
        }
        return Effect.fail(producerFailure(outcome))
      }

      return {
        append: (body, options) =>
          SynchronizedRef.modifyEffect(ref, (state) =>
            send(state, body, options?.close ?? false).pipe(
              Effect.flatMap((outcome) => handle(state, body, options?.close ?? false, outcome)),
            ),
          ),
        state: SynchronizedRef.get(ref),
      }
    }),
  )
}
