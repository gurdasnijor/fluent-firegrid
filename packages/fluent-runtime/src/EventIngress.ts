import { Context, Effect, Layer } from "effect"
import type { ProducerAppendResult } from "effect-durable-client"
import type { SessionHandle, SessionId, StateChangeMessage, TurnId, WaitId } from "./Domain.ts"
import { FluentSources, type AlreadyMatchedTurnWait, type UnmatchedTurnWait } from "./Sources.ts"
import { FluentStore, type FluentRuntimeError } from "./Store.ts"

export interface IngestExternalEventInput {
  readonly sessionId: SessionId
  readonly turnId: TurnId
  readonly deliveryId: string
  readonly type: string
  readonly key: string
  readonly value: unknown
  readonly oldValue?: unknown
  readonly source?: string
  readonly headers?: Readonly<Record<string, string>>
}

export interface IngressMatchedWait {
  readonly waitId: WaitId
  readonly write: ProducerAppendResult
}

export interface IngressWaitSummary {
  readonly matched: ReadonlyArray<IngressMatchedWait>
  readonly notMatched: ReadonlyArray<UnmatchedTurnWait>
  readonly alreadyMatched: ReadonlyArray<AlreadyMatchedTurnWait>
}

export type IngestExternalEventResult =
  | {
    readonly _tag: "Appended"
    readonly session: SessionHandle
    readonly write: ProducerAppendResult
    readonly change: StateChangeMessage
    readonly waits: IngressWaitSummary
    readonly redrive: boolean
  }
  | {
    readonly _tag: "Duplicate"
    readonly session: SessionHandle
    readonly write: ProducerAppendResult
    readonly change: StateChangeMessage
    readonly waits: IngressWaitSummary
    readonly redrive: false
  }

export class FluentEventIngress extends Context.Tag("@firegrid/fluent-runtime/EventIngress/FluentEventIngress")<
  FluentEventIngress,
  {
    readonly ingestExternalEvent: (
      input: IngestExternalEventInput,
    ) => Effect.Effect<IngestExternalEventResult, FluentRuntimeError>
  }
>() {}

const encodeSegment = (segment: string): string => encodeURIComponent(segment)

const eventIngressProducerId = (
  input: IngestExternalEventInput,
): string =>
  [
    "fluent-runtime",
    "event-ingress",
    encodeSegment(input.source ?? "external"),
    encodeSegment(input.deliveryId),
  ].join("/")

const emptyWaitSummary: IngressWaitSummary = {
  matched: [],
  notMatched: [],
  alreadyMatched: [],
}

const stateChangeForInput = (
  input: IngestExternalEventInput,
  producerId: string,
): StateChangeMessage => ({
  type: input.type,
  key: input.key,
  value: input.value,
  ...(input.oldValue === undefined ? {} : { old_value: input.oldValue }),
  headers: {
    ...(input.headers ?? {}),
    operation: input.headers?.operation ?? "external",
    delivery_id: input.deliveryId,
    producer_id: producerId,
    source: input.source ?? "external",
  },
})

export const FluentEventIngressLive = Layer.effect(
  FluentEventIngress,
  Effect.gen(function* () {
    const store = yield* FluentStore
    const sources = yield* FluentSources

    return {
      ingestExternalEvent: (input) =>
        Effect.gen(function* () {
          const producerId = eventIngressProducerId(input)
          const change = stateChangeForInput(input, producerId)
          const append = yield* store.appendStateChangeFenced({
            sessionId: input.sessionId,
            change,
            fence: { producerId, epoch: 0, seq: 0 },
          })

          if (append.write._tag === "Duplicate") {
            return {
              _tag: "Duplicate" as const,
              session: append.handle,
              write: append.write,
              change,
              waits: emptyWaitSummary,
              redrive: false,
            } satisfies IngestExternalEventResult
          }

          const waits = yield* sources.matchPendingTurnWaits({
            sessionId: input.sessionId,
            turnId: input.turnId,
            matchedOffset: append.write.offset,
            event: change,
          })
          return {
            _tag: "Appended" as const,
            session: append.handle,
            write: append.write,
            change,
            waits: {
              matched: waits.matched,
              notMatched: waits.notMatched,
              alreadyMatched: waits.alreadyMatched,
            },
            redrive: waits.matched.length > 0,
          } satisfies IngestExternalEventResult
        }).pipe(
          Effect.withSpan("fluent_runtime.event_ingress.external", {
            attributes: {
              "firegrid.session.id": input.sessionId,
              "firegrid.turn.id": input.turnId,
              "fluent_runtime.event.type": input.type,
              "fluent_runtime.event.delivery_id": input.deliveryId,
            },
          }),
        ),
    }
  }),
)
