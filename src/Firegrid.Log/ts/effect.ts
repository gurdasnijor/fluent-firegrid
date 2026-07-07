import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"

import {
  makeFiregridLog,
  S2Error,
  type AppendAck,
  type AppendInput,
  type FiregridLogClient,
  type ReadBatch,
  type ReadInput,
  type ReadRecord,
  type S2ClientConfig,
  type StreamPosition,
  type StreamRef
} from "./index.ts"

export interface FiregridLogEffectStream {
  readonly append: (input: AppendInput) => Effect.Effect<AppendAck, S2Error>
  readonly checkTail: () => Effect.Effect<{ readonly tail: StreamPosition }, S2Error>
  readonly read: (input?: ReadInput) => Effect.Effect<ReadBatch, S2Error>
  readonly readSession: (input?: ReadInput) => Stream.Stream<ReadRecord, S2Error>
}

export interface FiregridLogEffectClient {
  readonly ensureBasin: (basin: string) => Effect.Effect<void, S2Error>
  readonly ensureStream: (target: StreamRef) => Effect.Effect<void, S2Error>
  readonly stream: (target: StreamRef) => FiregridLogEffectStream
}

export class FiregridLog extends Context.Service<FiregridLog, FiregridLogEffectClient>()(
  "firegrid/log/FiregridLog"
) {}

const asS2Error = (cause: unknown): S2Error =>
  cause instanceof S2Error
    ? cause
    : new S2Error({ message: cause instanceof Error ? cause.message : String(cause), cause })

const promiseEffect = <A>(evaluate: () => Promise<A>): Effect.Effect<A, S2Error> =>
  Effect.tryPromise({
    try: evaluate,
    catch: asS2Error
  })

export const fromPromiseClient = (client: FiregridLogClient): FiregridLogEffectClient => ({
  ensureBasin: (basin) => promiseEffect(() => client.ensureBasin(basin)),
  ensureStream: (target) => promiseEffect(() => client.ensureStream(target)),
  stream: (target) => {
    const stream = client.stream(target)
    return {
      append: (input) => promiseEffect(() => stream.append(input)),
      checkTail: () => promiseEffect(() => stream.checkTail()),
      read: (input) => promiseEffect(() => stream.read(input)),
      readSession: (input) => Stream.fromAsyncIterable(stream.readSession(input), asS2Error)
    }
  }
})

export const makeFiregridLogEffect = (
  config: S2ClientConfig
): Effect.Effect<FiregridLogEffectClient, S2Error> =>
  promiseEffect(() => makeFiregridLog(config)).pipe(Effect.map(fromPromiseClient))

export const layerFiregridLog = (
  config: S2ClientConfig
): Layer.Layer<FiregridLog, S2Error> =>
  Layer.effect(FiregridLog, makeFiregridLogEffect(config))
