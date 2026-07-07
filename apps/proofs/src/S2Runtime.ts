import { S2 } from "@s2-dev/streamstore"
import { makeFiregridLog } from "@firegrid/log"
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"

import { VerificationError } from "./VerificationError.ts"

interface S2StreamInput {
  readonly basin: string
  readonly stream: string
  readonly ensure?: boolean
}

export interface StreamApi {
  readonly append: (input: { readonly records?: ReadonlyArray<unknown> }) => Effect.Effect<any, unknown>
  readonly checkTail: () => Effect.Effect<any, unknown>
  readonly readSession: (input: unknown) => Stream.Stream<any, unknown>
}

export interface S2Runtime {
  readonly endpoint: string | undefined
  readonly stream: (input: S2StreamInput) => Effect.Effect<StreamApi, VerificationError>
}

export type S2RuntimeDriver = "upstream-sdk" | "firegrid-log"

const makeClient = (endpoint: string) =>
  new S2({
    accessToken: "s2_access_token",
    endpoints: {
      account: endpoint,
      basin: endpoint
    },
    retry: { maxAttempts: 1 }
  })

const s2Error = (message: string) => (cause: unknown): VerificationError => new VerificationError({ message, cause })

const errorAttributes = (cause: unknown): Record<string, string> => {
  if (typeof cause !== "object" || cause === null) {
    return { "s2.error.kind": typeof cause }
  }
  const record = cause as Record<string, unknown>
  return {
    "s2.error.kind": cause.constructor.name,
    ...("code" in record ? { "s2.error.code": String(record.code) } : {}),
    ...("status" in record ? { "s2.error.status": String(record.status) } : {}),
    ...("expectedSeqNum" in record ? { "s2.error.expected_seq_num": String(record.expectedSeqNum) } : {}),
    ...("expectedFencingToken" in record ? { "s2.error.expected_fencing_token": String(record.expectedFencingToken) } : {})
  }
}

const traced = <A>(
  span: string,
  effect: Effect.Effect<A, unknown>,
  attributes: Record<string, string> = {}
): Effect.Effect<A, unknown> =>
  effect.pipe(
    Effect.tap(() =>
      Effect.annotateCurrentSpan({
        ...attributes,
        "s2.operation.status": "ok"
      })
    ),
    Effect.tapError((cause) =>
      Effect.annotateCurrentSpan({
        ...attributes,
        ...errorAttributes(cause),
        "s2.operation.status": "error"
      })
    ),
    Effect.withSpan(span, {
      attributes: {
        ...attributes,
        "s2.operation.status": "running"
      }
    })
  )

const promiseEffect = <A>(thunk: () => PromiseLike<A>): Effect.Effect<A, unknown> =>
  Effect.tryPromise({
    try: thunk,
    catch: (cause) => cause
  })

const makeStreamApi = (spanPrefix: string, stream: any): StreamApi => ({
  append: (input) =>
    traced(
      `${spanPrefix}.append`,
      promiseEffect(() => stream.append(input)),
      { "s2.append.record_count": String(input.records?.length ?? 0) }
    ),
  checkTail: () =>
    traced(`${spanPrefix}.check-tail`, promiseEffect(() => stream.checkTail())),
  readSession: (input) =>
    Stream.fromIterableEffect(
      traced(
        `${spanPrefix}.read-session`,
        promiseEffect(async() => {
          const records: Array<unknown> = []
          const session = await stream.readSession(input)
          for await (const record of session) {
            records.push(record)
          }
          return records
        })
      )
    )
})

const makeUpstreamStream = (
  endpoint: string,
  input: S2StreamInput
): Effect.Effect<StreamApi, VerificationError> =>
  Effect.gen(function*() {
    const client = makeClient(endpoint)
    const basin = client.basin(input.basin)
    if (input.ensure ?? true) {
      yield* Effect.tryPromise({
        try: () => client.basins.ensure({ basin: input.basin }),
        catch: s2Error(`failed to ensure basin ${input.basin}`)
      })
      yield* Effect.tryPromise({
        try: () => basin.streams.ensure({ stream: input.stream }),
        catch: s2Error(`failed to ensure stream ${input.stream}`)
      })
    }
    return makeStreamApi("effect-s2", basin.stream(input.stream))
  })

const makeFiregridLogStream = (
  endpoint: string,
  input: S2StreamInput
): Effect.Effect<StreamApi, VerificationError> =>
  Effect.gen(function*() {
    const client = yield* Effect.tryPromise({
      try: () =>
        makeFiregridLog({
          accessToken: "s2_access_token",
          endpoint
        }),
      catch: s2Error("failed to create Firegrid.Log client")
    })
    if (input.ensure ?? true) {
      yield* Effect.tryPromise({
        try: () => client.ensureBasin(input.basin),
        catch: s2Error(`failed to ensure basin ${input.basin}`)
      })
      yield* Effect.tryPromise({
        try: () => client.ensureStream({ basin: input.basin, stream: input.stream }),
        catch: s2Error(`failed to ensure stream ${input.stream}`)
      })
    }
    return makeStreamApi("firegrid-log", client.stream({ basin: input.basin, stream: input.stream }))
  })

export const makeS2Runtime = (
  endpoint: string | undefined,
  driver: S2RuntimeDriver = "upstream-sdk"
): S2Runtime => ({
  endpoint,
  stream: (input) => {
    if (endpoint === undefined) {
      return new VerificationError({ message: "property does not have s2Lite configured" })
    }
    return driver === "firegrid-log"
      ? makeFiregridLogStream(endpoint, input)
      : makeUpstreamStream(endpoint, input)
  }
})
