import { basin, basins, layer as S2Layer, type S2Error, stream as s2Stream, type StreamApi } from "@firegrid/log"
import * as Effect from "effect/Effect"

import { VerificationError } from "./VerificationError.ts"

interface S2StreamInput {
  readonly basin: string
  readonly stream: string
  readonly ensure?: boolean
}

export interface S2Runtime {
  readonly endpoint: string | undefined
  readonly stream: (input: S2StreamInput) => Effect.Effect<StreamApi, VerificationError>
}

const s2Layer = (endpoint: string) =>
  S2Layer({
    accessToken: "s2_access_token",
    endpoints: {
      account: endpoint,
      basin: endpoint
    },
    retry: { maxAttempts: 1 }
  })

const s2Error = (message: string) => (cause: S2Error): VerificationError => new VerificationError({ message, cause })

export const makeS2Runtime = (endpoint: string | undefined): S2Runtime => ({
  endpoint,
  stream: (input) => {
    if (endpoint === undefined) {
      return new VerificationError({ message: "property does not have s2Lite configured" })
    }
    return Effect.gen(function*() {
      if (input.ensure ?? true) {
        yield* basins.ensure({ basin: input.basin }).pipe(
          Effect.mapError(s2Error(`failed to ensure basin ${input.basin}`))
        )
        const basinApi = yield* basin(input.basin).pipe(
          Effect.mapError(s2Error(`failed to open basin ${input.basin}`))
        )
        yield* basinApi.streams.ensure({ stream: input.stream }).pipe(
          Effect.mapError(s2Error(`failed to ensure stream ${input.stream}`))
        )
      }
      return yield* s2Stream(input.basin, input.stream).pipe(
        Effect.mapError(s2Error(`failed to open stream ${input.basin}/${input.stream}`))
      )
    }).pipe(Effect.provide(s2Layer(endpoint)))
  }
})
