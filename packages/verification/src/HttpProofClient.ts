import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as HttpClient from "effect/unstable/http/HttpClient"

import { VerificationError } from "./VerificationError.ts"

export interface ProofHttpRequestOptions {
  readonly method?: "GET" | "POST"
}

const execute = (
  url: string,
  options: ProofHttpRequestOptions | undefined
) =>
  options?.method === "POST"
    ? HttpClient.post(url, { acceptJson: true })
    : HttpClient.get(url, { acceptJson: true })

export const requestJson = <A>(
  url: string,
  options?: ProofHttpRequestOptions
): Effect.Effect<A, VerificationError> =>
  Effect.gen(function*() {
    const response = yield* execute(url, options)
    if (response.status < 200 || response.status >= 300) {
      const body = yield* response.text
      return yield* new VerificationError({
        cause: body,
        message: `proof HTTP request ${url} failed with ${response.status}`
      })
    }
    const json = yield* response.json
    return json as A
  }).pipe(
    Effect.mapError((cause) =>
      Schema.is(VerificationError)(cause)
        ? cause
        : new VerificationError({ cause, message: `proof HTTP request failed: ${url}` })
    ),
    Effect.provide(NodeHttpClient.layerFetch)
  )
