import * as Clock from "effect/Clock"
import * as Effect from "effect/Effect"

import { type CompletedTrial, type PropertySpec, runProperty, type RunPropertyOptions } from "./Property.ts"

export interface ProofContext {
  readonly trialId: string
}

export interface Proof<A = any> {
  readonly name: string
  readonly description: string
  readonly makeSpec: (context: ProofContext) => PropertySpec<A>
}

export interface CompletedProof<A = any> {
  readonly proof: Proof<A>
  readonly trial: CompletedTrial<A>
}

export interface RunProofOptions {
  readonly trialId?: string
  readonly reportDir?: string
  readonly s2Lite?: RunPropertyOptions["s2Lite"]
}

const trialIdFromProof = (name: string, now: number): string => `${name}-${now}`.replace(/[^A-Za-z0-9_.-]/g, "-")

export const runProof = Effect.fn("runProof")(function*<A>(
  proof: Proof<A>,
  options: RunProofOptions = {}
) {
  const now = yield* Clock.currentTimeMillis
  const trialId = options.trialId ?? trialIdFromProof(proof.name, now)
  const spec = proof.makeSpec({ trialId })
  const trial = yield* runProperty(spec, {
    trialId,
    ...(options.reportDir === undefined ? {} : { reportDir: options.reportDir }),
    ...(options.s2Lite === undefined ? {} : { s2Lite: options.s2Lite })
  })
  return { proof, trial } satisfies CompletedProof<A>
})
