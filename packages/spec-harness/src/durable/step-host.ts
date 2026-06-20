import type { StepDefinitionPatternType, StepMatchArgument } from "@cucumber/messages"
import { matchStep } from "./matcher.ts"
import type { CompiledStep, SupportBundle } from "./support.ts"
import type { StepDefId } from "./types.ts"

/**
 * The step host — the owner of the support code, mirroring the cucumber-wire
 * protocol's read-only requests (`step_matches`, `snippet_text`) plus the
 * messages-era discovery. Pure and deterministic: it owns step-definition
 * **identity** (the ids `step_matches` returns and `invoke` takes), matching,
 * and enumeration. Both the runner (assembly) and the per-scenario object
 * (invoke resolution) consume it; neither reimplements matching.
 *
 * It is deliberately a plain module, not a durable service: matching is pure, it
 * is co-located in the same deployment (no process boundary forcing an RPC), and
 * an in-handler stateless-service call would not be replay-stable anyway.
 */

export interface SupportDescriptor {
  readonly stepDefinitions: ReadonlyArray<{ readonly id: StepDefId; readonly source: string; readonly type: StepDefinitionPatternType }>
  readonly beforeHooks: ReadonlyArray<{ readonly id: string; readonly name?: string; readonly tagExpression?: string }>
  readonly afterHooks: ReadonlyArray<{ readonly id: string; readonly name?: string; readonly tagExpression?: string }>
  readonly parameterTypes: ReadonlyArray<{
    readonly id: string
    readonly name: string
    readonly regularExpressions: ReadonlyArray<string>
    readonly preferForRegexpMatch: boolean
    readonly useForSnippets: boolean
  }>
}

/** A `step_matches` result: the matched step id + args, or undefined/ambiguous. */
export type HostMatch =
  | { readonly _tag: "defined"; readonly stepDefId: StepDefId; readonly arguments: ReadonlyArray<StepMatchArgument> }
  | { readonly _tag: "undefined" }
  | { readonly _tag: "ambiguous"; readonly stepDefIds: ReadonlyArray<StepDefId> }

export interface StepHost {
  /** Enumerate the support code (messages discovery envelopes). */
  readonly describe: () => SupportDescriptor
  /** Match a pickle step's text (the wire `step_matches`). */
  readonly match: (text: string) => HostMatch
  /** Resolve a step id back to its compiled definition (for `invoke`). */
  readonly resolve: (id: StepDefId) => CompiledStep | undefined
}

const hookDescriptor = (prefix: string) => (hook: { readonly name?: string; readonly tags?: string }, index: number) => ({
  id: `${prefix}-${index}`,
  ...(hook.name === undefined ? {} : { name: hook.name }),
  ...(hook.tags === undefined ? {} : { tagExpression: hook.tags }),
})

export const stepHost = (support: SupportBundle): StepHost => {
  // Deterministic, host-owned identity — stable across runner + scenario object
  // without an id table to ship around.
  const stepIds = support.steps.map((_, index) => `sd-${index}`)
  const byId = new Map(stepIds.map((id, index) => [id, support.steps[index]!] as const))

  return {
    describe: () => ({
      stepDefinitions: support.steps.map((step, index) => ({
        id: stepIds[index]!,
        source: step.expression.source,
        type: step.patternType,
      })),
      beforeHooks: support.beforeHooks.map(hookDescriptor("bh")),
      afterHooks: support.afterHooks.map(hookDescriptor("ah")),
      parameterTypes: support.parameterTypes.map((parameterType, index) => ({ id: `pt-${index}`, ...parameterType })),
    }),
    match: (text) => {
      const match = matchStep(support, text)
      switch (match._tag) {
        case "undefined":
          return { _tag: "undefined" }
        case "ambiguous":
          return { _tag: "ambiguous", stepDefIds: match.indices.map((index) => stepIds[index]!) }
        case "defined":
          return { _tag: "defined", stepDefId: stepIds[match.index]!, arguments: match.arguments }
      }
    },
    resolve: (id) => byId.get(id),
  }
}
