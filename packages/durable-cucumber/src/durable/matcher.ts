import type { Argument, Group as ExpressionGroup } from "@cucumber/cucumber-expressions"
import type { Group, StepMatchArgument } from "@cucumber/messages"
import type { SupportBundle } from "./support.ts"

/**
 * Pure step matching over a `SupportBundle`, replacing `@cucumber/core`'s
 * `AssembledTestStep.prepare()`. Mirrors cucumber-js: a step text is matched
 * against every step definition's expression; zero matches is UNDEFINED, more
 * than one is AMBIGUOUS, exactly one is the resolved step + its match arguments.
 */

export type StepMatch =
  | { readonly _tag: "defined"; readonly index: number; readonly arguments: ReadonlyArray<StepMatchArgument> }
  | { readonly _tag: "undefined" }
  | { readonly _tag: "ambiguous"; readonly indices: ReadonlyArray<number>; readonly expressions: ReadonlyArray<string> }

/** Map a cucumber-expressions match group to the canonical messages `Group` (omit absent fields). */
const toGroup = (group: ExpressionGroup): Group => ({
  ...(group.start === undefined ? {} : { start: group.start }),
  ...(group.value === undefined ? {} : { value: group.value }),
  ...(group.children === undefined || group.children.length === 0
    ? {}
    : { children: group.children.map(toGroup) }),
})

const toStepMatchArgument = (argument: Argument): StepMatchArgument => ({
  group: toGroup(argument.group),
  ...(argument.parameterType.name === undefined ? {} : { parameterTypeName: argument.parameterType.name }),
})

/** Match a pickle step's text against the bundle's step definitions. */
export const matchStep = (bundle: SupportBundle, text: string): StepMatch => {
  const matches = bundle.steps.flatMap((step, index) => {
    const args = step.expression.match(text)
    return args === null ? [] : [{ index, args }]
  })
  if (matches.length === 0) return { _tag: "undefined" }
  if (matches.length > 1) {
    return {
      _tag: "ambiguous",
      indices: matches.map((m) => m.index),
      expressions: matches.map((m) => bundle.steps[m.index]!.expression.source),
    }
  }
  const only = matches[0]!
  return { _tag: "defined", index: only.index, arguments: only.args.map(toStepMatchArgument) }
}
