import {
  CucumberExpression,
  type Expression,
  ParameterType,
  ParameterTypeRegistry,
  RegularExpression,
} from "@cucumber/cucumber-expressions"
import { StepDefinitionPatternType } from "@cucumber/messages"

/**
 * The step-definition DSL — the glue binding Gherkin step text to executable
 * bodies, plus hooks and parameter types. This is the only Cucumber-specific
 * authoring surface, modelled on cucumber-js's `Given/When/Then` methods but
 * **without** its global singleton builder.
 *
 * `defineSteps(register)` returns a plain `SupportBundle` **value**: the
 * compiled step expressions (via `@cucumber/cucumber-expressions`, the same lib
 * cucumber-js uses) + hooks + parameter types. That value is captured in the
 * durable `runner`/`world` handler closures at definition time and registered
 * in the run's layer — a dependency of the deployment, the way Restate handlers
 * carry their dependencies, not a module-global mutated by import side effects.
 * On recovery the layer is rebuilt from the same bundle, re-establishing the
 * closures exactly like the handler code itself.
 */

// eslint-disable-next-line @typescript-eslint/no-explicit-any -- step bodies are classic Cucumber callbacks: variadic args + an existential `this` (base World, or a harness-specific extension like SpecWorld).
export type StepBody = (this: any, ...args: ReadonlyArray<any>) => unknown

/** The scenario World handed to a step/hook body as `this` (attach/log/link). */
export interface World {
  attach(data: unknown, options?: string | { mediaType: string; fileName?: string }): Promise<void>
  log(text: string): Promise<void>
  link(uri: string): Promise<void>
}

export interface HookOptions {
  readonly name?: string
  readonly tags?: string
}

export interface ParameterTypeOptions {
  readonly name: string
  readonly regexp: string | RegExp | ReadonlyArray<string | RegExp>
  readonly transformer?: (...match: ReadonlyArray<string>) => unknown
  readonly useForSnippets?: boolean
  readonly preferForRegexpMatch?: boolean
}

/** The authoring surface passed to a `defineSteps` callback. */
export interface SupportApi {
  readonly Given: (pattern: string | RegExp, body: StepBody) => void
  readonly When: (pattern: string | RegExp, body: StepBody) => void
  readonly Then: (pattern: string | RegExp, body: StepBody) => void
  readonly Before: (optionsOrBody: HookOptions | StepBody, body?: StepBody) => void
  readonly After: (optionsOrBody: HookOptions | StepBody, body?: StepBody) => void
  readonly BeforeAll: (body: StepBody) => void
  readonly AfterAll: (body: StepBody) => void
  readonly ParameterType: (options: ParameterTypeOptions) => void
}

/** A compiled step definition: its matchable expression + body + pattern kind (for the envelope). */
export interface CompiledStep {
  readonly expression: Expression
  readonly patternType: StepDefinitionPatternType
  readonly fn: StepBody
}

/** A compiled scenario hook (Before/After), with an optional tag filter. */
export interface CompiledHook {
  readonly name?: string
  readonly tags?: string
  readonly fn: StepBody
}

/** A user-defined parameter type, retained for `parameterType` envelope emission. */
export interface CompiledParameterType {
  readonly name: string
  readonly regularExpressions: ReadonlyArray<string>
  readonly preferForRegexpMatch: boolean
  readonly useForSnippets: boolean
}

/**
 * The finalized support code as a serializable-shaped value (closures aside).
 * Captured in the durable handler closures; never looked up from a global.
 */
export interface SupportBundle {
  readonly steps: ReadonlyArray<CompiledStep>
  readonly beforeHooks: ReadonlyArray<CompiledHook>
  readonly afterHooks: ReadonlyArray<CompiledHook>
  readonly beforeAll: ReadonlyArray<StepBody>
  readonly afterAll: ReadonlyArray<StepBody>
  readonly parameterTypes: ReadonlyArray<CompiledParameterType>
  readonly registry: ParameterTypeRegistry
}

const normalizeHook = (
  optionsOrBody: HookOptions | StepBody,
  body: StepBody | undefined,
): CompiledHook =>
  typeof optionsOrBody === "function"
    ? { fn: optionsOrBody }
    : { ...optionsOrBody, fn: body as StepBody }

/**
 * Build a support bundle from a registration callback. Pure and deterministic:
 * parameter types are collected first and registered into the expression
 * registry, then step patterns are compiled against it — so a step may use a
 * parameter type declared anywhere in the callback (cucumber-js's collect-then-
 * finalize order).
 */
export const defineSteps = (register: (api: SupportApi) => void): SupportBundle => {
  const rawSteps: Array<{ readonly pattern: string | RegExp; readonly fn: StepBody }> = []
  const beforeHooks: Array<CompiledHook> = []
  const afterHooks: Array<CompiledHook> = []
  const beforeAll: Array<StepBody> = []
  const afterAll: Array<StepBody> = []
  const rawParameterTypes: Array<ParameterTypeOptions> = []

  const step = (pattern: string | RegExp, fn: StepBody): void => {
    rawSteps.push({ pattern, fn })
  }
  register({
    Given: step,
    When: step,
    Then: step,
    Before: (optionsOrBody, body) => beforeHooks.push(normalizeHook(optionsOrBody, body)),
    After: (optionsOrBody, body) => afterHooks.push(normalizeHook(optionsOrBody, body)),
    BeforeAll: (fn) => beforeAll.push(fn),
    AfterAll: (fn) => afterAll.push(fn),
    ParameterType: (options) => rawParameterTypes.push(options),
  })

  const registry = new ParameterTypeRegistry()
  const parameterTypes = rawParameterTypes.map((options): CompiledParameterType => {
    const useForSnippets = options.useForSnippets !== false
    const preferForRegexpMatch = options.preferForRegexpMatch === true
    const parameterType = new ParameterType(
      options.name,
      options.regexp,
      null,
      options.transformer ?? ((value: string) => value),
      useForSnippets,
      preferForRegexpMatch,
    )
    registry.defineParameterType(parameterType)
    return { name: options.name, regularExpressions: parameterType.regexpStrings, preferForRegexpMatch, useForSnippets }
  })

  const steps = rawSteps.map((raw): CompiledStep =>
    typeof raw.pattern === "string"
      ? {
        expression: new CucumberExpression(raw.pattern, registry),
        patternType: StepDefinitionPatternType.CUCUMBER_EXPRESSION,
        fn: raw.fn,
      }
      : {
        expression: new RegularExpression(raw.pattern, registry),
        patternType: StepDefinitionPatternType.REGULAR_EXPRESSION,
        fn: raw.fn,
      },
  )

  return { steps, beforeHooks, afterHooks, beforeAll, afterAll, parameterTypes, registry }
}
