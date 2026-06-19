import { buildSupportCode, type SupportCodeLibrary } from "@cucumber/core"
import type { NewParameterType, SupportCodeFunction } from "@cucumber/core"
import type { IdGenerator, SourceReference } from "@cucumber/messages"

/**
 * Cucumber-shaped support DSL lowered onto `@cucumber/core`'s `buildSupportCode`.
 *
 * Following the Cucumber wire-protocol model, step/hook bodies live in the
 * step-definition **host** (the `world` object), not in the runner and not in
 * the durable call payloads. A bundle is registered **once at module load**
 * under a name; the durable handlers select it by that serializable name. On a
 * fresh process the bundle re-registers at import, so recovery re-establishes it
 * before any handler is re-driven — the closures themselves never need to be
 * serialized or captured per run.
 */

export type StepBody = SupportCodeFunction

export interface HookOptions {
  readonly name?: string
  readonly tags?: string
}

export interface SupportApi {
  readonly Given: (pattern: string | RegExp, body: StepBody) => void
  readonly When: (pattern: string | RegExp, body: StepBody) => void
  readonly Then: (pattern: string | RegExp, body: StepBody) => void
  readonly Before: (optionsOrBody: HookOptions | StepBody, body?: StepBody) => void
  readonly After: (optionsOrBody: HookOptions | StepBody, body?: StepBody) => void
  readonly BeforeAll: (body: StepBody) => void
  readonly AfterAll: (body: StepBody) => void
  readonly ParameterType: (options: Omit<NewParameterType, "sourceReference">) => void
}

export type SupportModule = (api: SupportApi) => void

// Bundles registered at module load, addressed by name across the durable
// boundary. Not durable-authority state (it is code, re-derived from imports on
// every boot), so it is exempt from the durable-runtime registry guardrail.
const supportBundles = new Map<string, SupportModule>()

/** Register a named support bundle and return its name (for `runFeatures`). */
export const defineSupport = (name: string, register: SupportModule): string => {
  supportBundles.set(name, register)
  return name
}

const sourceRef = (): SourceReference => ({ uri: "cucumber-effect/support", location: { line: 0 } })

const normalizeHook = (
  optionsOrBody: HookOptions | StepBody,
  body: StepBody | undefined,
): { readonly options: HookOptions; readonly fn: StepBody } =>
  typeof optionsOrBody === "function"
    ? { options: {}, fn: optionsOrBody }
    : { options: optionsOrBody, fn: body as StepBody }

/** Build the `@cucumber/core` support library for a registered bundle. */
export const buildSupportLibrary = (bundleName: string, newId: IdGenerator.NewId): SupportCodeLibrary => {
  const module = supportBundles.get(bundleName)
  // A missing bundle is a wiring/programmer error (the host process must register
  // its support at module load) — surface it as a defect.
  if (module === undefined) throw new Error(`no support bundle registered under ${JSON.stringify(bundleName)}`)

  const builder = buildSupportCode({ newId })
  const step = (pattern: string | RegExp, fn: StepBody): void => {
    builder.step({ pattern, fn, sourceReference: sourceRef() })
  }
  const api: SupportApi = {
    Given: step,
    When: step,
    Then: step,
    Before: (optionsOrBody, body) => {
      const { options, fn } = normalizeHook(optionsOrBody, body)
      builder.beforeHook({ ...options, fn, sourceReference: sourceRef() })
    },
    After: (optionsOrBody, body) => {
      const { options, fn } = normalizeHook(optionsOrBody, body)
      builder.afterHook({ ...options, fn, sourceReference: sourceRef() })
    },
    BeforeAll: (fn) => builder.beforeAllHook({ fn, sourceReference: sourceRef() }),
    AfterAll: (fn) => builder.afterAllHook({ fn, sourceReference: sourceRef() }),
    ParameterType: (options) => builder.parameterType({ ...options, sourceReference: sourceRef() }),
  }
  module(api)
  return builder.build()
}
