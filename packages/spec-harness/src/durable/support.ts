import { buildSupportCode, type SupportCodeLibrary } from "@cucumber/core"
import type { NewParameterType, SupportCodeFunction } from "@cucumber/core"
import type { IdGenerator, SourceReference } from "@cucumber/messages"

/**
 * Cucumber-shaped support DSL lowered onto `@cucumber/core`'s
 * {@link buildSupportCode} builder. `@cucumber/core` owns matching, the
 * `PreparedStep` structure, and the support-code envelopes; this module only
 * adapts the ergonomic `Given/When/Then/Before/After/...` surface onto it.
 *
 * Durability is NOT implemented here. A step body becomes durable by running
 * inside the durable worker object and reaching for `effect-s2-durable`
 * primitives (`run`, `state`, `signal`, ...) — see {@link ../durable/worker.ts}.
 */

/** A user-authored step or hook body. May be sync, return a Promise, or return an Effect. */
export type StepBody = SupportCodeFunction

export interface HookOptions {
  readonly name?: string
  readonly tags?: string
}

/** The Cucumber-shaped authoring surface handed to a {@link SupportModule}. */
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

/**
 * A unit of support code. It is a plain function over {@link SupportApi}, so
 * its step bodies are ordinary closures — they are NEVER serialized across the
 * durable call boundary. The coordinator and worker both close over the same
 * module and rebuild the support library deterministically.
 */
export type SupportModule = (api: SupportApi) => void

/**
 * Declare a unit of support code. This is an identity helper that exists for
 * ergonomics and type inference — `defineSupport(({ Given }) => { ... })`.
 */
export const defineSupport = (register: SupportModule): SupportModule => register

const sourceRef = (): SourceReference => ({ uri: "cucumber-effect/support", location: { line: 0 } })

const normalizeHook = (
  optionsOrBody: HookOptions | StepBody,
  body: StepBody | undefined,
): { readonly options: HookOptions; readonly fn: StepBody } =>
  typeof optionsOrBody === "function"
    ? { options: {}, fn: optionsOrBody }
    : { options: optionsOrBody, fn: body as StepBody }

/**
 * Build and seal a {@link SupportCodeLibrary} from a support module. `newId`
 * is threaded so the support-code envelope ids line up with the surrounding
 * discovery/test-plan ids in a single deterministic sequence.
 */
export const buildSupportLibrary = (
  module: SupportModule,
  newId: IdGenerator.NewId,
): SupportCodeLibrary => {
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
    BeforeAll: (fn) => {
      builder.beforeAllHook({ fn, sourceReference: sourceRef() })
    },
    AfterAll: (fn) => {
      builder.afterAllHook({ fn, sourceReference: sourceRef() })
    },
    ParameterType: (options) => {
      builder.parameterType({ ...options, sourceReference: sourceRef() })
    },
  }

  module(api)
  return builder.build()
}
