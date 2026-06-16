import type { Effect, Layer } from "effect"
import type { ClaimDef, CoverageSpec } from "./runner/coverage.ts"

export type { ClaimDef, CoverageObservations, CoverageSpec } from "./runner/coverage.ts"

export type FirelabClaimResult = void | boolean

export type FirelabGateClaim<C, E = unknown, R = never> =
  (component: C) => FirelabClaimResult | Effect.Effect<FirelabClaimResult, E, R>

export interface FirelabFeatureRef {
  /** The feature.name in features/<product>/<name>.feature.yaml. */
  readonly name: string
  /** The feature.product in the feature yaml. */
  readonly product: string
}

export interface FirelabComponentContext {
  readonly validationId: string
  readonly runId: string
  readonly feature: FirelabFeatureRef
  /** Full requirement id, e.g. `storage-primitives.CHECKPOINT.1`. */
  readonly requirementId: string
  /** Requirement id within the feature, e.g. `CHECKPOINT.1`. */
  readonly requirementLocalId: string
  readonly requirementDescription: string
  /** Fresh, stable key for this validation run + requirement. */
  readonly key: string
  /** Derives additional fresh keys under the same requirement scope. */
  readonly keyFor: (suffix: string) => string
}

export interface FirelabRequirement<C, E = unknown, R = never> {
  /**
   * Requirement id within the feature, e.g. `CHECKPOINT.1` or `ADMISSION.3`.
   * The runner reports it as `<feature.name>.<id>`, matching feature-yaml prose.
   */
  readonly id: string
  readonly description: string
  /** CEL over trace spans. This must name SUT/runtime evidence spans. */
  readonly evidence: string
  /** Self-contained exercise + assertion for this requirement. */
  readonly claim: FirelabGateClaim<C, E, R>
}

export type FirelabComponentFactory<C, E = unknown, R = never> =
  (context: FirelabComponentContext) => Effect.Effect<C, E, R>

export interface FirelabValidationDefinition<C, E = unknown, R = never> {
  readonly id: string
  readonly description: string
  readonly feature: FirelabFeatureRef
  readonly backend?: Layer.Layer<R, E>
  /**
   * Builds the requirement fixture under test. The returned object is inferred
   * and passed to each requirement claim, so scenario helpers like `open`,
   * `reopen`, `client`, or `drain` live here without a separate interface.
   * The runner builds a fresh component for each requirement claim and passes a
   * requirement-scoped fresh key.
   */
  readonly component: FirelabComponentFactory<C, E, R>
  readonly requirements: ReadonlyArray<FirelabRequirement<C, E, R>>
  readonly corroborations?: ReadonlyArray<ClaimDef>
}

declare const FirelabValidationBrand: unique symbol

export type FirelabValidation<C, E = unknown, R = never> =
  FirelabValidationDefinition<C, E, R> & {
    readonly [FirelabValidationBrand]: typeof FirelabValidationBrand
  }

export const defineValidation = <C, E = unknown, R = never>(
  validation: FirelabValidationDefinition<C, E, R>,
): FirelabValidation<C, E, R> =>
  validation as FirelabValidation<C, E, R>

export const requirementId = (
  validation: Pick<FirelabValidationDefinition<unknown>, "feature">,
  id: string,
): string => `${validation.feature.name}.${id}`

export const compileCoverage = (
  validation: FirelabValidation<unknown>,
): CoverageSpec | undefined => {
  if (validation.requirements.length === 0) return undefined
  return {
    gates: validation.requirements.map((requirement): ClaimDef => {
      const id = requirementId(validation, requirement.id)
      return {
        id,
        description: requirement.description,
        claim: `(${requirement.evidence}) && observation("${id}").passed == true`,
        scope: {
          attribute: "firelab.requirement.id",
          value: id,
        },
      }
    }),
    ...(validation.corroborations === undefined ? {} : { corroborations: validation.corroborations }),
  }
}
