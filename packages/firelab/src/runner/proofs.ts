import { Console, Data, Effect } from "effect"
import { access, mkdir, readdir, readFile, writeFile } from "node:fs/promises"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { parseDocument } from "yaml"
import { analyzeCoverage } from "./coverage.ts"
import { listValidations, selectedValidation } from "./list.ts"
import { compileCoverage, requirementId, type FirelabValidation } from "../types.ts"

const repoRoot = fileURLToPath(new URL("../../../../", import.meta.url))
const featuresRoot = path.join(repoRoot, "features")
const validationsRoot = path.join(repoRoot, "packages/firelab/src/validations")

interface FeatureRequirement {
  readonly localId: string
  readonly fullId: string
  readonly description: string
  readonly group: string
  readonly index: string
}

interface FeatureSpec {
  readonly product: string
  readonly name: string
  readonly description: string
  readonly file: string
  readonly requirements: ReadonlyArray<FeatureRequirement>
}

interface ProofIssue {
  readonly level: "error" | "warning"
  readonly id: string
  readonly message: string
  readonly validationId?: string
}

class ProofCommandError extends Data.TaggedClass("ProofCommandError")<{
  readonly message: string
  readonly cause?: unknown
}> {}

const usage = `
firelab proofs <command>

Commands:
  proofs check [feature|validation-id] [--all] [--all-features] [--allow-missing]
  proofs init <feature> [--dry-run] [--force]
  proofs scaffold <feature|validation-id> [--all]

Examples:
  firelab proofs check effect-s2-stream-db/storage-primitives --allow-missing
  firelab proofs init effect-s2/resource-spec
  firelab proofs check effect-s2-stream-db-storage-primitives
  firelab proofs scaffold effect-s2-stream-db/storage-primitives
`.trim()

const hasFlag = (args: ReadonlyArray<string>, name: string): boolean =>
  args.includes(name)

const positional = (args: ReadonlyArray<string>): ReadonlyArray<string> =>
  args.filter((arg) => !arg.startsWith("--"))

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)

const stringValue = (value: unknown): string | undefined =>
  typeof value === "string" ? value : undefined

const requirementDescription = (value: unknown): string | undefined => {
  if (typeof value === "string") return value
  if (typeof value === "number" || typeof value === "boolean") return String(value)
  return undefined
}

const featureKey = (feature: Pick<FeatureSpec, "product" | "name">): string =>
  `${feature.product}/${feature.name}`

const validationIdFor = (feature: Pick<FeatureSpec, "product" | "name">): string =>
  `${feature.product}-${feature.name}`.replace(/[^A-Za-z0-9_.-]/g, "-").replace(/-+/g, "-")

const validationFeatureKey = (validation: FirelabValidation<unknown>): string =>
  `${validation.feature.product}/${validation.feature.name}`

const walkFeatureFiles = async (dir: string): Promise<ReadonlyArray<string>> => {
  const entries = await readdir(dir, { withFileTypes: true })
  const nested = await Promise.all(entries.map(async (entry) => {
    const full = path.join(dir, entry.name)
    if (entry.isDirectory()) return walkFeatureFiles(full)
    return entry.isFile() && entry.name.endsWith(".feature.yaml") ? [full] : []
  }))
  return nested.flat().sort()
}

const parseFeature = (file: string, source: string): FeatureSpec => {
  const doc = parseDocument(source)
  if (doc.errors.length > 0) {
    const first = doc.errors[0]
    throw new Error(`${file}: ${first?.message ?? "invalid yaml"}`)
  }
  const value = doc.toJS() as unknown
  if (!isRecord(value)) throw new Error(`${file}: expected yaml document object`)
  const feature = value["feature"]
  if (!isRecord(feature)) throw new Error(`${file}: missing feature object`)
  const name = stringValue(feature["name"])
  const product = stringValue(feature["product"])
  const description = stringValue(feature["description"]) ?? ""
  if (name === undefined) throw new Error(`${file}: missing feature.name`)
  if (product === undefined) throw new Error(`${file}: missing feature.product`)

  const requirements: Array<FeatureRequirement> = []
  const collect = (sectionRoot: unknown) => {
    if (!isRecord(sectionRoot)) return
    for (const [group, groupValue] of Object.entries(sectionRoot)) {
      if (!isRecord(groupValue)) continue
      const groupRequirements = groupValue["requirements"]
      if (!isRecord(groupRequirements)) continue
      for (const [index, rawDescription] of Object.entries(groupRequirements)) {
        const reqDescription = requirementDescription(rawDescription)
        if (reqDescription === undefined) {
          throw new Error(`${file}: requirement ${group}.${index} must be a scalar description`)
        }
        const localId = `${group}.${index}`
        requirements.push({
          localId,
          fullId: `${name}.${localId}`,
          description: reqDescription,
          group,
          index,
        })
      }
    }
  }

  collect(value["components"])
  collect(value["constraints"])

  return {
    product,
    name,
    description,
    file: path.relative(repoRoot, file),
    requirements,
  }
}

const loadFeatures = Effect.tryPromise({
  try: async () => {
    const files = await walkFeatureFiles(featuresRoot)
    return Promise.all(files.map(async (file) => parseFeature(file, await readFile(file, "utf8"))))
  },
  catch: (error) =>
    new ProofCommandError({
      message: error instanceof Error ? error.message : String(error),
      cause: error,
    }),
})

const targetMatchesFeature = (target: string, feature: FeatureSpec): boolean => {
  const normalized = target.replace(/\\/g, "/")
  const relative = feature.file.replace(/\\/g, "/")
  return normalized === feature.name ||
    normalized === featureKey(feature) ||
    normalized === relative ||
    normalized === relative.replace(/\.feature\.yaml$/, "")
}

const resolveFeatureTarget = (
  features: ReadonlyArray<FeatureSpec>,
  target: string,
): FeatureSpec | undefined => {
  const matches = features.filter((feature) => targetMatchesFeature(target, feature))
  return matches.length === 1 ? matches[0] : undefined
}

const proofShapeIssues = (
  validation: FirelabValidation<unknown>,
): ReadonlyArray<ProofIssue> => {
  const issues: Array<ProofIssue> = []
  const seen = new Set<string>()
  validation.requirements.forEach((proof) => {
    const id = requirementId(validation, proof.id)
    if (!/^[A-Z][A-Z0-9_]*\.[A-Za-z0-9_-]+$/.test(proof.id)) {
      issues.push({
        level: "error",
        id,
        validationId: validation.id,
        message: "proof id must be a local feature requirement id like GROUP.1",
      })
    }
    if (seen.has(proof.id)) {
      issues.push({
        level: "error",
        id,
        validationId: validation.id,
        message: "duplicate proof id in validation",
      })
    }
    seen.add(proof.id)
    if (proof.description.trim().length === 0) {
      issues.push({
        level: "error",
        id,
        validationId: validation.id,
        message: "proof description is empty",
      })
    }
    if (proof.evidence.trim().length === 0) {
      issues.push({
        level: "error",
        id,
        validationId: validation.id,
        message: "proof evidence CEL is empty",
      })
    }
    if (typeof proof.claim !== "function") {
      issues.push({
        level: "error",
        id,
        validationId: validation.id,
        message: "proof claim must be a function",
      })
    }
  })

  const coverage = compileCoverage(validation)
  if (coverage !== undefined) {
    const report = analyzeCoverage(coverage, [], {})
    report.gates.forEach((gate) => {
      if (gate.error !== undefined) {
        issues.push({
          level: "error",
          id: gate.id,
          validationId: validation.id,
          message: `invalid evidence CEL: ${gate.error}`,
        })
      }
      if (gate.illegal.length > 0) {
        issues.push({
          level: "error",
          id: gate.id,
          validationId: validation.id,
          message: `evidence names non-production spans: ${gate.illegal.join(", ")}`,
        })
      }
      if (gate.refs.length === 0) {
        issues.push({
          level: "error",
          id: gate.id,
          validationId: validation.id,
          message: "evidence must reference at least one named evidence span",
        })
      }
    })
  }

  return issues
}

const featureProofIssues = (
  feature: FeatureSpec,
  validations: ReadonlyArray<FirelabValidation<unknown>>,
  allowMissing: boolean,
): ReadonlyArray<ProofIssue> => {
  const issues: Array<ProofIssue> = []
  const featureIds = new Set(feature.requirements.map((requirement) => requirement.localId))
  const proofs = new Map<string, Array<string>>()

  validations.forEach((validation) => {
    proofShapeIssues(validation).forEach((issue) => issues.push(issue))
    validation.requirements.forEach((proof) => {
      if (!featureIds.has(proof.id)) {
        issues.push({
          level: "error",
          id: requirementId(validation, proof.id),
          validationId: validation.id,
          message: `proof id does not exist in ${featureKey(feature)}`,
        })
      }
      const proofValidations = proofs.get(proof.id) ?? []
      proofValidations.push(validation.id)
      proofs.set(proof.id, proofValidations)
    })
  })

  feature.requirements.forEach((requirement) => {
    if (!proofs.has(requirement.localId)) {
      issues.push({
        level: allowMissing ? "warning" : "error",
        id: requirement.fullId,
        message: `missing proof for ${requirement.description}`,
      })
    }
  })

  return issues
}

const loadTargetValidations = (
  target: string | undefined,
  all: boolean,
  allFeatures: boolean,
) =>
  Effect.gen(function*() {
    const features = yield* loadFeatures
    const validations = yield* listValidations

    if (allFeatures) {
      return features.map((feature) => ({
        feature,
        validations: validations.filter((validation) => validationFeatureKey(validation) === featureKey(feature)),
      }))
    }

    if (all || target === undefined) {
      const keys = [...new Set(validations.map(validationFeatureKey))].sort()
      return keys.flatMap((key) => {
        const feature = features.find((candidate) => featureKey(candidate) === key)
        return feature === undefined
          ? []
          : [{
            feature,
            validations: validations.filter((validation) => validationFeatureKey(validation) === key),
          }]
      })
    }

    const selected = yield* selectedValidation(target).pipe(
      Effect.option,
    )
    if (selected._tag === "Some") {
      const feature = features.find((candidate) => featureKey(candidate) === validationFeatureKey(selected.value))
      if (feature === undefined) {
        return yield* Effect.fail(new ProofCommandError({
          message: `validation ${target} references missing feature ${validationFeatureKey(selected.value)}`,
        }))
      }
      return [{
        feature,
        validations: [selected.value],
      }]
    }

    const feature = resolveFeatureTarget(features, target)
    if (feature === undefined) {
      return yield* Effect.fail(new ProofCommandError({
        message: `unknown feature or validation target: ${target}`,
      }))
    }
    return [{
      feature,
      validations: validations.filter((validation) => validationFeatureKey(validation) === featureKey(feature)),
    }]
  })

const printCheck = (
  feature: FeatureSpec,
  validations: ReadonlyArray<FirelabValidation<unknown>>,
  issues: ReadonlyArray<ProofIssue>,
) =>
  Effect.gen(function*() {
    const errors = issues.filter((issue) => issue.level === "error")
    const warnings = issues.filter((issue) => issue.level === "warning")
    const proofCount = validations.reduce((count, validation) => count + validation.requirements.length, 0)
    const covered = new Set(validations.flatMap((validation) => validation.requirements.map((proof) => proof.id)))
    const knownRequirements = new Set(feature.requirements.map((requirement) => requirement.localId))
    const coveredKnown = [...covered].filter((id) => knownRequirements.has(id)).length

    yield* Console.log(`\n${featureKey(feature)}`)
    yield* Console.log(`  feature: ${feature.file}`)
    yield* Console.log(`  validations: ${validations.length === 0 ? "(none)" : validations.map((validation) => validation.id).join(", ")}`)
    yield* Console.log(`  requirements: ${coveredKnown}/${feature.requirements.length} covered (${proofCount} proof entries)`)
    yield* Console.log(`  issues: ${errors.length} errors, ${warnings.length} warnings`)

    yield* Effect.forEach(issues, (issue) => {
      const marker = issue.level === "error" ? "✗" : "!"
      const origin = issue.validationId === undefined ? "" : ` [${issue.validationId}]`
      return Console.log(`  ${marker} ${issue.id}${origin}: ${issue.message}`)
    })
  })

const jsonString = (value: string): string => JSON.stringify(value)

const errorMessage = (error: unknown): string => {
  if (error instanceof Error) return error.message
  if (isRecord(error) && typeof error["message"] === "string") return error["message"]
  return String(error)
}

const scaffoldEntry = (requirement: FeatureRequirement): string =>
  `    {
      id: ${jsonString(requirement.localId)},
      description: ${jsonString(requirement.description)},
      evidence:
        'spans.exists(s, named(s, "TODO.replace.with.production.span"))',
      claim: (_component) =>
        Effect.gen(function*() {
          // TODO: drive ${requirement.fullId} and assert the observable behavior.
          return false
        }),
    },`

const validationFileFor = (feature: FeatureSpec): string =>
  path.join(validationsRoot, validationIdFor(feature), "index.ts")

const fileExists = (file: string) =>
  Effect.tryPromise({
    try: () => access(file).then(() => true, () => false),
    catch: (error) =>
      new ProofCommandError({
        message: errorMessage(error),
        cause: error,
      }),
  })

const generatedValidation = (feature: FeatureSpec): string =>
  `import { Effect } from "effect"
import { defineValidation } from "../../types.ts"

export default defineValidation({
  id: ${jsonString(validationIdFor(feature))},
  description: ${jsonString(`Validation proofs for ${featureKey(feature)}.`)},
  feature: {
    product: ${jsonString(feature.product)},
    name: ${jsonString(feature.name)},
  },
  component: () =>
    Effect.succeed({
      // TODO: return the handles each requirement claim needs.
    }),
  requirements: [
${feature.requirements.map(scaffoldEntry).join("\n")}
  ],
})
`

const runCheck = (args: ReadonlyArray<string>) =>
  Effect.gen(function*() {
    const target = positional(args)[0]
    const allowMissing = hasFlag(args, "--allow-missing")
    const groups = yield* loadTargetValidations(target, hasFlag(args, "--all"), hasFlag(args, "--all-features"))
    let failed = false
    for (const group of groups) {
      const issues = featureProofIssues(group.feature, group.validations, allowMissing)
      if (issues.some((issue) => issue.level === "error")) failed = true
      yield* printCheck(group.feature, group.validations, issues)
    }
    if (failed) {
      yield* Effect.sync(() => {
        process.exitCode = 1
      })
    }
  })

const runScaffold = (args: ReadonlyArray<string>) =>
  Effect.gen(function*() {
    const target = positional(args)[0]
    if (target === undefined) {
      yield* Console.error(usage)
      yield* Effect.sync(() => {
        process.exitCode = 1
      })
      return
    }
    const groups = yield* loadTargetValidations(target, false, false)
    const group = groups[0]
    if (group === undefined) {
      yield* Console.error(`no feature found for ${target}`)
      yield* Effect.sync(() => {
        process.exitCode = 1
      })
      return
    }
    const existing = new Set(group.validations.flatMap((validation) => validation.requirements.map((proof) => proof.id)))
    const requirements = hasFlag(args, "--all")
      ? group.feature.requirements
      : group.feature.requirements.filter((requirement) => !existing.has(requirement.localId))

    yield* Console.log(`// ${featureKey(group.feature)} proof ${hasFlag(args, "--all") ? "entries" : "stubs for missing requirements"}`)
    yield* Console.log(`// Paste into defineValidation({ requirements: [...] }) and replace TODO evidence/claims.`)
    if (requirements.length === 0) {
      yield* Console.log("// no missing requirements")
      return
    }
    yield* Console.log(requirements.map(scaffoldEntry).join("\n"))
  })

const runInit = (args: ReadonlyArray<string>) =>
  Effect.gen(function*() {
    const target = positional(args)[0]
    if (target === undefined) {
      yield* Console.error(usage)
      yield* Effect.sync(() => {
        process.exitCode = 1
      })
      return
    }

    const features = yield* loadFeatures
    const feature = resolveFeatureTarget(features, target)
    if (feature === undefined) {
      return yield* Effect.fail(new ProofCommandError({
        message: `unknown feature target: ${target}`,
      }))
    }

    const file = validationFileFor(feature)
    const relativeFile = path.relative(repoRoot, file)
    const source = generatedValidation(feature)
    const dryRun = hasFlag(args, "--dry-run")
    const force = hasFlag(args, "--force")

    if (dryRun) {
      yield* Console.log(`// ${relativeFile}`)
      yield* Console.log(source.trimEnd())
      return
    }

    if ((yield* fileExists(file)) && !force) {
      return yield* Effect.fail(new ProofCommandError({
        message: `${relativeFile} already exists; pass --force to overwrite`,
      }))
    }

    yield* Effect.tryPromise({
      try: async () => {
        await mkdir(path.dirname(file), { recursive: true })
        await writeFile(file, source)
      },
      catch: (error) =>
        new ProofCommandError({
          message: errorMessage(error),
          cause: error,
        }),
    })
    yield* Console.log(`created ${relativeFile}`)
    yield* Console.log(`next: replace TODO component/evidence/claims, then run firelab proofs check ${featureKey(feature)} --allow-missing`)
  })

export const proofsCommand = (args: ReadonlyArray<string>) =>
  Effect.gen(function*() {
    const [cmd = "help", ...rest] = args
    switch (cmd) {
      case "check":
        return yield* runCheck(rest)
      case "init":
        return yield* runInit(rest)
      case "scaffold":
        return yield* runScaffold(rest)
      default:
        return yield* Console.log(usage)
    }
  }).pipe(
    Effect.catch((error: unknown) =>
      Console.error(errorMessage(error)).pipe(
        Effect.andThen(Effect.sync(() => {
          process.exitCode = 1
        })),
      ),
    ),
  )
