import js from "@eslint/js"
import stylistic from "@stylistic/eslint-plugin"
import globals from "globals"
import tseslint from "typescript-eslint"

const sourceFiles = [
  "src/**/*.ts",
  "src/**/*.tsx",
  "packages/**/*.ts",
  "packages/**/*.tsx",
  "apps/**/*.ts",
  "apps/**/*.tsx",
]

const packageSourceFiles = [
  "packages/**/src/**/*.ts",
  "packages/**/src/**/*.tsx",
]

const testFiles = [
  "packages/**/test/**/*.ts",
  "packages/**/src/__tests__/**/*.ts",
  "packages/**/*.test.ts",
  "packages/**/*.test.tsx",
  "apps/**/test/**/*.ts",
  "apps/**/src/__tests__/**/*.ts",
  "apps/**/*.test.ts",
  "apps/**/*.test.tsx",
]

const riskyEffectRuntimeCalls = [
  {
    selector: "CallExpression[callee.object.name='Effect'][callee.property.name='runPromise']",
    message: "Effect.runPromise belongs at runtime boundaries or in tests; keep library code Effect-native.",
  },
  {
    selector: "CallExpression[callee.object.name='Effect'][callee.property.name='runPromiseExit']",
    message: "Effect.runPromiseExit belongs at runtime boundaries or in tests; keep library code Effect-native.",
  },
  {
    selector: "CallExpression[callee.object.name='Effect'][callee.property.name='runSync']",
    message: "Effect.runSync belongs at explicit runtime boundaries; keep library code Effect-native.",
  },
  {
    selector: "CallExpression[callee.object.name='Effect'][callee.property.name='runFork']",
    message: "Effect.runFork creates an unmanaged fiber unless it is scoped very deliberately.",
  },
]

const effectDebtGuardrails = [
  {
    selector: "CallExpression[callee.object.name='Effect'][callee.property.name='orDie']",
    message: "Effect.orDie collapses typed errors into defects; prefer typed errors unless this is a documented crash boundary.",
  },
  {
    selector: "CallExpression[callee.object.name='Layer'][callee.property.name='orDie']",
    message: "Layer.orDie collapses acquisition errors into defects; prefer typed errors unless this is a documented crash boundary.",
  },
  {
    selector: "CallExpression[callee.object.name='Effect'][callee.property.name='die']",
    message: "Effect.die should represent an unexpected defect, not ordinary domain failure.",
  },
]

const tsOnly = (configs) =>
  configs.map((config) => ({
    ...config,
    files: sourceFiles,
  }))
const relativeJsSpecifierPattern = /^\.{1,2}\/.*\.js$/u
const rewriteJsSpecifierToTs = (specifier) => specifier.replace(/\.js$/u, ".ts")
const durableAuthorityNamePattern =
  /(?:cache|registry|registries|runs|claims|completions|pending|subscribers|eventplanes?|eventPlane)/u
const hostAuthorityRegistryNamePattern =
  /(?:(?:run|completion|claim|eventPlane).*(?:cache|registry)|(?:cache|registry).*(?:run|completion|claim|eventPlane))/u
const pollingAllowComment = "durable-lint-allow-polling"
const cacheAllowComment = "durable-lint-allow-cache"
// C2 / WORKFLOW_ADMISSION: every production `Workflow.make` is an owned durable
// workflow that must be SDD-justified in docs/workflow-make-admission-ledger.md.
// The admission comment is the per-site gate (replaces the retired count ratchet).
const workflowMakeAdmissionComment = "workflow-make-admission"

const getStaticPropertyName = (node) => {
  if (node?.type !== "MemberExpression" || node.computed) {
    return undefined
  }

  return node.property.type === "Identifier" ? node.property.name : undefined
}

const getCallMember = (node) => {
  if (node?.type !== "CallExpression" || node.callee.type !== "MemberExpression") {
    return undefined
  }

  const objectName = node.callee.object.type === "Identifier" ? node.callee.object.name : undefined
  const propertyName = getStaticPropertyName(node.callee)
  return objectName != null && propertyName != null ? { objectName, propertyName } : undefined
}

const isFixedDurationExpression = (node) => {
  if (node == null) {
    return false
  }

  if (node.type === "Literal") {
    return true
  }

  if (node.type === "TemplateLiteral" && node.expressions.length === 0) {
    return true
  }

  const member = getCallMember(node)
  return member?.objectName === "Duration" && node.arguments[0]?.type === "Literal"
}

const isNewOf = (node, names) =>
  node?.type === "NewExpression" && node.callee.type === "Identifier" && names.has(node.callee.name)

const isTopLevelDeclaration = (node) => {
  const parent = node.parent
  const grandparent = parent?.parent
  return parent?.type === "Program" || grandparent?.type === "Program"
}

// Walk ancestors for an enclosing `Object.method(...)` call (e.g. inside
// `Effect.sync(() => …)` / `Effect.gen(…)`) — the ESLint-AST analogue of the
// retired ratchet's `isInsideMemberCall` pattern-inside check.
const hasMemberCallAncestor = (node, objectName, propertyName) => {
  let current = node.parent
  while (current != null) {
    if (
      current.type === "CallExpression" &&
      current.callee?.type === "MemberExpression" &&
      !current.callee.computed &&
      current.callee.object?.type === "Identifier" &&
      current.callee.object.name === objectName &&
      current.callee.property?.type === "Identifier" &&
      current.callee.property.name === propertyName
    ) {
      return true
    }
    current = current.parent
  }
  return false
}

const hasLoopAncestor = (node) => {
  let current = node.parent

  while (current != null) {
    if (
      current.type === "WhileStatement" ||
      current.type === "DoWhileStatement" ||
      current.type === "ForStatement" ||
      current.type === "ForInStatement" ||
      current.type === "ForOfStatement"
    ) {
      return true
    }

    current = current.parent
  }

  return false
}

const buildScanFlags = (flags) => Array.from(new Set(`${flags ?? ""}g`)).join("")
const makeSourceRegexBanRule = () => ({
  meta: {
    type: "problem",
    docs: {
      description: "Disallow source-text shapes ported from the retired Semgrep ruleset.",
    },
    schema: [
      {
        type: "array",
        items: {
          type: "object",
          properties: {
            pattern: { type: "string" },
            flags: { type: "string" },
            message: { type: "string" },
          },
          required: ["pattern", "message"],
          additionalProperties: false,
        },
      },
    ],
    messages: { banned: "{{message}}" },
  },
  create(context) {
    const entries = context.options[0] ?? []
    return {
      "Program:exit"() {
        const text = context.sourceCode.getText()
        for (const { pattern, flags, message } of entries) {
          const re = new RegExp(pattern, buildScanFlags(flags))
          let match
          while ((match = re.exec(text)) !== null) {
            context.report({
              loc: {
                start: context.sourceCode.getLocFromIndex(match.index),
                end: context.sourceCode.getLocFromIndex(match.index + match[0].length),
              },
              messageId: "banned",
              data: { message },
            })
            if (match.index === re.lastIndex) {
              re.lastIndex += 1
            }
          }
        }
      },
    }
  },
})
const sourceRegexBanRule = makeSourceRegexBanRule()

const hrtimeTupleProperties = new Set(["startTime", "endTime", "duration"])
const isHrtimeTupleIndexZero = (node) =>
  node?.type === "MemberExpression" &&
  node.computed &&
  node.property?.type === "Literal" &&
  node.property.value === 0 &&
  node.object?.type === "MemberExpression" &&
  !node.object.computed &&
  node.object.property?.type === "Identifier" &&
  hrtimeTupleProperties.has(node.object.property.name)

const hasNearbyAllowComment = (context, node, allowedTags) => {
  if (node?.loc == null) {
    return false
  }

  return context.sourceCode
    .getAllComments()
    .some(
      (comment) =>
        allowedTags.some((tag) => comment.value.includes(tag)) &&
        comment.loc.end.line >= node.loc.start.line - 2 &&
        comment.loc.end.line <= node.loc.start.line,
    )
}

const local = {
  rules: {
    "relative-ts-extensions": {
      meta: {
        type: "problem",
        fixable: "code",
        docs: {
          description: "Require relative TypeScript source imports to use .ts extensions.",
        },
        schema: [],
        messages: {
          useTsExtension:
            "Use a .ts extension for relative source imports; TypeScript rewrites these to .js during build.",
        },
      },
      create(context) {
        const report = (node) => {
          if (node == null || typeof node.value !== "string" || !relativeJsSpecifierPattern.test(node.value)) {
            return
          }

          context.report({
            node,
            messageId: "useTsExtension",
            fix(fixer) {
              const raw = context.sourceCode.getText(node)
              const quote = raw[0] === "'" ? "'" : "\""
              return fixer.replaceText(node, `${quote}${rewriteJsSpecifierToTs(node.value)}${quote}`)
            },
          })
        }

        return {
          ImportDeclaration(node) {
            report(node.source)
          },
          ExportAllDeclaration(node) {
            report(node.source)
          },
          ExportNamedDeclaration(node) {
            report(node.source)
          },
          ImportExpression(node) {
            report(node.source)
          },
          TSImportType(node) {
            report(node.argument)
          },
        }
      },
    },
    "no-literal-dynamic-import": {
      meta: {
        type: "problem",
        docs: {
          description:
            "Disallow dynamic import() with a literal specifier; import modules known at author time statically.",
        },
        schema: [],
        messages: {
          noLiteralDynamicImport:
            "Do not use dynamic import() with a literal specifier — import the module statically. Dynamic literal imports defeat the dead-code import-graph gate (tf-uc8u); they silently shielded permissionDecisionDeferred and sseStream as false-positive DEAD flags. Computed import(expr) for genuine runtime module loading (e.g. plugin loaders) is allowed.",
        },
      },
      create(context) {
        return {
          ImportExpression(node) {
            if (node.source.type === "Literal") {
              context.report({ node, messageId: "noLiteralDynamicImport" })
            }
          },
        }
      },
    },
    "no-fixed-polling": {
      meta: {
        type: "problem",
        docs: {
          description: "Disallow fixed polling primitives and sleep-in-loop scans in production durable runtime code.",
        },
        schema: [],
        messages: {
          noPolling:
            "Avoid fixed polling in durable runtime code; drive work from durable subscriptions, state folds, or explicit deadlines.",
        },
      },
      create(context) {
        const bannedMembers = new Map([
          ["Schedule", new Set(["fixed", "recurs", "spaced"])],
          ["Stream", new Set(["tick"])],
        ])

        return {
          CallExpression(node) {
            const member = getCallMember(node)
            if (member == null || hasNearbyAllowComment(context, node, [pollingAllowComment])) {
              return
            }

            if (bannedMembers.get(member.objectName)?.has(member.propertyName)) {
              context.report({ node, messageId: "noPolling" })
              return
            }

            if (
              member.objectName === "Effect" &&
              member.propertyName === "sleep" &&
              hasLoopAncestor(node) &&
              isFixedDurationExpression(node.arguments[0])
            ) {
              context.report({ node, messageId: "noPolling" })
            }
          },
        }
      },
    },
    "no-module-durable-cache": {
      meta: {
        type: "problem",
        docs: {
          description: "Disallow module-scope mutable durable-state caches in production code.",
        },
        schema: [],
        messages: {
          noTopLevelLet:
            "Avoid module-scope mutable state in durable runtime code; derive restart state from Durable Streams/State.",
          noDurableCache:
            "Avoid module-scope durable-authority caches or registries; durable state must remain the source of truth.",
        },
      },
      create(context) {
        const cacheConstructors = new Set(["Map", "Set", "WeakMap", "WeakSet"])
        const isCacheInitializer = (node) =>
          isNewOf(node, cacheConstructors) || node?.type === "ArrayExpression" || node?.type === "ObjectExpression"

        return {
          VariableDeclaration(node) {
            if (!isTopLevelDeclaration(node) || hasNearbyAllowComment(context, node, [cacheAllowComment])) {
              return
            }

            if (node.kind === "let") {
              context.report({ node, messageId: "noTopLevelLet" })
              return
            }

            for (const declarator of node.declarations) {
              const name = declarator.id.type === "Identifier" ? declarator.id.name : undefined
              if (
                name != null &&
                durableAuthorityNamePattern.test(name) &&
                isCacheInitializer(declarator.init)
              ) {
                context.report({ node: declarator, messageId: "noDurableCache" })
              }
            }
          },
        }
      },
    },
    "no-host-authority-registry": {
      meta: {
        type: "problem",
        docs: {
          description: "Disallow host-owned run/completion/claim/event-plane registry names.",
        },
        schema: [],
        messages: {
          noRegistry:
            "Host code must not define durable-authority registries/caches for runs, completions, claims, or event planes.",
        },
      },
      create(context) {
        const reportName = (node, name) => {
          if (
            typeof name === "string" &&
            hostAuthorityRegistryNamePattern.test(name) &&
            !hasNearbyAllowComment(context, node, [cacheAllowComment])
          ) {
            context.report({ node, messageId: "noRegistry" })
          }
        }

        return {
          VariableDeclarator(node) {
            reportName(node, node.id.type === "Identifier" ? node.id.name : undefined)
          },
          FunctionDeclaration(node) {
            reportName(node, node.id?.name)
          },
          ClassDeclaration(node) {
            reportName(node, node.id?.name)
          },
        }
      },
    },
    // Relocated from the effect-quality ratchet (`newDurableStreamSiteCount`).
    // Direct `new DurableStream(...)` bypasses the DurableTable / declared-service
    // boundary; construct streams through the supported factories.
    "no-new-durable-stream": {
      meta: {
        type: "problem",
        docs: { description: "Disallow direct `new DurableStream(...)`; use the supported factories." },
        schema: [],
        messages: {
          noNewDurableStream:
            "Do not construct `new DurableStream(...)` directly; go through DurableTable / the declared stream factories.",
        },
      },
      create(context) {
        return {
          NewExpression(node) {
            if (node.callee?.type === "Identifier" && node.callee.name === "DurableStream") {
              context.report({ node, messageId: "noNewDurableStream" })
            }
          },
        }
      },
    },
    // Relocated from the effect-quality ratchet (`forOfInPackageSourceCount`).
    // Effect-native source prefers Array/Stream/Chunk combinators over imperative
    // `for…of` iteration.
    "no-for-of-in-source": {
      meta: {
        type: "problem",
        docs: { description: "Disallow imperative for…of in library source; use Array/Stream/Chunk combinators." },
        schema: [],
        messages: {
          noForOf:
            "Avoid imperative `for…of` in library source; use Array/Stream/Chunk combinators (Effect-native iteration).",
        },
      },
      create(context) {
        return {
          ForOfStatement(node) {
            context.report({ node, messageId: "noForOf" })
          },
        }
      },
    },
    // Relocated from the effect-quality ratchet (`anyNoContextCastCount`). Casting
    // to `Schema.Schema.AnyNoContext` launders away the schema's real context.
    "no-any-no-context-cast": {
      meta: {
        type: "problem",
        docs: { description: "Disallow `as …Schema.AnyNoContext` casts (type laundering)." },
        schema: [],
        messages: {
          noAnyNoContextCast:
            "Do not cast to `Schema.Schema.AnyNoContext`; carry the schema's real context type instead of laundering it.",
        },
      },
      create(context) {
        return {
          TSAsExpression(node) {
            const text = context.sourceCode.getText(node.typeAnnotation)
            if (text.includes("Schema.Schema.AnyNoContext")) {
              context.report({ node, messageId: "noAnyNoContextCast" })
            }
          },
        }
      },
    },
    // Relocated from the effect-quality ratchet (`detachedPromiseInEffectSyncCount`,
    // STRICT_ZERO). A `void <promise>.then(...)` inside `Effect.sync(...)` detaches
    // an unmanaged promise from the Effect runtime (no interruption / error
    // propagation). Ancestor-walking (the ratchet's pattern-inside semantics).
    "no-detached-promise-in-effect-sync": {
      meta: {
        type: "problem",
        docs: { description: "Disallow `void <promise>.then(...)` inside Effect.sync (detached unmanaged promise)." },
        schema: [],
        messages: {
          noDetachedPromise:
            "Detached `void <promise>.then(...)` inside Effect.sync escapes the Effect runtime. Model the async work as an Effect (Effect.promise / Effect.tryPromise) and fork it with the runtime instead.",
        },
      },
      create(context) {
        return {
          UnaryExpression(node) {
            if (node.operator !== "void") return
            let arg = node.argument
            if (
              arg?.type === "CallExpression" &&
              arg.callee?.type === "MemberExpression" &&
              !arg.callee.computed &&
              arg.callee.property?.type === "Identifier" &&
              arg.callee.property.name === "catch" &&
              arg.callee.object?.type === "CallExpression"
            ) {
              arg = arg.callee.object
            }
            const isThenCall =
              arg?.type === "CallExpression" &&
              arg.callee?.type === "MemberExpression" &&
              !arg.callee.computed &&
              arg.callee.property?.type === "Identifier" &&
              arg.callee.property.name === "then"
            if (isThenCall && hasMemberCallAncestor(node, "Effect", "sync")) {
              context.report({ node, messageId: "noDetachedPromise" })
            }
          },
        }
      },
    },
    // Re-homed C2 / WORKFLOW_ADMISSION guard from the retired effect-quality
    // ratchet (`workflowMakeSiteCount`). Was a grandfathered COUNT (fail-on-
    // increase) — now a per-site annotation gate: every production `Workflow.make`
    // must carry a nearby `// workflow-make-admission` comment, forcing a net-new
    // owner workflow to add its ledger justification. Pins the finding to
    // path+line (which the count ratchet could not). See
    // docs/workflow-make-admission-ledger.md.
    "no-unclassified-workflow-make": {
      meta: {
        type: "problem",
        docs: { description: "Require a workflow-make-admission ledger annotation on every production Workflow.make." },
        schema: [],
        messages: {
          noUnclassified:
            "Net-new `Workflow.make` is an owned durable workflow (C2 / WORKFLOW_ADMISSION). SDD-justify it in docs/workflow-make-admission-ledger.md and annotate this site with `// workflow-make-admission`.",
        },
      },
      create(context) {
        return {
          CallExpression(node) {
            const callee = node.callee
            if (
              callee.type === "MemberExpression" &&
              !callee.computed &&
              callee.object?.type === "Identifier" &&
              callee.object.name === "Workflow" &&
              callee.property?.type === "Identifier" &&
              callee.property.name === "make" &&
              !hasNearbyAllowComment(context, node, [workflowMakeAdmissionComment])
            ) {
              context.report({ node, messageId: "noUnclassified" })
            }
          },
        }
      },
    },
    // firegrid-remediation-hardening.STATIC_QUALITY.14 (relocated from ast-grep)
    "hrtime-number-arithmetic": {
      meta: {
        type: "problem",
        docs: {
          description:
            "Disallow OpenTelemetry hrtime tuple arithmetic in number space; use the nsFromHrTime / startNs / endNs bigint helpers.",
        },
        schema: [],
        messages: {
          noHrtimeMath:
            "hrtime tuple arithmetic in number space — use nsFromHrTime / startNs / endNs (precision loss above ~26h).",
        },
      },
      create(context) {
        return {
          BinaryExpression(node) {
            if (node.operator === "*" && isHrtimeTupleIndexZero(node.left)) {
              context.report({ node, messageId: "noHrtimeMath" })
            }
          },
        }
      },
    },
    // tf-0awo.21 §6: ban the `as unknown as T` double-launder cast (matches the
    // inner `… as unknown`). A distinct rule id (not no-restricted-syntax) so a
    // tier-scoped enabling block never clobbers the shared no-restricted-syntax
    // config via ESLint's per-rule last-wins merge.
    "no-launder-cast": {
      meta: {
        type: "problem",
        docs: {
          description:
            "Disallow `as unknown as T` casts; drive residual requirements to never and orDie infra errors at their boundary so composition is launchable by construction.",
        },
        schema: [],
        messages: {
          noLaunderCast:
            "Do not launder types through `as unknown as`. Make the composition type-correct (drive residual R to `never`, orDie infra errors at their boundary) instead of asserting the shape (tf-0awo.21 §6).",
        },
      },
      create(context) {
        return {
          // Match the INNER `x as unknown` of a double-launder `x as unknown as T`
          // (its parent is the outer `as T` expression). A bare `x as unknown`
          // (e.g. narrowing `any` → `unknown`) is legitimate and not flagged.
          TSAsExpression(node) {
            if (
              node.typeAnnotation?.type === "TSUnknownKeyword" &&
              node.parent?.type === "TSAsExpression"
            ) {
              context.report({ node, messageId: "noLaunderCast" })
            }
          },
        }
      },
    },
    "sg-no-random-durable-identity": sourceRegexBanRule,
    "sg-no-inline-tagged-error-fail": sourceRegexBanRule,
    "sg-no-mutable-identity-let": sourceRegexBanRule,
    "sg-match-should-be-exhaustive": sourceRegexBanRule,
  },
}

// Test-file ignores shared by the ported-Semgrep blocks (the original rules all
// excluded `**/*.test.{ts,tsx}` and `**/__tests__/**`).
const portedSemgrepTestIgnores = [
  ...testFiles,
]


export default tseslint.config(
  {
    ignores: [
      "node_modules/**",
      "coverage/**",
      "**/dist/**",
      ".eslintcache",
      "*.tsbuildinfo",
      // repos/** contains pinned git submodules used as read-only design references.
      // They are not part of this repo's build graph.
      "repos/**",
      // Firelab is retained as a legacy verification shell. It will be rebuilt
      // on the fluent modules before returning to blocking lint/typecheck.
      "packages/firelab/**",
    ],
  },
  {
    linterOptions: {
      reportUnusedDisableDirectives: "error",
      reportUnusedInlineConfigs: "error",
    },
  },
  js.configs.recommended,
  ...tsOnly(tseslint.configs.recommendedTypeChecked),
  {
    files: ["**/*.js"],
    languageOptions: {
      globals: globals.node,
    },
  },
  {
    files: sourceFiles,
    languageOptions: {
      globals: { ...globals.node, ...globals.browser },
      parserOptions: {
        project: "./tsconfig.eslint.json",
        tsconfigRootDir: import.meta.dirname,
      },
    },
    plugins: {
      "@stylistic": stylistic,
      local,
    },
    rules: {
      "@typescript-eslint/consistent-type-imports": [
        "error",
        {
          fixStyle: "inline-type-imports",
        },
      ],
      "@typescript-eslint/no-floating-promises": "error",
      "@typescript-eslint/no-confusing-void-expression": "off",
      "@typescript-eslint/no-misused-promises": [
        "error",
        {
          checksVoidReturn: false,
        },
      ],
      "@typescript-eslint/no-unnecessary-type-assertion": "error",
      "@typescript-eslint/no-unused-vars": [
        "error",
        {
          argsIgnorePattern: "^_",
          caughtErrorsIgnorePattern: "^_",
          varsIgnorePattern: "^_",
        },
      ],
      "@typescript-eslint/require-await": "off",
      "@typescript-eslint/restrict-template-expressions": [
        "error",
        {
          allowBoolean: true,
          allowNever: true,
          allowNullish: true,
          allowNumber: true,
        },
      ],
      "@typescript-eslint/no-base-to-string": "error",
      "local/relative-ts-extensions": "error",
      "local/hrtime-number-arithmetic": "error",
      "no-restricted-syntax": [
        "error",
        ...riskyEffectRuntimeCalls,
        ...effectDebtGuardrails,
      ],
      "no-unused-labels": "error",
      "no-useless-assignment": "error",
      "no-undef": "off",
      "no-var": "error",
      "object-shorthand": "error",
      "prefer-const": "error",
      "prefer-template": "error",
      "require-yield": "off",
      "@stylistic/comma-dangle": ["error", "always-multiline"],
      "@stylistic/eol-last": ["error", "always"],
      "@stylistic/quotes": ["error", "double", { avoidEscape: true }],
      "@stylistic/semi": ["error", "never"],
    },
  },
  {
    files: packageSourceFiles,
    ignores: [
      "packages/**/src/__tests__/**/*.ts",
      "packages/**/*.test.ts",
    ],
    rules: {
      "local/no-fixed-polling": "error",
      "local/no-module-durable-cache": "error",
      "no-restricted-syntax": [
        "error",
        ...effectDebtGuardrails,
      ],
    },
  },
  {
    // Ban dynamic `import("literal")` in production source. A module known at
    // author time must be imported statically: literal dynamic imports defeat
    // the import-graph analysis the dead-code gate relies on. Computed
    // specifiers (`import(expr)`) are NOT matched and remain legal.
    files: ["packages/*/src/**/*.ts", "src/**/*.ts"],
    plugins: { local },
    rules: {
      "local/no-literal-dynamic-import": "error",
    },
  },
  {
    // Production package-source quality rules (Effect-native invariants).
    files: packageSourceFiles,
    ignores: [
      "packages/**/src/__tests__/**/*.ts",
      "packages/**/*.test.ts",
    ],
    rules: {
      "local/no-launder-cast": "error",
      "local/no-new-durable-stream": "error",
      "local/no-for-of-in-source": "error",
      "local/no-any-no-context-cast": "error",
      "local/no-detached-promise-in-effect-sync": "error",
      "local/no-unclassified-workflow-make": "error",
    },
  },
  // ===========================================================================
  // Generic source-text quality guards (ported from the retired Semgrep pack;
  // package-boundary-specific rules dropped in the fluent extraction).
  // ===========================================================================
  {
    // No durable identity from crypto.randomUUID(); identity must come from
    // stable config/storage.
    files: ["packages/*/src/**/*.ts", "packages/*/src/**/*.tsx", "src/**/*.ts"],
    ignores: portedSemgrepTestIgnores,
    rules: {
      "local/sg-no-random-durable-identity": [
        "error",
        [{ pattern: '((hostId|workerId|durableExecutionId)\\s*[:=]\\s*crypto\\.randomUUID\\(\\)|`(host|worker|durableExecution)[_-]\\$\\{crypto\\.randomUUID\\(\\)\\}`)', message: "Durable identity generated from crypto.randomUUID(); identity must come from stable config/storage." }],
      ],
    },
  },
  {
    // Inline tagged-error fail / mutable identity let / non-exhaustive Match.
    files: ["packages/*/src/**/*.ts", "packages/*/src/**/*.tsx"],
    ignores: portedSemgrepTestIgnores,
    rules: {
      "local/sg-no-inline-tagged-error-fail": [
        "error",
        [{ pattern: 'Effect\\.fail\\(\\s*\\{[^}]*\\b_tag\\s*:\\s*["\']', message: "Inline tagged-error object passed to Effect.fail; use a Data.TaggedError or Schema.TaggedError class." }],
      ],
      "local/sg-no-mutable-identity-let": [
        "error",
        [{ pattern: '\\blet\\s+[A-Za-z0-9_]*(?:sessionId|contextId|hostId)[A-Za-z0-9_]*(?:\\s*:\\s*string)?\\s*=\\s*""', message: "let declaration for an identity field initialized to a placeholder; sequence initialization so the identity is const before first use." }],
      ],
      "local/sg-match-should-be-exhaustive": [
        "error",
        [{ pattern: 'Match\\.value\\([^)]*\\)\\.pipe\\(\\s*(?:[^()]|\\([^)]*\\)|\\n)*Match\\.tag\\([^)]*\\),\\s*\\)', message: "Match expression does not end with Match.exhaustive or Match.orElse." }],
      ],
    },
  },
  {
    files: testFiles,
    rules: {
      "@typescript-eslint/no-base-to-string": "off",
      "@typescript-eslint/no-empty-object-type": "off",
      "@typescript-eslint/no-explicit-any": "off",
      "@typescript-eslint/no-floating-promises": "off",
      "@typescript-eslint/no-non-null-assertion": "off",
      "@typescript-eslint/no-redundant-type-constituents": "off",
      "@typescript-eslint/no-unsafe-argument": "off",
      "@typescript-eslint/no-unsafe-assignment": "off",
      "@typescript-eslint/no-unsafe-call": "off",
      "@typescript-eslint/no-unsafe-member-access": "off",
      "@typescript-eslint/no-unsafe-return": "off",
      "@typescript-eslint/prefer-promise-reject-errors": "off",
      "@typescript-eslint/restrict-template-expressions": "off",
      "local/no-fixed-polling": "off",
      "local/no-for-of-in-source": "off",
      "local/no-launder-cast": "off",
      "local/no-module-durable-cache": "off",
      "local/no-new-durable-stream": "off",
      "local/sg-match-should-be-exhaustive": "off",
      "local/sg-no-inline-tagged-error-fail": "off",
      "local/sg-no-mutable-identity-let": "off",
      "local/sg-no-random-durable-identity": "off",
      "no-restricted-syntax": "off",
      "no-undef": "off",
      "@stylistic/quotes": "off",
    },
  },
)
