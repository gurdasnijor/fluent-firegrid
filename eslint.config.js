import effect from "@effect/eslint-plugin"
import js from "@eslint/js"
import stylistic from "@stylistic/eslint-plugin"
import globals from "globals"
import tseslint from "typescript-eslint"
import effectEslint from "@codeforbreakfast/eslint-effect"

// @codeforbreakfast/eslint-effect rules that are clean (or auto-fixable to
// clean) across packages/*/src today — enforced as errors. The opinionated /
// high-blast-radius rules the team has agreed to adopt (functional immutability
// suite + effect/prefer-effect-platform + moderate tier) run in the non-blocking
// `lint:effect` burn-down lane (eslint.config.effect-advisory.mjs) until their
// counts reach zero, then graduate here. no-gen / no-if-statement /
// no-method-pipe are intentionally NOT adopted (they fight this repo's
// Effect.gen-first / imperative-control-flow idioms).
// NOTE: this plugin's autofixers are NOT reliable — `prefer-andThen` rewrites a
// multi-statement `flatMap(() => { … })` block into an invalid object literal,
// and `prefer-as-some` emits `Effect.asSome()` (called with no self). Do not run
// `eslint --fix` for effect/* rules. Rules that currently report findings
// (prefer-andThen, prefer-as-some) live in the advisory lane for MANUAL fixing,
// not here — the blocking set is rules that are already clean at zero.
const effectBlockingRules = {
  "effect/no-runSync": "error",
  "effect/no-runPromise": "error",
  "effect/prefer-as": "error",
  "effect/no-pipe-first-arg-call": "error",
  "effect/no-unnecessary-pipe-wrapper": "error",
  "effect/no-effect-if-option-check": "error",
  "effect/prefer-match-tag": "error",
  "effect/prefer-match-over-conditionals": "error",
  "effect/prefer-effect-if-over-match-boolean": "error",
  "effect/prefer-as-void": "error",
  "effect/prefer-as-some-error": "error",
  "effect/prefer-flatten": "error",
  "effect/prefer-zip-left": "error",
  "effect/prefer-zip-right": "error",
  "effect/prefer-ignore": "error",
  "effect/prefer-ignore-logged": "error",
  "effect/prefer-from-nullable": "error",
  "effect/prefer-get-or-else": "error",
  "effect/prefer-get-or-null": "error",
  "effect/prefer-get-or-undefined": "error",
  "effect/prefer-succeed-none": "error",
}

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

const nodeProcessImportMessage =
  "Do not import node:process from product source; use @effect/platform / @effect/platform-node runtime boundaries instead."

// Raw node: I/O builtins banned from product source — use the @effect/platform
// services (provided by NodeContext.layer at the CLI/runtime boundary). Genuine
// boundaries (bin entrypoints, the OTel-node integration) are scoped out / escape-
// hatched with a documented reason. tf-636o.
const rawNodeIoImportMessages = {
  "node:fs": "Use @effect/platform FileSystem (`yield* FileSystem.FileSystem`) instead of node:fs.",
  "node:fs/promises": "Use @effect/platform FileSystem (`yield* FileSystem.FileSystem`) instead of node:fs/promises.",
  "node:path": "Use @effect/platform Path (`yield* Path.Path`) instead of node:path.",
  "node:url": "Use @effect/platform Path (`fromFileUrl` / `toFileUrl`) instead of node:url.",
  "node:child_process": "Use @effect/platform Command (`Command.make` / `Command.string`) instead of node:child_process.",
  // tf-h1ld: node:net banned in product source — bind via @effect/platform-node
  // `NodeHttpServer.layer` and read the bound address with `HttpServer.addressWith`.
  // (`node:http` is NOT banned: `createServer` is the documented NodeHttpServer.layer
  // factory argument — a legit @effect/platform-node boundary, not raw I/O.)
  "node:net": "Use @effect/platform HttpServer (`NodeHttpServer.layer` + `HttpServer.addressWith`) instead of node:net.",
  // node:stream is banned in product source; bin/ stdio bridges (process.stdin/stdout
  // → WHATWG Web streams for the ACP edge) are scoped out as genuine boundaries.
  "node:stream": "Use @effect/platform Stream / `@effect/platform-node` NodeStream instead of node:stream (bin/ stdio bridges are exempt).",
}
const tsOnly = (configs) =>
  configs.map((config) => ({
    ...config,
    files: [
      "src/**/*.ts",
      "packages/**/*.ts",
      "packages/**/*.tsx",
      "apps/**/*.ts",
      "apps/**/*.tsx",
    ],
  }))
const relativeJsSpecifierPattern = /^\.{1,2}\/.*\.js$/u
const rewriteJsSpecifierToTs = (specifier) => specifier.replace(/\.js$/u, ".ts")
const durableAuthorityNamePattern =
  /(?:cache|registry|registries|runs|claims|completions|pending|subscribers|eventplanes?|eventPlane)/u
const hostAuthorityRegistryNamePattern =
  /(?:(?:run|completion|claim|eventPlane).*(?:cache|registry)|(?:cache|registry).*(?:run|completion|claim|eventPlane))/u
const controlPlaneImportPattern =
  /^(?:node:)?https?$|^(?:express|fastify|hono|koa)$|^@hono\/node-server$|^@effect\/platform\/HttpServer/u
const pollingAllowComment = "durable-lint-allow-polling"
const timerAllowComment = "durable-lint-allow-timer"
const cacheAllowComment = "durable-lint-allow-cache"
const controlPlaneAllowComment = "durable-lint-allow-control-plane"
// firegrid-remediation-hardening.STATIC_QUALITY.10
const extendsErrorAllowComment = "effect-quality-allow-extends-error"
const processEnvAllowComment = "effect-quality-allow-process-env"
// Pure value-builders / non-durable metadata / CLI filename stamps may default
// to wall-clock at a documented boundary; durable Effect code must read Clock.
const wallClockAllowComment = "effect-quality-allow-wall-clock"
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

const getCallName = (node) => {
  if (node?.type !== "CallExpression") {
    return undefined
  }

  if (node.callee.type === "Identifier") {
    return node.callee.name
  }

  return getStaticPropertyName(node.callee)
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

// firegrid-remediation-hardening.STATIC_QUALITY.14 — relocated from the retired
// ast-grep rule pack. OpenTelemetry hrtime tuples [seconds, nanos] need bigint
// arithmetic to preserve precision; direct Number() math on index [0] loses
// precision past ~2^53 ns (~26h). Flags any `<x>.{startTime,endTime,duration}[0]`
// used as the left operand of a `*` (the reach-around the trace.ts helpers).
// Ported Semgrep `pattern-regex` enforcement (Semgrep retirement, consolidation
// phase 2). These rules scan source TEXT for banned shapes, mirroring Semgrep's
// text-regex semantics exactly (the same regexes, applied to the same source),
// with per-rule file scoping handled by ESLint's native `files`/`ignores` on the
// enabling config block. One shared rule implementation is registered under
// several distinct ids so overlapping scopes don't collide on ESLint's per-rule
// config merge; each enabling block passes that scope's pattern list via options.
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
// One shared implementation, registered under distinct ids (below) so that a
// file matching several scope blocks gets each scope's full pattern list.
const sourceRegexBanRule = makeSourceRegexBanRule()

// tf-1kuk: AST upgrade of the pure import-path bans (previously source-text regex
// via sourceRegexBanRule). Matches each `pattern` against the resolved module
// specifier of import/export declarations only — so it can never false-positive
// on a path that merely appears in a comment or string literal. Same options
// shape (per-scope `{ pattern, flags?, message }[]`) and same distinct-id-per-
// scope registration; the `pattern` is now the module-specifier regex (the
// `from\s+"…"` wrapper is dropped).
const makeBannedImportPathRule = () => ({
  meta: {
    type: "problem",
    docs: {
      description: "Disallow imports whose module specifier matches a banned path pattern (AST, import-source only).",
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
    const entries = (context.options[0] ?? []).map((e) => ({
      re: new RegExp(e.pattern, e.flags ?? ""),
      message: e.message,
    }))
    const check = (node) => {
      const source = node?.source?.value
      if (typeof source !== "string") return
      for (const { re, message } of entries) {
        if (re.test(source)) {
          context.report({ node: node.source, messageId: "banned", data: { message } })
        }
      }
    }
    return {
      ImportDeclaration: check,
      ExportNamedDeclaration: check,
      ExportAllDeclaration: check,
    }
  },
})
const bannedImportPathRule = makeBannedImportPathRule()

// tf-0ska: AST upgrade for `sg-*` identifier-surface bans (previously source-text
// regex via sourceRegexBanRule). Tests each `pattern` against the NAME of every
// `Identifier` node — which the TS parser also emits for type references and
// qualified-name parts — so a banned symbol is caught wherever it's used in code,
// but never when it merely appears in a comment or string literal (those aren't
// Identifier nodes). Same options shape + distinct-id-per-scope registration.
const makeBannedIdentifierRule = () => ({
  meta: {
    type: "problem",
    docs: {
      description: "Disallow identifiers whose name matches a banned-surface pattern (AST, code references only).",
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
    const entries = (context.options[0] ?? []).map((e) => ({
      re: new RegExp(e.pattern, e.flags ?? ""),
      message: e.message,
    }))
    return {
      Identifier(node) {
        for (const { re, message } of entries) {
          if (re.test(node.name)) {
            context.report({ node, messageId: "banned", data: { message } })
          }
        }
      },
    }
  },
})
const bannedIdentifierRule = makeBannedIdentifierRule()

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
    "no-node-process-import": {
      meta: {
        type: "problem",
        docs: {
          description: "Disallow direct node:process imports from product source.",
        },
        schema: [],
        messages: {
          noNodeProcess: nodeProcessImportMessage,
        },
      },
      create(context) {
        const report = (node) => {
          if (node?.source?.value === "node:process") {
            context.report({ node, messageId: "noNodeProcess" })
          }
        }

        return {
          ImportDeclaration: report,
          ExportAllDeclaration: report,
          ExportNamedDeclaration: report,
        }
      },
    },
    "no-raw-node-io": {
      meta: {
        type: "problem",
        docs: {
          description: "Disallow raw node:fs/path/url/child_process in product source; use @effect/platform services.",
        },
        schema: [],
        messages: { noRawNodeIo: "{{guidance}}" },
      },
      create(context) {
        const report = (node) => {
          const source = node?.source?.value
          if (
            typeof source === "string" &&
            Object.prototype.hasOwnProperty.call(rawNodeIoImportMessages, source)
          ) {
            context.report({
              node,
              messageId: "noRawNodeIo",
              data: { guidance: rawNodeIoImportMessages[source] },
            })
          }
        }
        return {
          ImportDeclaration: report,
          ExportAllDeclaration: report,
          ExportNamedDeclaration: report,
        }
      },
    },
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
    "no-production-js-timers": {
      meta: {
        type: "problem",
        docs: {
          description: "Disallow production JS timers that can become fixed polling loops.",
        },
        schema: [],
        messages: {
          noTimer:
            "Avoid JS timers in production runtime code; use durable subscriptions, deadline-derived Effects, or a reviewed escape comment.",
        },
      },
      create(context) {
        const timerNames = new Set(["setInterval", "setTimeout", "setImmediate"])

        return {
          CallExpression(node) {
            if (!timerNames.has(getCallName(node)) || hasNearbyAllowComment(context, node, [timerAllowComment, pollingAllowComment])) {
              return
            }

            context.report({ node, messageId: "noTimer" })
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
    "no-hidden-control-plane": {
      meta: {
        type: "problem",
        docs: {
          description: "Disallow hidden HTTP/control-plane imports in production packages.",
        },
        schema: [],
        messages: {
          noControlPlane:
            "Avoid hidden HTTP/control-plane surfaces in production packages; add a reviewed escape comment if this is intentional.",
        },
      },
      create(context) {
        return {
          ImportDeclaration(node) {
            if (
              typeof node.source.value === "string" &&
              controlPlaneImportPattern.test(node.source.value) &&
              !hasNearbyAllowComment(context, node, [controlPlaneAllowComment])
            ) {
              context.report({ node: node.source, messageId: "noControlPlane" })
            }
          },
        }
      },
    },
    "simulation-host-real-firegrid-host": {
      meta: {
        type: "problem",
        docs: {
          description:
            "Require firelab simulation hosts to compose through the sanctioned published host-sdk runtime root so sim == prod and no per-sim layer assembly creeps back in.",
        },
        schema: [],
        messages: {
          missingFactoryImport:
            "Simulation host.ts must compose through a published runtime root — import firegridHost/runFiregridHost from @firegrid/host-sdk or FluentRuntimeLive from @firegrid/fluent-runtime, not hand-assemble a runtime + ingress per sim.",
          missingFactoryCall:
            "Simulation host.ts imported a published runtime root but never called it.",
        },
      },
      create(context) {
        const factoryNamesBySource = new Map([
          ["@firegrid/host-sdk", new Set(["firegridHost", "runFiregridHost"])],
          ["@firegrid/fluent-runtime", new Set(["FluentRuntimeLive"])],
        ])
        let importedFactoryLocalName
        let calledFactory = false

        return {
          ImportDeclaration(node) {
            const factoryNames = factoryNamesBySource.get(node.source.value)
            if (factoryNames === undefined) {
              return
            }

            for (const specifier of node.specifiers) {
              if (
                specifier.type === "ImportSpecifier" &&
                specifier.imported.type === "Identifier" &&
                factoryNames.has(specifier.imported.name)
              ) {
                importedFactoryLocalName = specifier.local.name
              }
            }
          },
          CallExpression(node) {
            if (
              importedFactoryLocalName !== undefined &&
              node.callee.type === "Identifier" &&
              node.callee.name === importedFactoryLocalName
            ) {
              calledFactory = true
            }
          },
          "Program:exit"(node) {
            if (importedFactoryLocalName === undefined) {
              context.report({ node, messageId: "missingFactoryImport" })
            } else if (!calledFactory) {
              context.report({ node, messageId: "missingFactoryCall" })
            }
          },
        }
      },
    },
    // firegrid-remediation-hardening.STATIC_QUALITY.10
    // firegrid-remediation-hardening.EFFECT_CONSISTENCY.2
    "no-extends-error": {
      meta: {
        type: "problem",
        docs: {
          description:
            "Disallow class extends Error declarations in package source; use Data.TaggedError.",
        },
        schema: [],
        messages: {
          noExtendsError:
            "Use Data.TaggedError(\"...\")<...>{} instead of class extends Error. Domain errors must be tagged for catchTag/Match.tag/Schema.is to work. The repo policy keeps Data.TaggedError; growth of class extends Error is blocked here.",
        },
      },
      create(context) {
        const isErrorSuper = (node) =>
          node?.type === "Identifier" && node.name === "Error"
        const visit = (node) => {
          if (
            node?.superClass != null &&
            isErrorSuper(node.superClass) &&
            !hasNearbyAllowComment(context, node, [extendsErrorAllowComment])
          ) {
            context.report({ node, messageId: "noExtendsError" })
          }
        }
        return {
          ClassDeclaration: visit,
          ClassExpression: visit,
        }
      },
    },
    // firegrid-remediation-hardening.STATIC_QUALITY.10
    "no-process-env-outside-bin": {
      meta: {
        type: "problem",
        docs: {
          description:
            "Disallow process.env reads outside bin/ and scripts/; use @effect/platform Config or boundary-injected configuration.",
        },
        schema: [],
        messages: {
          noProcessEnv:
            "process.env reads belong at the binary entry boundary (bin/) or in tooling scripts (scripts/). In application code use Config.string / Config.option / Config.redacted, or accept config as an explicit parameter.",
        },
      },
      create(context) {
        const isGlobalThisProcess = (object) =>
          object?.type === "MemberExpression" &&
          !object.computed &&
          object.object?.type === "Identifier" &&
          object.object.name === "globalThis" &&
          object.property?.type === "Identifier" &&
          object.property.name === "process"
        // Semgrep matched `globalThis.process.env.X` / `[X]` (a trailing access),
        // not a bare `globalThis.process.env` value passed around; mirror that so
        // the port neither weakens nor over-reaches. The direct `process.env`
        // form keeps the existing any-access behavior.
        const hasTrailingAccess = (node) =>
          node.parent?.type === "MemberExpression" && node.parent.object === node

        return {
          MemberExpression(node) {
            if (
              node.property?.type !== "Identifier" ||
              node.property.name !== "env" ||
              hasNearbyAllowComment(context, node, [processEnvAllowComment])
            ) {
              return
            }
            if (node.object?.type === "Identifier" && node.object.name === "process") {
              context.report({ node, messageId: "noProcessEnv" })
            } else if (isGlobalThisProcess(node.object) && hasTrailingAccess(node)) {
              context.report({ node, messageId: "noProcessEnv" })
            }
          },
        }
      },
    },
    // Ported Semgrep `firegrid-no-date-now` (ERROR): Date.now() is not
    // replay-safe; use Clock.currentTimeMillis. AST-precise (no comment/string
    // false positives), matching Semgrep's `pattern: Date.now()`.
    "no-date-now": {
      meta: {
        type: "problem",
        docs: {
          description:
            "Disallow Date.now() in library code; use Clock.currentTimeMillis or a caller-resolved timestamp.",
        },
        schema: [],
        messages: {
          noDateNow:
            "Date.now() is not replay-safe. Use Clock.currentTimeMillis inside Effect code or accept a timestamp resolved by the caller's Effect scope.",
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
              callee.object.name === "Date" &&
              callee.property?.type === "Identifier" &&
              callee.property.name === "now" &&
              node.arguments.length === 0
            ) {
              context.report({ node, messageId: "noDateNow" })
            }
          },
        }
      },
    },
    // Relocated from the effect-quality ts-morph ratchet (`newDateIsoCount`).
    // `new Date().toISOString()` (no-arg) reads wall-clock outside Effect, so it
    // is not replay-safe; durable code must read `Clock.currentTimeMillis` and
    // format `new Date(millis).toISOString()`. Pure value-builders / non-durable
    // metadata / CLI filename stamps escape-hatch with the documented allow
    // comment (matches the ratchet's AST-precise detector — no string/comment FPs).
    "no-new-date-iso": {
      meta: {
        type: "problem",
        docs: {
          description:
            "Disallow `new Date().toISOString()` (no-arg) in library code; read Clock.currentTimeMillis and format `new Date(millis).toISOString()`.",
        },
        schema: [],
        messages: {
          noNewDateIso:
            "`new Date().toISOString()` reads wall-clock and is not replay-safe. Read `yield* Clock.currentTimeMillis` (or `DateTime.now`) inside Effect code and format `new Date(millis).toISOString()`. Pure value-builders / CLI stamps may escape-hatch with `// effect-quality-allow-wall-clock`.",
        },
      },
      create(context) {
        return {
          CallExpression(node) {
            const callee = node.callee
            if (
              callee.type !== "MemberExpression" ||
              callee.computed ||
              callee.property?.type !== "Identifier" ||
              callee.property.name !== "toISOString" ||
              node.arguments.length !== 0
            ) {
              return
            }
            const receiver = callee.object
            if (
              receiver?.type === "NewExpression" &&
              receiver.callee?.type === "Identifier" &&
              receiver.callee.name === "Date" &&
              receiver.arguments.length === 0 &&
              !hasNearbyAllowComment(context, node, [wallClockAllowComment])
            ) {
              context.report({ node, messageId: "noNewDateIso" })
            }
          },
        }
      },
    },
    // Relocated from the effect-quality ratchet (`nodeCryptoImportCount`). Node's
    // crypto RNG is not replay-safe; use a deterministic / Effect-resolved source.
    "no-node-crypto-import": {
      meta: {
        type: "problem",
        docs: { description: "Disallow node:crypto / crypto imports in library code (non-replay-safe RNG)." },
        schema: [],
        messages: {
          noNodeCrypto:
            "node:crypto / crypto is not replay-safe. Use a deterministic id/hash helper or an Effect-resolved randomness source instead.",
        },
      },
      create(context) {
        const report = (node) => {
          const source = node?.source?.value
          if (source === "node:crypto" || source === "crypto") {
            context.report({ node, messageId: "noNodeCrypto" })
          }
        }
        return { ImportDeclaration: report, ExportAllDeclaration: report, ExportNamedDeclaration: report }
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
    // Ported Semgrep source-regex rules (Semgrep retirement). Each id shares the
    // one `sourceRegexBanRule` implementation; the enabling config block sets the
    // file scope (matching the original Semgrep `paths`) and passes that rule's
    // pattern list via options. One id per original Semgrep rule so each keeps its
    // exact scope and distinct ids never collide on ESLint's per-rule config merge.
    "sg-no-inline-stream-url-construction": sourceRegexBanRule,
    "sg-no-filesystem-in-runtime-package": sourceRegexBanRule,
    "sg-no-host-id-env-authority": sourceRegexBanRule,
    "sg-runtime-context-workflow-requires-local-authority": sourceRegexBanRule,
    "sg-no-replay-path-output-scan": sourceRegexBanRule,
    "sg-runtime-owned-table-writes-use-authorities": sourceRegexBanRule,
    "sg-runtime-subscribers-transforms-no-table-facades": sourceRegexBanRule,
    "sg-runtime-no-exported-authority-singletons": sourceRegexBanRule,
    "sg-runtime-no-custom-authority-wrapper-types": sourceRegexBanRule,
    "sg-runtime-no-authority-static-helper-calls": sourceRegexBanRule,
    "sg-runtime-no-singleton-authority-specifiers": sourceRegexBanRule,
    "sg-runtime-no-second-durable-capability-provider": sourceRegexBanRule,
    "sg-runtime-no-source-collection-handle-in-static-subscriber-contract": sourceRegexBanRule,
    "sg-runtime-no-table-service-yield-outside-providers": sourceRegexBanRule,
    "sg-runtime-no-authority-registry-surface": bannedIdentifierRule,
    "sg-runtime-host-no-direct-source-collection-registration": sourceRegexBanRule,
    "sg-runtime-no-host-internal-imports-outside-host": bannedImportPathRule,
    "sg-runtime-no-runtime-errors-imports-outside-runtime": bannedImportPathRule,
    "sg-runtime-no-old-singleton-authority-tag-keys": sourceRegexBanRule,
    "sg-runtime-no-table-type-parameters-outside-authorities": sourceRegexBanRule,
    "sg-runtime-no-exported-authority-registry-api": sourceRegexBanRule,
    "sg-factory-exported-contracts-use-schema": sourceRegexBanRule,
    "sg-no-random-durable-identity": sourceRegexBanRule,
    "sg-no-raw-stream-authority-string-schema": sourceRegexBanRule,
    "sg-no-inline-tagged-error-fail": sourceRegexBanRule,
    "sg-no-mutable-identity-let": sourceRegexBanRule,
    "sg-match-should-be-exhaustive": sourceRegexBanRule,
    "sg-c4-no-new-durable-deferred-runtime-wait": sourceRegexBanRule,
    "sg-c6-no-source-specific-cursor-event-taxonomy-in-agent-tools": sourceRegexBanRule,
    "sg-c7-no-edge-local-terminal-synthesis": sourceRegexBanRule,
    "sg-shape-c-no-workflow-engine-in-runtime-context-subscriber": sourceRegexBanRule,
    "sg-transforms-purity-import-boundary": sourceRegexBanRule,
    "sg-shape-c-runtime-context-no-workflow-machinery": sourceRegexBanRule,
    "sg-composition-no-legacy-imports": sourceRegexBanRule,
    "sg-host-sdk-imports": bannedImportPathRule,
    "sg-no-numbered-runtime-subpath": bannedImportPathRule,
  },
}

// Test-file ignores shared by the ported-Semgrep blocks (the original rules all
// excluded `**/*.test.{ts,tsx}` and `**/__tests__/**`).
const portedSemgrepTestIgnores = [
  "packages/**/*.test.ts",
  "packages/**/*.test.tsx",
  "packages/**/src/__tests__/**/*.ts",
  "packages/**/src/__tests__/**/*.tsx",
  "apps/**/*.test.ts",
  "apps/**/*.test.tsx",
  "apps/**/src/__tests__/**/*.ts",
  "apps/**/src/__tests__/**/*.tsx",
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
    ],
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
    files: [
      "src/**/*.ts",
      "packages/**/*.ts",
      "packages/**/*.tsx",
    ],
    languageOptions: {
      globals: { ...globals.node, ...globals.browser },
      parserOptions: {
        project: "./tsconfig.eslint.json",
        tsconfigRootDir: import.meta.dirname,
      },
    },
    plugins: {
      "@effect": effect,
      "@stylistic": stylistic,
      local,
    },
    rules: {
      "@typescript-eslint/consistent-type-imports": [
        "warn",
        {
          fixStyle: "inline-type-imports",
        },
      ],
      "@typescript-eslint/no-floating-promises": "error",
      "@typescript-eslint/no-misused-promises": "error",
      "@typescript-eslint/no-unnecessary-type-assertion": "warn",
      "@typescript-eslint/no-unused-vars": [
        "warn",
        {
          argsIgnorePattern: "^_",
          caughtErrorsIgnorePattern: "^_",
          varsIgnorePattern: "^_",
        },
      ],
      "@typescript-eslint/require-await": "off",
      "@typescript-eslint/restrict-template-expressions": [
        "warn",
        {
          allowBoolean: true,
          allowNever: true,
          allowNullish: true,
          allowNumber: true,
        },
      ],
      "@typescript-eslint/no-base-to-string": "warn",
      "local/relative-ts-extensions": "error",
      "local/no-node-process-import": "error",
      "local/hrtime-number-arithmetic": "error",
      "no-restricted-syntax": [
        "warn",
        ...riskyEffectRuntimeCalls,
        ...effectDebtGuardrails,
      ],
      "no-unused-labels": "warn",
      "no-useless-assignment": "warn",
      "require-yield": "off",
      "@stylistic/comma-dangle": ["error", "always-multiline"],
      "@stylistic/eol-last": ["error", "always"],
      "@stylistic/quotes": ["error", "double", { avoidEscape: true }],
      "@stylistic/semi": ["error", "never"],
    },
  },
  {
    files: ["packages/**/src/**/*.ts"],
    ignores: [
      "packages/**/src/__tests__/**/*.ts",
      "packages/**/*.test.ts",
    ],
    rules: {
      "local/no-fixed-polling": "warn",
      "local/no-module-durable-cache": "warn",
      "local/no-production-js-timers": "error",
      "no-restricted-syntax": [
        "warn",
        ...effectDebtGuardrails,
      ],
    },
  },
  {
    files: [
      "packages/conformance/**/*.ts",
      "packages/**/test/**/*.ts",
    ],
    rules: {
      "@typescript-eslint/no-base-to-string": "off",
      "@typescript-eslint/no-explicit-any": "off",
      "@typescript-eslint/no-floating-promises": "off",
      "@typescript-eslint/no-redundant-type-constituents": "off",
      "@typescript-eslint/no-unsafe-argument": "off",
      "@typescript-eslint/no-unsafe-assignment": "off",
      "@typescript-eslint/no-unsafe-call": "off",
      "@typescript-eslint/no-unsafe-member-access": "off",
      "@typescript-eslint/prefer-promise-reject-errors": "off",
      "@typescript-eslint/restrict-template-expressions": "off",
      "local/no-date-now": "off",
      "local/no-fixed-polling": "off",
      "local/no-for-of-in-source": "off",
      "local/no-module-durable-cache": "off",
      "local/no-new-date-iso": "off",
      "local/no-new-durable-stream": "off",
      "local/no-node-crypto-import": "off",
      "local/no-process-env-outside-bin": "off",
      "local/no-production-js-timers": "off",
      "local/no-raw-node-io": "off",
      "no-restricted-syntax": "off",
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
    files: [
      "packages/**/src/**/*.ts",
      "packages/**/src/**/*.tsx",
    ],
    ignores: [
      "packages/**/src/__tests__/**/*.ts",
      "packages/**/*.test.ts",
    ],
    rules: {
      "local/no-extends-error": "error",
      "local/no-process-env-outside-bin": "error",
      "local/no-date-now": "error",
      "local/no-new-date-iso": "error",
      "local/no-launder-cast": "error",
      "local/no-node-crypto-import": "error",
      "local/no-new-durable-stream": "error",
      "local/no-for-of-in-source": "error",
      "local/no-any-no-context-cast": "error",
      "local/no-detached-promise-in-effect-sync": "error",
      "local/no-unclassified-workflow-make": "error",
    },
  },
  {
    // Ban raw node: I/O builtins in product source — use the @effect/platform
    // services (FileSystem / Path / Command). Bin entrypoints and tests are
    // scoped out (genuine node boundaries).
    files: ["packages/*/src/**/*.ts"],
    ignores: [
      "**/bin/**",
      "**/*.test.ts",
      "**/*.spec.ts",
      "**/__tests__/**",
    ],
    plugins: { local },
    rules: {
      "local/no-raw-node-io": "error",
    },
  },
  {
    files: [
      "packages/**/src/__tests__/**/*.ts",
      "packages/**/*.test.ts",
    ],
    rules: {
      "@typescript-eslint/no-unsafe-argument": "warn",
      "@typescript-eslint/no-unsafe-assignment": "warn",
      "@typescript-eslint/no-unsafe-member-access": "warn",
      "@typescript-eslint/no-non-null-assertion": "off",
      "no-restricted-syntax": [
        "warn",
        ...effectDebtGuardrails,
      ],
    },
  },
  {
    // process.env belongs at the binary entry boundary: bin/ entry points
    // (spawn targets, CLI mains) read boundary configuration from env.
    files: ["packages/*/src/bin/**/*.ts"],
    rules: {
      "local/no-process-env-outside-bin": "off",
      "local/no-date-now": "off",
      "local/no-new-date-iso": "off",
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
    files: [
      "packages/conformance/**/*.ts",
      "packages/conformance/**/*.mjs",
      "packages/**/test/**/*.ts",
    ],
    rules: {
      "@typescript-eslint/no-base-to-string": "off",
      "@typescript-eslint/no-empty-object-type": "off",
      "@typescript-eslint/no-explicit-any": "off",
      "@typescript-eslint/no-floating-promises": "off",
      "@typescript-eslint/no-redundant-type-constituents": "off",
      "@typescript-eslint/no-unsafe-argument": "off",
      "@typescript-eslint/no-unsafe-assignment": "off",
      "@typescript-eslint/no-unsafe-call": "off",
      "@typescript-eslint/no-unsafe-member-access": "off",
      "@typescript-eslint/no-unsafe-return": "off",
      "@typescript-eslint/prefer-promise-reject-errors": "off",
      "@typescript-eslint/restrict-template-expressions": "off",
      "local/no-date-now": "off",
      "local/no-fixed-polling": "off",
      "local/no-for-of-in-source": "off",
      "local/no-launder-cast": "off",
      "local/no-module-durable-cache": "off",
      "local/no-new-date-iso": "off",
      "local/no-new-durable-stream": "off",
      "local/no-node-crypto-import": "off",
      "local/no-process-env-outside-bin": "off",
      "local/no-production-js-timers": "off",
      "local/no-raw-node-io": "off",
      "local/sg-match-should-be-exhaustive": "off",
      "local/sg-no-inline-tagged-error-fail": "off",
      "local/sg-no-mutable-identity-let": "off",
      "local/sg-no-random-durable-identity": "off",
      "no-restricted-syntax": "off",
      "no-undef": "off",
      "stylistic/quotes": "off",
    },
  },
  {
    // @codeforbreakfast/eslint-effect — blocking subset (clean today). The
    // burn-down lane for the agreed opinionated rules lives in
    // eslint.config.effect-advisory.mjs (`pnpm run lint:effect`).
    files: ["packages/**/src/**/*.ts"],
    ignores: [
      "packages/**/src/__tests__/**/*.ts",
      "packages/**/*.test.ts",
    ],
    plugins: { effect: { rules: effectEslint.rules } },
    rules: effectBlockingRules,
  },
)
