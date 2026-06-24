import fs from "node:fs"
import path from "node:path"

const jsExtensions = [".js", ".jsx", ".mjs", ".cjs"]
const extensionMap = {
  ".js": ".ts",
  ".jsx": ".tsx",
  ".mjs": ".mts",
  ".cjs": ".cts"
}
const sourceExtensions = [".ts", ".tsx", ".mts", ".cts", ".js", ".jsx", ".mjs", ".cjs"]
const schemaSources = new Set(["effect", "effect/Schema"])
const schemaNamespaceSources = new Set(["effect/Schema"])

const isRelativeImport = (source) => source.startsWith("./") || source.startsWith("../")

const getJsExtension = (source) => jsExtensions.find((extension) => source.endsWith(extension))

const hasIndexFile = (directory) =>
  sourceExtensions.some((extension) => fs.existsSync(path.join(directory, `index${extension}`)))

const isIndexImport = (importPath) => {
  const basename = path.basename(importPath)
  return basename === "index" || /^index\.(?:ts|tsx|js|jsx|mts|cts|mjs|cjs)$/u.test(basename)
}

const resolvesToBarrel = (importSource, currentFile) => {
  if (isIndexImport(importSource)) {
    return true
  }
  return hasIndexFile(path.resolve(path.dirname(currentFile), importSource))
}

const getImportedName = (specifier) =>
  specifier.imported.type === "Identifier" ? specifier.imported.name : specifier.imported.value

const createRule = (visitors, meta = { type: "problem" }) => ({
  meta,
  create(context) {
    return visitors(context)
  }
})

const noBigintLiterals = createRule((context) => ({
  Literal(node) {
    if (typeof node.value !== "bigint") {
      return
    }

    context.report({
      node,
      message: "BigInt literals are not allowed",
      fix: (fixer) => fixer.replaceText(node, `BigInt(${node.value})`)
    })
  }
}), {
  type: "problem",
  docs: { description: "Disallow bigint literals" },
  fixable: "code"
})

const noImportFromBarrelPackage = createRule((context) => {
  const options = context.options[0] ?? {}
  const patterns = (options.checkPatterns ?? []).map((pattern) => new RegExp(pattern))
  const checkRelative = options.checkRelativeIndexImports !== false
  const isBarrelImport = (source) => {
    if (isRelativeImport(source)) {
      return checkRelative && resolvesToBarrel(source, context.filename)
    }
    return patterns.some((pattern) => pattern.test(source))
  }

  return {
    ImportDeclaration(node) {
      if (node.importKind === "type") {
        return
      }

      const importSource = node.source.value
      if (typeof importSource !== "string" || !isBarrelImport(importSource)) {
        return
      }

      for (const specifier of node.specifiers) {
        if (specifier.type === "ImportNamespaceSpecifier") {
          context.report({
            node: specifier,
            message:
              `Do not use namespace import from barrel file "${importSource}", import from specific modules instead`
          })
        } else if (specifier.type === "ImportSpecifier" && specifier.importKind !== "type") {
          const moduleName = getImportedName(specifier)
          const localName = specifier.local.name
          const message = isRelativeImport(importSource)
            ? `Do not import "${moduleName}" from barrel file "${importSource}", import from specific module instead`
            : `Use import * as ${localName} from "${importSource}/${moduleName}" instead`
          context.report({ node: specifier, message })
        }
      }
    }
  }
}, {
  type: "suggestion",
  docs: { description: "Disallow importing from barrel files" },
  schema: [{
    type: "object",
    properties: {
      checkPatterns: { type: "array", items: { type: "string" } },
      checkRelativeIndexImports: { type: "boolean" }
    },
    additionalProperties: false
  }]
})

const noJsExtensionImports = createRule((context) => {
  const checkSource = (source) => {
    if (source == null || typeof source.value !== "string" || !isRelativeImport(source.value)) {
      return
    }

    const extension = getJsExtension(source.value)
    if (extension === undefined) {
      return
    }

    const tsExtension = extensionMap[extension]
    const fixedSource = source.value.slice(0, -extension.length) + tsExtension
    context.report({
      node: source,
      message: `Use "${tsExtension}" extension instead of "${extension}" for relative imports`,
      fix: (fixer) => fixer.replaceTextRange(source.range, `"${fixedSource}"`)
    })
  }

  return {
    ImportDeclaration: (node) => checkSource(node.source),
    ExportAllDeclaration: (node) => checkSource(node.source),
    ExportNamedDeclaration: (node) => checkSource(node.source),
    ImportExpression: (node) => checkSource(node.source),
    TSImportType: (node) => checkSource(node.argument)
  }
}, {
  type: "problem",
  docs: { description: "Disallow JS extensions in relative TypeScript imports" },
  fixable: "code"
})

const noOpaqueInstanceFields = createRule((context) => {
  const schemaIdentifiers = new Set()
  const opaqueIdentifiers = new Set()

  const isSchemaObject = (node) => node?.type === "Identifier" && schemaIdentifiers.has(node.name)
  const isOpaqueCallee = (node) => {
    if (node?.type === "Identifier") {
      return opaqueIdentifiers.has(node.name)
    }
    return node?.type === "MemberExpression" && node.property?.type === "Identifier"
      && node.property.name === "Opaque" && isSchemaObject(node.object)
  }
  const isSchemaOpaqueExtension = (node) =>
    node.superClass?.type === "CallExpression"
    && node.superClass.callee?.type === "CallExpression"
    && isOpaqueCallee(node.superClass.callee.callee)

  const checkClass = (node) => {
    if (!isSchemaOpaqueExtension(node)) {
      return
    }

    for (const element of node.body.body) {
      if (
        (element.type === "PropertyDefinition" || element.type === "MethodDefinition")
        && element.static !== true
      ) {
        context.report({
          node: element,
          message: "Classes extending Schema.Opaque must not have instance members"
        })
      }
    }
  }

  return {
    ImportDeclaration(node) {
      if (node.importKind === "type") {
        return
      }

      const source = node.source.value
      if (typeof source !== "string" || !schemaSources.has(source)) {
        return
      }

      for (const specifier of node.specifiers) {
        if (specifier.type === "ImportNamespaceSpecifier" && schemaNamespaceSources.has(source)) {
          schemaIdentifiers.add(specifier.local.name)
        } else if (specifier.type === "ImportSpecifier" && specifier.importKind !== "type") {
          const importedName = getImportedName(specifier)
          if (importedName === "Schema") {
            schemaIdentifiers.add(specifier.local.name)
          } else if (importedName === "Opaque") {
            opaqueIdentifiers.add(specifier.local.name)
          }
        }
      }
    },
    ClassDeclaration: checkClass,
    ClassExpression: checkClass
  }
}, {
  type: "problem",
  docs: { description: "Disallow instance members in Schema.Opaque classes" }
})

const jsdocs = createRule(() => ({}), {
  type: "problem",
  docs: { description: "Placeholder for Effect public API JSDoc model checks" }
})

const restrictedSyntax = createRule((context) => ({
  "CallExpression[callee.object.name='Effect'][callee.property.name='runPromise']"(node) {
    context.report({
      node,
      message: "Effect.runPromise belongs at runtime boundaries or in tests; keep library code Effect-native."
    })
  },
  "CallExpression[callee.object.name='Effect'][callee.property.name='runPromiseExit']"(node) {
    context.report({
      node,
      message: "Effect.runPromiseExit belongs at runtime boundaries or in tests; keep library code Effect-native."
    })
  },
  "CallExpression[callee.object.name='Effect'][callee.property.name='runSync']"(node) {
    context.report({
      node,
      message: "Effect.runSync belongs at explicit runtime boundaries; keep library code Effect-native."
    })
  },
  "CallExpression[callee.object.name='Effect'][callee.property.name='runFork']"(node) {
    context.report({
      node,
      message: "Effect.runFork creates an unmanaged fiber unless it is scoped deliberately."
    })
  },
  "CallExpression[callee.object.name='Effect'][callee.property.name='orDie']"(node) {
    context.report({
      node,
      message:
        "Effect.orDie collapses typed errors into defects; prefer typed errors unless this is a documented crash boundary."
    })
  },
  "CallExpression[callee.object.name='Layer'][callee.property.name='orDie']"(node) {
    context.report({
      node,
      message:
        "Layer.orDie collapses acquisition errors into defects; prefer typed errors unless this is a documented crash boundary."
    })
  },
  "CallExpression[callee.object.name='Effect'][callee.property.name='die']"(node) {
    context.report({
      node,
      message: "Effect.die should represent an unexpected defect, not ordinary domain failure."
    })
  },
  "CallExpression[callee.object.name='Schedule'][callee.property.name=/^(fixed|recurs|spaced)$/]"(node) {
    context.report({
      node,
      message:
        "Avoid fixed polling in durable runtime code; drive work from durable subscriptions, state folds, or explicit deadlines."
    })
  },
  "CallExpression[callee.object.name='Stream'][callee.property.name='tick']"(node) {
    context.report({
      node,
      message:
        "Avoid fixed polling in durable runtime code; drive work from durable subscriptions, state folds, or explicit deadlines."
    })
  },
  ":matches(ForStatement,ForInStatement,ForOfStatement,WhileStatement,DoWhileStatement) CallExpression[callee.object.name='Effect'][callee.property.name='sleep']"(
    node
  ) {
    context.report({ node, message: "Avoid sleep-in-loop polling in durable runtime code." })
  },
  "Program > VariableDeclaration[kind='let']"(node) {
    context.report({
      node,
      message: "Avoid module-scope mutable state in durable runtime code; derive restart state from durable state."
    })
  },
  "Program > VariableDeclaration > VariableDeclarator[id.name=/.*(?:cache|registry|registries|runs|claims|completions|pending|subscribers|eventplanes?|eventPlane).*/u][init.type=/^(ArrayExpression|ObjectExpression)$/]"(
    node
  ) {
    context.report({
      node,
      message:
        "Avoid module-scope durable-authority caches or registries; durable state must remain the source of truth."
    })
  },
  "Program > VariableDeclaration > VariableDeclarator[id.name=/.*(?:cache|registry|registries|runs|claims|completions|pending|subscribers|eventplanes?|eventPlane).*/u][init.type='NewExpression'][init.callee.name=/^(Map|Set|WeakMap|WeakSet)$/]"(
    node
  ) {
    context.report({
      node,
      message:
        "Avoid module-scope durable-authority caches or registries; durable state must remain the source of truth."
    })
  },
  ":matches(VariableDeclarator,FunctionDeclaration,ClassDeclaration)[id.name=/.*(?:(?:run|completion|claim|eventPlane).*(?:cache|registry)|(?:cache|registry).*(?:run|completion|claim|eventPlane)).*/ui]"(
    node
  ) {
    context.report({
      node,
      message:
        "Host code must not define durable-authority registries/caches for runs, completions, claims, or event planes."
    })
  },
  "ImportExpression[source.type='Literal']"(node) {
    context.report({
      node,
      message: "Do not use dynamic import() with a literal specifier; import modules known at author time statically."
    })
  },
  "NewExpression[callee.name='DurableStream']"(node) {
    context.report({
      node,
      message: "Do not construct DurableStream directly; go through DurableTable or the declared stream factories."
    })
  },
  "ForOfStatement"(node) {
    context.report({
      node,
      message: "Avoid imperative for-of in library source; use Array, Stream, or Chunk combinators."
    })
  },
  "CallExpression[callee.object.name='Workflow'][callee.property.name='make']"(node) {
    context.report({
      node,
      message: "Net-new Workflow.make is an owned durable workflow; justify it in the workflow admission ledger."
    })
  },
  "CallExpression[callee.object.name='Effect'][callee.property.name='sync'] UnaryExpression[operator='void'] CallExpression[callee.property.name='then']"(
    node
  ) {
    context.report({
      node,
      message: "Detached promise work inside Effect.sync escapes the Effect runtime; model the async work as an Effect."
    })
  },
  "CallExpression[callee.object.name='Effect'][callee.property.name='fail'] > ObjectExpression.arguments > Property[key.name='_tag']"(
    node
  ) {
    context.report({
      node,
      message: "Inline tagged-error object passed to Effect.fail; use a Data.TaggedError or Schema.TaggedError class."
    })
  },
  "CallExpression[callee.object.name='Effect'][callee.property.name='fail'] > ObjectExpression.arguments > Property[key.value='_tag']"(
    node
  ) {
    context.report({
      node,
      message: "Inline tagged-error object passed to Effect.fail; use a Data.TaggedError or Schema.TaggedError class."
    })
  },
  "VariableDeclaration[kind='let'] > VariableDeclarator[id.name=/.*(?:sessionId|contextId|hostId).*/u][init.value='']"(
    node
  ) {
    context.report({
      node,
      message: "Do not initialize identity fields with a mutable empty-string placeholder."
    })
  },
  "BinaryExpression[operator='*'] > MemberExpression.left[computed=true][property.value=0][object.property.name=/^(startTime|endTime|duration)$/u]"(
    node
  ) {
    context.report({
      node,
      message: "OpenTelemetry hrtime tuple arithmetic must use bigint helpers to avoid precision loss."
    })
  }
}))

export default {
  meta: {
    name: "effect"
  },
  rules: {
    "no-bigint-literals": noBigintLiterals,
    "no-import-from-barrel-package": noImportFromBarrelPackage,
    "no-js-extension-imports": noJsExtensionImports,
    "no-opaque-instance-fields": noOpaqueInstanceFields,
    "jsdocs": jsdocs,
    "restricted-syntax": restrictedSyntax
  }
}
