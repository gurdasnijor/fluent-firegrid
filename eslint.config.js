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

const tsOnly = (configs) =>
  configs.map((config) => ({
    ...config,
    files: sourceFiles,
  }))

const relativeJsSpecifierPattern = /^\.{1,2}\/.*\.js$/u
const rewriteJsSpecifierToTs = (specifier) => specifier.replace(/\.js$/u, ".ts")

const effectRuntimeGuardrails = [
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
    message: "Effect.runFork creates an unmanaged fiber unless it is scoped deliberately.",
  },
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

const packageRuntimeGuardrails = [
  {
    selector: "CallExpression[callee.object.name='Schedule'][callee.property.name=/^(fixed|recurs|spaced)$/]",
    message: "Avoid fixed polling in durable runtime code; drive work from durable subscriptions, state folds, or explicit deadlines.",
  },
  {
    selector: "CallExpression[callee.object.name='Stream'][callee.property.name='tick']",
    message: "Avoid fixed polling in durable runtime code; drive work from durable subscriptions, state folds, or explicit deadlines.",
  },
  {
    selector:
      ":matches(ForStatement,ForInStatement,ForOfStatement,WhileStatement,DoWhileStatement) CallExpression[callee.object.name='Effect'][callee.property.name='sleep']",
    message: "Avoid sleep-in-loop polling in durable runtime code.",
  },
  {
    selector: "Program > VariableDeclaration[kind='let']",
    message: "Avoid module-scope mutable state in durable runtime code; derive restart state from durable state.",
  },
  {
    selector:
      "Program > VariableDeclaration > VariableDeclarator[id.name=/.*(?:cache|registry|registries|runs|claims|completions|pending|subscribers|eventplanes?|eventPlane).*/u][init.type=/^(ArrayExpression|ObjectExpression)$/]",
    message: "Avoid module-scope durable-authority caches or registries; durable state must remain the source of truth.",
  },
  {
    selector:
      "Program > VariableDeclaration > VariableDeclarator[id.name=/.*(?:cache|registry|registries|runs|claims|completions|pending|subscribers|eventplanes?|eventPlane).*/u] > NewExpression.init[callee.name=/^(Map|Set|WeakMap|WeakSet)$/]",
    message: "Avoid module-scope durable-authority caches or registries; durable state must remain the source of truth.",
  },
  {
    selector:
      ":matches(VariableDeclarator,FunctionDeclaration,ClassDeclaration)[id.name=/.*(?:(?:run|completion|claim|eventPlane).*(?:cache|registry)|(?:cache|registry).*(?:run|completion|claim|eventPlane)).*/u]",
    message: "Host code must not define durable-authority registries/caches for runs, completions, claims, or event planes.",
  },
]

const packageQualityGuardrails = [
  {
    selector: "ImportExpression[source.type='Literal']",
    message: "Do not use dynamic import() with a literal specifier; import modules known at author time statically.",
  },
  {
    selector: "NewExpression[callee.name='DurableStream']",
    message: "Do not construct DurableStream directly; go through DurableTable or the declared stream factories.",
  },
  {
    selector: "ForOfStatement",
    message: "Avoid imperative for-of in library source; use Array, Stream, or Chunk combinators.",
  },
  {
    selector: "CallExpression[callee.object.name='Workflow'][callee.property.name='make']",
    message: "Net-new Workflow.make is an owned durable workflow; justify it in the workflow admission ledger.",
  },
  {
    selector:
      "CallExpression[callee.object.name='Effect'][callee.property.name='sync'] UnaryExpression[operator='void'] CallExpression[callee.property.name='then']",
    message: "Detached promise work inside Effect.sync escapes the Effect runtime; model the async work as an Effect.",
  },
  {
    selector:
      "CallExpression[callee.object.name='Effect'][callee.property.name='fail'] > ObjectExpression.arguments > Property[key.name='_tag']",
    message: "Inline tagged-error object passed to Effect.fail; use a Data.TaggedError or Schema.TaggedError class.",
  },
  {
    selector:
      "CallExpression[callee.object.name='Effect'][callee.property.name='fail'] > ObjectExpression.arguments > Property[key.value='_tag']",
    message: "Inline tagged-error object passed to Effect.fail; use a Data.TaggedError or Schema.TaggedError class.",
  },
  {
    selector:
      "VariableDeclaration[kind='let'] > VariableDeclarator[id.name=/.*(?:sessionId|contextId|hostId).*/u][init.value='']",
    message: "Do not initialize identity fields with a mutable empty-string placeholder.",
  },
]

const telemetryGuardrails = [
  {
    selector:
      "BinaryExpression[operator='*'] > MemberExpression.left[computed=true][property.value=0][object.property.name=/^(startTime|endTime|duration)$/u]",
    message: "OpenTelemetry hrtime tuple arithmetic must use bigint helpers to avoid precision loss.",
  },
]

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
  },
}

export default tseslint.config(
  {
    ignores: [
      "node_modules/**",
      "coverage/**",
      "**/dist/**",
      ".eslintcache",
      "*.tsbuildinfo",
      "repos/**",
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
      "@typescript-eslint/no-base-to-string": "error",
      "@typescript-eslint/no-confusing-void-expression": "off",
      "@typescript-eslint/no-floating-promises": "error",
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
      "@stylistic/comma-dangle": ["error", "always-multiline"],
      "@stylistic/eol-last": ["error", "always"],
      "@stylistic/quotes": ["error", "double", { avoidEscape: true }],
      "@stylistic/semi": ["error", "never"],
      "local/relative-ts-extensions": "error",
      "no-restricted-syntax": [
        "error",
        ...effectRuntimeGuardrails,
        ...telemetryGuardrails,
      ],
      "no-undef": "off",
      "no-unused-labels": "error",
      "no-useless-assignment": "error",
      "no-var": "error",
      "object-shorthand": "error",
      "prefer-const": "error",
      "prefer-template": "error",
      "require-yield": "off",
    },
  },
  {
    files: packageSourceFiles,
    ignores: testFiles,
    rules: {
      "no-restricted-syntax": [
        "error",
        ...effectRuntimeGuardrails,
        ...packageRuntimeGuardrails,
        ...packageQualityGuardrails,
        ...telemetryGuardrails,
      ],
    },
  },
  {
    files: testFiles,
    rules: {
      "@stylistic/quotes": "off",
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
      "no-restricted-syntax": "off",
      "no-undef": "off",
    },
  },
)
