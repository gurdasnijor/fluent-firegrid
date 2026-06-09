module.exports = {
  forbidden: [
    {
      name: "not-to-unresolvable",
      severity: "error",
      comment:
        "This module depends on a module that cannot be found. If it is an npm module, add it to package.json; otherwise fix the import path.",
      from: {},
      to: { couldNotResolve: true },
    },
    {
      name: "no-non-package-json",
      severity: "error",
      comment:
        "This module depends on an npm package that is not declared in package.json.",
      from: {},
      to: {
        dependencyTypes: ["npm-no-pkg", "npm-unknown"],
      },
    },
    {
      name: "not-to-deprecated",
      severity: "error",
      comment:
        "This module uses a deprecated npm package. Upgrade it or replace it.",
      from: {},
      to: {
        dependencyTypes: ["deprecated"],
      },
    },
    {
      name: "no-duplicate-dep-types",
      severity: "error",
      comment:
        "This module depends on an external package declared in more than one dependency bucket.",
      from: {},
      to: {
        moreThanOneDependencyType: true,
        dependencyTypesNot: ["type-only"],
      },
    },
    {
      name: "not-to-test-from-production",
      severity: "error",
      comment:
        "Production code must not import from test files. Factor shared helpers into production or test utility modules instead.",
      from: {
        path: "^packages/.*/src",
        pathNot: [
          "\\.test\\.(?:ts|tsx|mts)$",
          "^packages/.*/src/__tests__/",
        ],
      },
      to: {
        path: "[.](?:spec|test)[.](?:js|mjs|cjs|jsx|ts|mts|cts|tsx)$",
      },
    },
    {
      name: "no-circular",
      severity: "error",
      from: {},
      to: { circular: true },
    },
    {
      name: "fluent-acp-process-tiny-import-surface",
      severity: "error",
      comment:
        "fluent-acp-process is the ACP harness process owner (spawn -> acp.Stream). It must not import fluent-runtime, durable-streams substrate, or any Store/Host/EventIngress/Sources/projection internals. Allowed: @agentclientprotocol/sdk + effect (+ @effect/platform).",
      from: { path: "^packages/fluent-acp-process/src/" },
      to: {
        path: [
          "^packages/fluent-runtime/",
          "effect-durable-streams",
          "(^|/)node_modules/(?:\\.pnpm/)?@durable-streams/",
        ],
      },
    },
    {
      name: "fluent-runtime-no-legacy-runtime",
      severity: "error",
      comment:
        "fluent-runtime is the lean managed-agent runtime workbench. It must not depend on the legacy workflow engine.",
      from: { path: "^packages/fluent-runtime/src" },
      to: {
        path: [
          "^node_modules/@effect/workflow",
          "@effect/workflow",
        ],
      },
    },
    {
      name: "effect-durable-execution-no-server-substrate",
      severity: "error",
      comment:
        "effect-durable-execution owns execution semantics and must stay above the server substrate; durable stream server internals stay behind the client package.",
      from: { path: "^packages/effect-durable-execution/src/" },
      to: {
        path: [
          "^packages/fluent-runtime/src",
          "effect-durable-streams",
          "^node_modules/@effect/workflow",
          "@effect/workflow",
          "(^|/)node_modules/(?:\\.pnpm/)?@durable-streams/",
        ],
      },
    },
  ],
  options: {
    tsConfig: { fileName: "tsconfig.eslint.json" },
    doNotFollow: { path: "node_modules" },
    includeOnly: "^packages/.*/src",
    enhancedResolveOptions: { exportsFields: ["exports"] },
  },
}
