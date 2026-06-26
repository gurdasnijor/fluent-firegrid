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
      to: { dependencyTypes: ["npm-no-pkg", "npm-unknown"] },
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
      name: "production-not-to-apps",
      severity: "error",
      comment:
        "Production packages must not import examples, proof harnesses, or app composition code.",
      from: { path: "^packages/[^/]+/src/" },
      to: { path: "^apps/" },
    },
    {
      name: "core-not-to-firegrid-packages",
      severity: "error",
      comment:
        "@firegrid/core is the shared contract. It must not import other Firegrid packages.",
      from: { path: "^packages/core/src/" },
      to: { path: "^packages/(?!core/)[^/]+/" },
    },
    {
      name: "log-not-to-product-packages",
      severity: "error",
      comment:
        "@firegrid/log is the raw S2 log substrate and must not import product/runtime packages.",
      from: { path: "^packages/log/src/" },
      to: { path: "^packages/(?!log/)[^/]+/" },
    },
    {
      name: "trace-not-to-product-packages",
      severity: "error",
      comment:
        "@firegrid/trace is an observability sink and must not import product/runtime packages.",
      from: { path: "^packages/trace/src/" },
      to: { path: "^packages/(?!trace/)[^/]+/" },
    },
    {
      name: "fluent-not-to-trace-or-apps",
      severity: "error",
      comment:
        "@firegrid/fluent is the product package. Runtime and S2 adapters live inside it; keep trace sinks and apps out.",
      from: { path: "^packages/fluent/src/" },
      to: { path: "^packages/trace/|^apps/" },
    },
    {
      name: "proof-runtime-not-to-proof-registration",
      severity: "error",
      comment:
        "The proof harness runtime must not import concrete proof registrations.",
      from: { path: "^apps/proofs/src/" },
      to: { path: "^apps/proofs/proofs/" },
    },
  ],
  options: {
    tsConfig: { fileName: "tsconfig.json" },
    doNotFollow: { path: "node_modules" },
    includeOnly: "^(?:packages/.*/src|apps/proofs/(?:src|proofs)|apps/acp-process/src|apps/examples/full-stack-service/src)",
    enhancedResolveOptions: { exportsFields: ["exports"] },
  },
}
