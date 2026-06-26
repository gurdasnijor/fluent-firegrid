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
      name: "production-not-to-verification",
      severity: "error",
      comment:
        "Production packages must not import verification infrastructure or concrete proofs.",
      from: {
        path: "^packages/(?!verification/)[^/]+/src/",
      },
      to: {
        path: "^packages/verification/",
      },
    },
    {
      name: "fluent-authoring-not-to-transport-runtime-or-substrate",
      severity: "error",
      comment:
        "@firegrid/fluent-firegrid is the transport-neutral authoring surface. Keep HTTP, Node, S2 binding, TanStack/S2 store, raw S2 substrate, and verification imports below it.",
      from: { path: "^packages/fluent-firegrid/src/" },
      to: {
        path:
          "^packages/(?:fluent-firegrid-http|fluent-firegrid-node|fluent-firegrid-s2|tanstack-workflow-s2|effect-s2|verification)/",
      },
    },
    {
      name: "fluent-http-not-to-node-s2-or-verification",
      severity: "error",
      comment:
        "@firegrid/fluent-firegrid-http is a transport binding. Keep Node hosting, S2 runtime binding, raw S2 substrate, and verification imports out of it.",
      from: { path: "^packages/fluent-firegrid-http/src/" },
      to: {
        path:
          "^packages/(?:fluent-firegrid-node|fluent-firegrid-s2|tanstack-workflow-s2|effect-s2|verification)/",
      },
    },
    {
      name: "fluent-s2-not-to-transport-host-or-verification",
      severity: "error",
      comment:
        "@firegrid/fluent-firegrid-s2 is the S2 runtime binding. Keep HTTP transport, Node hosting, and verification imports out of it.",
      from: { path: "^packages/fluent-firegrid-s2/src/" },
      to: {
        path: "^packages/(?:fluent-firegrid-http|fluent-firegrid-node|verification)/",
      },
    },
    {
      name: "tanstack-core-runtime-not-to-firegrid-or-s2-store",
      severity: "error",
      comment:
        "Vendored TanStack core/runtime packages should stay generic and must not depend on Firegrid product APIs, S2 stores, raw S2 substrate, or verification.",
      from: { path: "^packages/tanstack-workflow-(?:core|runtime)/src/" },
      to: {
        path:
          "^packages/(?:fluent-firegrid|fluent-firegrid-http|fluent-firegrid-node|fluent-firegrid-s2|tanstack-workflow-s2|effect-s2|verification|observability)/",
      },
    },
    {
      name: "tanstack-s2-store-not-to-fluent-or-verification",
      severity: "error",
      comment:
        "@firegrid/tanstack-workflow-s2 is a lower-level store/runtime adapter. Keep fluent product APIs and verification imports out of it.",
      from: { path: "^packages/tanstack-workflow-s2/src/" },
      to: {
        path:
          "^packages/(?:fluent-firegrid|fluent-firegrid-http|fluent-firegrid-node|fluent-firegrid-s2|verification)/",
      },
    },
    {
      name: "effect-s2-substrate-not-to-product-packages",
      severity: "error",
      comment:
        "effect-s2 is the raw S2 substrate client. It must not import Firegrid product, TanStack store/runtime, verification, or observability packages.",
      from: { path: "^packages/effect-s2/src/" },
      to: {
        path:
          "^packages/(?:fluent-firegrid|fluent-firegrid-http|fluent-firegrid-node|fluent-firegrid-s2|tanstack-workflow-core|tanstack-workflow-runtime|tanstack-workflow-s2|verification|observability)/",
      },
    },
    {
      name: "fluent-acp-process-tiny-import-surface",
      severity: "error",
      comment:
        "fluent-acp-process is the ACP harness process owner (spawn -> acp.Stream). It must not import the durable-streams substrate or Store/Host/EventIngress/Sources/projection internals. Allowed: @agentclientprotocol/sdk + effect (+ @effect/platform).",
      from: { path: "^packages/fluent-acp-process/src/" },
      to: {
        path: [
          "(^|/)node_modules/(?:\\.pnpm/)?@durable-streams/",
        ],
      },
    },
    {
      name: "verification-runtime-not-to-proofs",
      severity: "error",
      comment:
        "The verification runtime must not import concrete proofs. Keep proof registration in packages/verification/proofs so src stays reusable verification infrastructure.",
      from: { path: "^packages/verification/src/" },
      to: { path: "^packages/verification/proofs/" },
    },
  ],
  options: {
    tsConfig: { fileName: "tsconfig.json" },
    doNotFollow: { path: "node_modules" },
    includeOnly: "^packages/.*/(?:src|proofs)",
    enhancedResolveOptions: { exportsFields: ["exports"] },
  },
}
