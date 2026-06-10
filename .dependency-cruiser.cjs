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
    {
      name: "fluent-store-is-leaf-package",
      severity: "error",
      comment:
        "fluent-store mirrors eventsourcing-store: it owns store contracts and must not import transport, protocol, server, client, HTTP/RPC transport, compatibility packages, or execution.",
      from: { path: "^packages/fluent-store/src/" },
      to: {
        path: "^packages/(?:fluent-store-inmemory|fluent-transport|fluent-transport-inmemory|fluent-transport-http|fluent-transport-rpc|fluent-protocol|fluent-server|fluent-client|effect-durable-streams|effect-durable-client|effect-durable-execution)/src/",
      },
    },
    {
      name: "fluent-store-inmemory-only-imports-store",
      severity: "error",
      comment:
        "fluent-store-inmemory mirrors eventsourcing-store-inmemory: production code may import fluent-store, but not transport, protocol, server, client, HTTP/RPC transport, or legacy stores.",
      from: { path: "^packages/fluent-store-inmemory/src/" },
      to: {
        path: "^packages/(?:fluent-transport|fluent-transport-inmemory|fluent-transport-http|fluent-transport-rpc|fluent-protocol|fluent-server|fluent-client|effect-durable-streams|effect-durable-client|effect-durable-execution)/src/",
      },
    },
    {
      name: "fluent-transport-is-leaf-package",
      severity: "error",
      comment:
        "fluent-transport mirrors eventsourcing-transport: it owns protocol-agnostic transport contracts and must not import store, protocol, server, client, concrete transports, or platform HTTP modules.",
      from: { path: "^packages/fluent-transport/src/" },
      to: {
        path: "^packages/(?:fluent-store|fluent-store-inmemory|fluent-transport-inmemory|fluent-transport-http|fluent-transport-rpc|fluent-protocol|fluent-server|fluent-client|effect-durable-streams|effect-durable-client|effect-durable-execution)/src/",
      },
    },
    {
      name: "fluent-transport-inmemory-only-imports-transport",
      severity: "error",
      comment:
        "fluent-transport-inmemory mirrors eventsourcing-transport-inmemory: production code may import fluent-transport only, not protocol, store, server, client, or other concrete transports.",
      from: { path: "^packages/fluent-transport-inmemory/src/" },
      to: {
        path: "^packages/(?:fluent-store|fluent-store-inmemory|fluent-transport-http|fluent-transport-rpc|fluent-protocol|fluent-server|fluent-client|effect-durable-streams|effect-durable-client|effect-durable-execution)/src/",
      },
    },
    {
      name: "fluent-transport-http-only-imports-transport",
      severity: "error",
      comment:
        "fluent-transport-http must stay a concrete transport like eventsourcing-transport-websocket: it may import fluent-transport, but not protocol, store, server, client, or other concrete transports.",
      from: { path: "^packages/fluent-transport-http/src/" },
      to: {
        path: "^packages/(?:fluent-store|fluent-store-inmemory|fluent-transport-inmemory|fluent-transport-rpc|fluent-protocol|fluent-server|fluent-client|effect-durable-streams|effect-durable-client|effect-durable-execution)/src/",
      },
    },
    {
      name: "fluent-transport-rpc-only-imports-transport",
      severity: "error",
      comment:
        "fluent-transport-rpc is a concrete transport: it may import fluent-transport, but not protocol, store, server, client, or other concrete transports.",
      from: { path: "^packages/fluent-transport-rpc/src/" },
      to: {
        path: "^packages/(?:fluent-store|fluent-store-inmemory|fluent-transport-inmemory|fluent-transport-http|fluent-protocol|fluent-server|fluent-client|effect-durable-streams|effect-durable-client|effect-durable-execution)/src/",
      },
    },
    {
      name: "fluent-protocol-only-imports-store-and-transport",
      severity: "error",
      comment:
        "fluent-protocol mirrors eventsourcing-protocol: production code may import fluent-store and fluent-transport, but not concrete implementations, server, client, HTTP/RPC transport, or compatibility packages.",
      from: { path: "^packages/fluent-protocol/src/" },
      to: {
        path: "^packages/(?:fluent-store-inmemory|fluent-transport-inmemory|fluent-transport-http|fluent-transport-rpc|fluent-server|fluent-client|effect-durable-streams|effect-durable-client|effect-durable-execution)/src/",
      },
    },
    {
      name: "fluent-server-only-imports-store",
      severity: "error",
      comment:
        "fluent-server mirrors eventsourcing-server: production code may import fluent-store only; protocol and concrete transports stay outside the semantic server.",
      from: { path: "^packages/fluent-server/src/" },
      to: {
        path: "^packages/(?:fluent-store-inmemory|fluent-transport-inmemory|fluent-transport-http|fluent-transport-rpc|fluent-transport|fluent-protocol|fluent-client|effect-durable-streams|effect-durable-client|effect-durable-execution)/src/",
      },
    },
    {
      name: "fluent-client-no-concrete-or-server-runtime",
      severity: "error",
      comment:
        "fluent-client may use fluent-protocol only; runtime code must not import raw transport, concrete in-memory transports/stores, server implementation, HTTP/RPC transport internals, compatibility packages, or execution.",
      from: { path: "^packages/fluent-client/src/" },
      to: {
        path: "^packages/(?:fluent-store|fluent-store-inmemory|fluent-transport|fluent-transport-inmemory|fluent-transport-http|fluent-transport-rpc|fluent-server|effect-durable-streams|effect-durable-client|effect-durable-execution)/src/",
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
