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
        "fluent-acp-process is the ACP harness process owner (spawn -> acp.Stream). It must not import the durable-streams substrate or Store/Host/EventIngress/Sources/projection internals. Allowed: @agentclientprotocol/sdk + effect (+ @effect/platform).",
      from: { path: "^packages/fluent-acp-process/src/" },
      to: {
        path: [
          "(^|/)node_modules/(?:\\.pnpm/)?@durable-streams/",
        ],
      },
    },
    {
      name: "fluent-stream-log-is-leaf-package",
      severity: "error",
      comment:
        "fluent-stream-log mirrors eventsourcing-store's Stream/Sink shape: it owns the byte log contract and must not import transport, protocol, client, HTTP/RPC transport, compatibility packages, or execution.",
      from: { path: "^packages/fluent-stream-log/src/" },
      to: {
        path: "^packages/(?:fluent-stream-log-inmemory|fluent-stream-log-s2-lite|fluent-transport|fluent-transport-inmemory|fluent-transport-http|fluent-transport-rpc|fluent-protocol|fluent-client)/src/",
      },
    },
    {
      name: "fluent-stream-log-inmemory-only-imports-stream-log",
      severity: "error",
      comment:
        "fluent-stream-log-inmemory mirrors eventsourcing-store-inmemory's concurrency shape: production code may import fluent-stream-log, but not transport, protocol, client, HTTP/RPC transport, or legacy stores.",
      from: { path: "^packages/fluent-stream-log-inmemory/src/" },
      to: {
        path: "^packages/(?:fluent-stream-log-s2-lite|fluent-transport|fluent-transport-inmemory|fluent-transport-http|fluent-transport-rpc|fluent-protocol|fluent-client)/src/",
      },
    },
    {
      name: "fluent-stream-log-s2-lite-only-imports-stream-log",
      severity: "error",
      comment:
        "fluent-stream-log-s2-lite is a concrete byte-log backend: production code may import fluent-stream-log, but not transport, protocol, client, HTTP/RPC transport, or other concrete stores.",
      from: { path: "^packages/fluent-stream-log-s2-lite/src/" },
      to: {
        path: "^packages/(?:fluent-stream-log-inmemory|fluent-transport|fluent-transport-inmemory|fluent-transport-http|fluent-transport-rpc|fluent-protocol|fluent-client)/src/",
      },
    },
    {
      name: "fluent-transport-is-leaf-package",
      severity: "error",
      comment:
        "fluent-transport mirrors eventsourcing-transport: it owns protocol-agnostic transport contracts and must not import store, protocol, client, concrete transports, or platform HTTP modules.",
      from: { path: "^packages/fluent-transport/src/" },
      to: {
        path: "^packages/(?:fluent-stream-log|fluent-stream-log-inmemory|fluent-stream-log-s2-lite|fluent-transport-inmemory|fluent-transport-http|fluent-transport-rpc|fluent-protocol|fluent-client)/src/",
      },
    },
    {
      name: "fluent-transport-inmemory-only-imports-transport",
      severity: "error",
      comment:
        "fluent-transport-inmemory mirrors eventsourcing-transport-inmemory: production code may import fluent-transport only, not protocol, store, client, or other concrete transports.",
      from: { path: "^packages/fluent-transport-inmemory/src/" },
      to: {
        path: "^packages/(?:fluent-stream-log|fluent-stream-log-inmemory|fluent-stream-log-s2-lite|fluent-transport-http|fluent-transport-rpc|fluent-protocol|fluent-client)/src/",
      },
    },
    {
      name: "fluent-transport-http-only-imports-transport",
      severity: "error",
      comment:
        "fluent-transport-http implements DurableTransport over HTTP/SSE: it may import fluent-protocol and Effect platform, but not stream-log internals, raw transport, concrete stores/transports, or client.",
      from: { path: "^packages/fluent-transport-http/src/" },
      to: {
        path: "^packages/(?:fluent-stream-log|fluent-stream-log-inmemory|fluent-stream-log-s2-lite|fluent-transport|fluent-transport-inmemory|fluent-transport-rpc|fluent-client)/src/",
      },
    },
    {
      name: "fluent-transport-rpc-only-imports-transport",
      severity: "error",
      comment:
        "fluent-transport-rpc is a concrete transport: it may import fluent-transport, but not protocol, store, client, or other concrete transports.",
      from: { path: "^packages/fluent-transport-rpc/src/" },
      to: {
        path: "^packages/(?:fluent-stream-log|fluent-stream-log-inmemory|fluent-stream-log-s2-lite|fluent-transport-inmemory|fluent-transport-http|fluent-protocol|fluent-client)/src/",
      },
    },
    {
      name: "fluent-protocol-only-imports-stream-log",
      severity: "error",
      comment:
        "fluent-protocol owns Durable Streams protocol algebra and may import fluent-stream-log only; raw transport, concrete transports, concrete stores, and client stay outside.",
      from: { path: "^packages/fluent-protocol/src/" },
      to: {
        path: "^packages/(?:fluent-stream-log-inmemory|fluent-stream-log-s2-lite|fluent-transport|fluent-transport-inmemory|fluent-transport-http|fluent-transport-rpc|fluent-client)/src/",
      },
    },
    {
      name: "fluent-client-no-concrete-or-server-runtime",
      severity: "error",
      comment:
        "fluent-client may use fluent-protocol only; runtime code must not import raw transport, concrete in-memory transports/stores, HTTP/RPC transport internals, compatibility packages, or execution.",
      from: { path: "^packages/fluent-client/src/" },
      to: {
        path: "^packages/(?:fluent-stream-log|fluent-stream-log-inmemory|fluent-stream-log-s2-lite|fluent-transport|fluent-transport-inmemory|fluent-transport-http|fluent-transport-rpc)/src/",
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
