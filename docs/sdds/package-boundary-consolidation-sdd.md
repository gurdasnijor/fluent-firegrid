# SDD: Package Boundary Consolidation

### Re-centering package boundaries around iteration instead of early extraction

|   |   |
| --- | --- |
| Status | Implemented; greenfield compatibility shims removed |
| Date | 2026-06-26 |
| Package focus | `@firegrid/core`, `@firegrid/fluent`, `@firegrid/log`, `@firegrid/trace` |

---

## Decision

The current package split is too granular for the maturity of the runtime and
authoring APIs. We should consolidate the product-facing workflow stack into a
single iteration package for now, then extract packages only after the internal
module boundaries have proven stable.

Implemented near-term shape:

- `@firegrid/fluent` becomes the main product package for authoring, clients,
  runtime driving, host composition, HTTP binding, testing helpers, and S2
  adapters.
- `@firegrid/log` remains a low-level S2 client package only if it has direct
  non-Fluent consumers; otherwise treat it as an internal S2 substrate module.
- `@firegrid/trace` can remain separate because it is observability
  infrastructure, not part of the workflow authoring/runtime loop.
- `@firegrid/runtime`, `@firegrid/clients`, and `@firegrid/store` were removed
  instead of kept as compatibility re-exports. This repo is greenfield, and
  trivial package shims obscure ownership without protecting a real external
  contract.
- `@firegrid/core` remains as the shared contract package for now, but should
  be shrunk only after the consolidated Fluent package stabilizes.

The principle is: first find stable module seams inside one package, then
promote proven seams to packages.

## Package Inventory Before Consolidation

### `@firegrid/core`

Current exports include:

- invocation request/response types;
- `createTanStackRuntimeBinding`;
- duration helpers;
- object state backend contracts;
- CEL state predicate helpers;
- materialized state helpers;
- workflow definition helpers;
- middleware;
- workflow engine functions;
- server parsing helpers;
- workflow registry/version selection;
- in-memory run store;
- shared workflow/run/error types.

This is not a small core. It mixes domain types, engine code, invocation
transport, state storage contracts, server helpers, and test storage. It has
become a dumping ground for anything that multiple packages need.

### `@firegrid/runtime`

Current exports include:

- `export * from "@firegrid/core"`;
- runtime definition builder;
- schedule constructors;
- in-memory workflow execution store;
- run-store adapter;
- runtime driver;
- schedule materializer;
- broad runtime/store/schedule/lease type surface.

This package is not just a vendored runtime. It re-exports `core`, owns runtime
driving, and owns store contracts. That makes it hard to tell whether a type is
domain, engine, host, or storage.

### `@firegrid/clients`

Current exports include:

- typed dynamic call/send client factories;
- object client factories;
- generic invocation helpers;
- attach/invocation handle helpers;
- request construction;
- aliases such as `serviceClient`, `workflowClient`, `objectClient`, and `rpc`.

This is a thin package over `@firegrid/core` and Effect. Its main consumer is
`@firegrid/fluent`, which re-exports most of it. It does not currently justify a
separate package boundary.

### `@firegrid/fluent`

Current exports include:

- fluent definitions;
- descriptor-first `iface` / `implement`;
- client helpers exported from `@firegrid/fluent` and `@firegrid/fluent/clients`;
- durable run/sleep/wait primitives;
- TanStack binding;
- durable context service;
- external events and awakeables;
- state/table/materialization helpers;
- state predicate helpers;
- HTTP binding;
- Effect combinators.

This is the product-facing package. It already aggregates the public workflow
SDK. That makes it the best place to keep iteration until the API stabilizes.

### `@firegrid/store`

Current exports include:

- `s2WorkflowExecutionStore`;
- S2 object state backend;
- S2 object runtime binding;
- delayed object invocation streams;
- S2 fluent definition binding options;
- `defineS2WorkflowRuntime`;
- `createS2WorkflowRuntimeHost`;
- runtime/store types now exported from `@firegrid/fluent/runtime` and
  `@firegrid/fluent/s2`;
- `LogConflictError`.

This is the clearest boundary problem. A package named `store` now creates
runtime definitions and runtime hosts. It is no longer just a persistence
adapter.

### `@firegrid/log`

Current exports include:

- Effect service wrappers for the S2 SDK;
- basin/stream/control-plane APIs;
- append/read/read-session APIs;
- producer and append-session APIs;
- S2 config/layer/service types;
- generated S2 API subpath.

This is a coherent substrate package. The open question is not whether the code
is coherent; it is whether Fluent should publish it as a public package or keep
it as an implementation dependency.

### `@firegrid/trace`

Current exports include:

- chDB client/layer;
- SQL literal helpers;
- OpenTelemetry span row conversion;
- chDB span exporters.

This can remain separate. It is optional infrastructure and does not create
confusing workflow runtime boundaries.

## Dependency Shape Before Consolidation

Current package dependencies flow like this:

```text
clients -> core
runtime -> core
fluent  -> clients, core, runtime
store   -> core, log, runtime
log     -> S2 SDK, Effect
trace   -> chdb, OpenTelemetry, Effect
```

The problematic direction is `store -> runtime`. A storage adapter should not be
the package that composes runtime definitions and host loops. That dependency is
why `@firegrid/store` can expose `createS2WorkflowRuntimeHost`.

The other problem is `runtime -> export * from core`. It hides ownership
instead of clarifying it.

## Misshapen Abstractions

### `store` Owns Host Composition

`createS2WorkflowRuntimeHost` is useful, but it does not belong in a store
package. It combines:

- an S2 execution store;
- workflow registrations;
- schedule materialization;
- stale-run recovery;
- runtime sweep;
- an operational polling loop.

That is host composition. It should live under a host/runtime/S2 adapter module,
not under a package whose name implies persistence.

### `store` Owns Object Invocation Runtime Binding

`createS2ObjectRuntimeBinding` does more than store object state. It handles
object invocation ordering, delayed starts, leases, state waits, completion
polling, and invocation references.

That is an S2-backed object runtime adapter. It should not be presented as a
generic store abstraction.

### `core` Is Too Broad

`@firegrid/core` currently owns engine code, invocation binding, state backend
contracts, server helpers, and run stores. A true core package should contain
types and pure helpers that have no reason to know about runtime hosts or
transport bindings.

Until that line is clear, keeping `core` as a package makes every other package
reach into it for unrelated concepts.

### `clients` Is Premature

The client package is a typed helper layer over definitions and invocation
bindings. Since `fluent` is the product-facing package and re-exports clients
anyway, `clients` is extra package management without much independent value.

Keep it as an internal module until a non-Fluent consumer proves the separate
package is needed.

### `runtime` Is Both Engine And Contract

The former `@firegrid/runtime` package contained runtime driver behavior, store interfaces,
schedule helpers, and type exports. It is useful code, but not yet a clean
package boundary. The runtime is evolving together with Fluent authoring and S2
hosting, so forcing package separation increases churn.

## Implemented Consolidated Shape

Move toward one main package with internal modules:

```text
packages/fluent/src/
  authoring/
    definitions.ts
    interface.ts
    run.ts
    state.ts
    externalEvents.ts
  clients/
    clients.ts
    invocation.ts
  runtime/
    defineRuntime.ts
    runtimeDriver.ts
    scheduleMaterializer.ts
    inMemoryStore.ts
    types.ts
  adapters/
    tanstack.ts
    http.ts
    s2/
      workflowExecutionStore.ts
      workflowRuntimeHost.ts
      objectStateBackend.ts
      objectRuntimeBinding.ts
      delayedStarts.ts
  substrate/
    s2Log.ts
  testing/
    inMemory.ts
```

Export subpaths from `@firegrid/fluent`:

```json
{
  ".": "./src/index.ts",
  "./runtime": "./src/runtime/index.ts",
  "./s2": "./src/adapters/s2/index.ts",
  "./http": "./src/adapters/http.ts",
  "./testing": "./src/testing/index.ts"
}
```

This keeps the namespaces clear without forcing separate packages:

- `@firegrid/fluent` for app authors;
- `@firegrid/fluent/runtime` for host/runtime integration;
- `@firegrid/fluent/s2` for S2-backed deployment;
- `@firegrid/fluent/http` for transport binding;
- `@firegrid/fluent/testing` for in-memory stores and fixtures.

## What Moved

### Moved `@firegrid/clients` Into `fluent/clients`

The implementation lives under `packages/fluent/src/clients`. The
`@firegrid/clients` package was removed. Use `@firegrid/fluent` or
`@firegrid/fluent/clients`.

### Moved `@firegrid/runtime` Into `fluent/runtime`

Runtime driver, schedule materializer, runtime types, and in-memory execution
store now live together under `packages/fluent/src/runtime`.

The `@firegrid/runtime` package was removed. Use `@firegrid/fluent/runtime`.

### Moved `@firegrid/store` Into `fluent/adapters/s2`

Rename by responsibility:

- `s2WorkflowExecutionStore` -> `createS2WorkflowExecutionStore`
- `createS2WorkflowRuntimeHost` -> `createS2RuntimeHost`
- `defineS2WorkflowRuntime` -> remove or fold into `createS2RuntimeHost`
- `createS2ObjectStateBackend` -> `createS2ObjectStateBackend`
- `createS2ObjectRuntimeBinding` -> `createS2ObjectInvocationBinding`

The implementation lives under `packages/fluent/src/adapters/s2`. The
`@firegrid/store` package was removed. Use `@firegrid/fluent/s2`.

### Deferred: Shrink `@firegrid/core` Into Internal Domain Modules

Split the current contents by responsibility inside `@firegrid/fluent`:

- `domain/types.ts`
- `domain/errors.ts`
- `domain/stateMessages.ts`
- `domain/statePredicates.ts`
- `runtime/workflowEngine.ts`
- `runtime/runStoreAdapter.ts`
- `transport/invocation.ts`

If a true `@firegrid/core` is extracted later, it should be small: domain
types, pure state/event helpers, and no runtime host factories.

### Keep Or Internalize `@firegrid/log`

There are two reasonable choices:

1. Keep `@firegrid/log` public because it is a coherent S2 Effect facade.
2. Mark it internal while Fluent is iterating and expose only
   `@firegrid/fluent/s2`.

Either is better than letting `@firegrid/store` be the public S2 product API,
so the package has been removed in favor of the `@firegrid/fluent/s2` subpath.

### Keep `@firegrid/trace` Separate

Trace is not blocking the workflow package design. Keep it separate unless it
starts importing Fluent internals.

## Proposed Public API After Consolidation

Application authors:

```ts
import { iface, implement, run, serviceClient } from "@firegrid/fluent"
```

HTTP binding:

```ts
import { createFluentHttpHandler } from "@firegrid/fluent/http"
```

S2 deployment:

```ts
import { createS2RuntimeHost, s2BindingOptions } from "@firegrid/fluent/s2"
```

Tests:

```ts
import { createInMemoryRuntimeHost } from "@firegrid/fluent/testing"
```

Low-level S2, only if retained:

```ts
import { S2Client } from "@firegrid/log"
```

## Package Extraction Criteria

Do not extract a package until all of these are true:

1. It has at least two real consumers that do not import the parent package.
2. Its public API has been stable across at least one implementation change.
3. It can explain its responsibility in one sentence without mentioning a
   sibling package.
4. It does not need to re-export a sibling package wholesale.
5. It can be tested with fixtures that do not require the full product stack.

By this standard:

- `trace` passes.
- `log` may pass.
- `core` does not pass yet.
- `clients`, `runtime`, and `store` are no longer packages; they are internal
  Fluent submodules/subpaths while the API stabilizes.

## Follow-Up Migration Plan

1. Stop adding new public packages for Fluent workflow concerns.
2. Keep `clients`, `runtime`, and `s2` as `@firegrid/fluent` subpaths until a
   real independent ownership boundary emerges.
3. Shrink `@firegrid/core` only after the consolidated Fluent internals have
   stabilized.

## Specific Store Recommendation

The immediate store cleanup should be:

- remove `defineS2WorkflowRuntime` from the public store API;
- move `createS2WorkflowRuntimeHost` to `@firegrid/fluent/s2`;
- move `createS2ObjectRuntimeBinding` to `@firegrid/fluent/s2`;
- keep `s2WorkflowExecutionStore` as an internal S2 persistence adapter;
- keep `createS2ObjectStateBackend` as an internal S2 object-state adapter;
- rename the package/module around S2 deployment, not generic storage.

The helper function for creating a runtime feels wrong because the abstraction
is wrong: the function is not storing anything. It is composing a host.

## Final Recommendation

Consolidate now. Treat package boundaries as extraction results, not design
inputs.

The product is still discovering the right shapes for authoring, durable
runtime driving, object state, S2 deployment, and clients. Keeping those in one
package with clear internal folders will make iteration faster and make later
extraction much cleaner.
