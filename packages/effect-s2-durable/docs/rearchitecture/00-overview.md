# Rearchitecture Overview

## Problem

Before the cutover, the package centered too much behavior in a generic
`Runtime.ts` module. After the cutover, the remaining implementation hotspot is
`src/engine/live.ts`, which still owns too many responsibilities:

- live engine API assembly;
- Effect layer composition;
- service submit / run / complete / recover;
- object submit / workflow start / run-head / body execution;
- shared object calls;
- child `call` / `send` routing;
- boot recovery;
- final assembly of primitive, completion, and ingress services.

That made the package feel like it had several hidden runtimes. The deeper issue
was not file count; it was that durable protocol decisions were spread across
drivers and services instead of being expressed as a state machine.

## Target

The package should read as:

```text
public API / host / ingress
  -> engine facade
  -> durable state machine
  -> ports and drivers
  -> S2 typed streams/tables
```

The state machine owns durable semantics. Drivers own IO and process-local
effects.

## Top-Level Design Axes

- **State machine first**: commands plus projection produce events and driver
  actions. See [`01-state-machine.md`](./01-state-machine.md).
- **Actor ergonomics above the machine**: actor/action contracts, invocation
  context, typed state facade, and dispatcher boundaries should sit above the
  state machine so the machine does not become the whole architecture. See
  [`01-state-machine.md`](./01-state-machine.md).
- **Storage substrate**: object owner logs are typed event streams; latest-value
  state remains tables/materialized views. See
  [`02-storage-substrate.md`](./02-storage-substrate.md).
- **Capability boundaries**: keep `DurableEngine` narrow and move handler
  primitives behind invocation-scoped capability objects. See
  [`03-capability-boundaries.md`](./03-capability-boundaries.md).
- **Dependency graph and naming**: remove `runtime` as a catch-all namespace and
  split entrypoints by audience. See
  [`04-dependency-graph-and-naming.md`](./04-dependency-graph-and-naming.md).
- **Migration and tests**: sequence the work so state-machine extraction happens
  before more service/layer extraction. See
  [`05-migration-and-tests.md`](./05-migration-and-tests.md).

## Non-Goals

- Do not expose internal services from the package public API.
- Do not introduce a second public engine service.
- Do not copy Restate's partition leadership implementation.
- Do not build lease/heartbeat/claim-sweep as part of this refactor.
- Do not move services onto object streams in the same change as file/module
  extraction.
- Do not replace the public free primitives.

## Acceptance Criteria

The rearchitecture is successful when:

- object durable protocol decisions live in a pure `object/machine/` boundary
  with no S2, Effect service, handler registry, fiber, or waiter dependency;
- `object/owner-driver.ts` is a driver around the object state machine:
  read/fold, decide, append, run emitted action;
- `engine/api.ts` is the public lifecycle/query API, handler-scoped operations
  flow through `invocation/scope.ts`, and engine assembly lives in
  `engine/live.ts` until the internal executor split is worth doing;
- object owner-log IO is a durable adapter over `effect-s2-stream-db`
  `EventStream<ActorEvent>` rather than bespoke S2 stream mechanics;
- object S2 fencing lives only in `object/drive-session.ts`;
- service legacy storage is isolated until deliberately unified;
- new implementation modules do not use `runtime` as a catch-all namespace;
- package tests and typecheck remain green after each step;
- the public API is intentionally small and organized by audience.
