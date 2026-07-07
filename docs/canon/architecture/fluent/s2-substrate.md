# Fluent S2 Substrate

Doc-Class: canon
Status: active
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: S2

This page is the current substrate canon for `fluent-firegrid` after the
EffSharp cutover. It replaces the idealized Durable Streams protocol sketch in
[`substrate-protocol.md`](substrate-protocol.md) for implementation decisions.

## Current Package Boundary

- `@firegrid/log` / `Firegrid.Log` wraps the S2 client/substrate through Fable.
- `@firegrid/store` / `Firegrid.Store` owns S2-backed workflow/object storage
  primitives and host recovery helpers.
- `@firegrid/runtime` keeps the vendored workflow runtime shape used by the
  current TypeScript surface.
- `@firegrid/fluent` owns the authoring surface, descriptors, clients, and HTTP
  binding. It should not grow a second S2 substrate implementation.

## Proven S2 Primitives

The current implementation relies on these S2 facts:

| Primitive | S2 mechanism | Proof coverage |
| --- | --- | --- |
| Append visibility | acknowledged append advances tail; later reads see the batch in order | `effect-s2.capability-a.read-after-append` |
| Restart cursor fold | replay from a persisted sequence number reads the suffix in order | `effect-s2.capability-a.cursor-fold` |
| Optimistic contention | two appends at one `matchSeqNum` cannot both commit | `effect-s2.capability-b.match-seq-num-contention` |
| Cooperative fencing | owners fence by writing and checking durable ownership facts, not by substrate-enforced exclusive leases | `effect-s2.capability-b.fence-semantics`, `store.object-live-fencing` |
| Event-log CAS | workflow events append with expected next index; stale writers surface a conflict | `store.event-log-cas` |
| Host recovery | a restarted host can recover/sweep due work from durable facts | `store.host-crash-restart`, `store.host-tick`, `store.runtime-timer-sweep`, `store.runtime-schedule-sweep` |

## S2 Deltas From The Idealized RFC

S2 is the durable log substrate, not a full coordination service. Current canon
must not assume these idealized Durable Streams features exist as substrate
primitives:

- named consumers with substrate-managed offsets;
- claim/ack/release worker leases;
- webhook wake delivery with substrate-owned retry;
- substrate-side wait matching or CEL predicate execution;
- producer epochs that automatically depose stale owners across all write paths.

The Firegrid implementation compensates by writing explicit durable facts,
performing cooperative ownership checks, and proving the resulting behavior with
the proof registry. A design that needs stronger substrate behavior must either
add it above S2 in `@firegrid/store` or move the requirement back into the RFC
as an aspirational invariant.

## Current Lowering

The current host loop uses S2 streams as append-only facts:

```text
read durable facts
  -> decide due work in the host
  -> append intent/result/ownership fact with expected sequence where needed
  -> replay or reconstruct from the durable suffix
  -> sweep overdue timers/schedules by appending one winning fact
```

Object state and workflow event streams are separate durable fact streams. They
share the same rules: append facts before side effects that must survive restart,
derive read models from facts, and treat stale expected-index writes as
conflicts rather than idempotent success.

## Conformance

The RFC conformance bridge lives at
[`../../../rfc/agent-substrate/operating/conformance.md`](../../../rfc/agent-substrate/operating/conformance.md).
Every S2-specific proof should map to at least one numbered invariant there. If
a canon page cites an invariant without a passing proof, mark it aspirational.
