# Outline

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

This page is a high-level map of the Stream-First Agent Substrate RFC. It is
not a separate proposal and does not replace the linked normative sections. Use
it when you want the whole shape of the RFC before reading the deeper pages.

## 1. Abstract

The substrate is built around one invariant: durable ordered facts are the
source of truth; everything else is a projection, live resource, adapter, or
operator over that log.

Read:

- [Core Principle](./concepts/core-principle.md)
- [Durable Log Requirements](./internals/durable-log.md)
- [Architecture](./internals/architecture.md)

## 2. Introduction

Agent systems need a shared durable account of launches, sessions, prompts,
chunks, approvals, timers, provider lifecycle, restart recovery, and audit. The
RFC defines a language-neutral and protocol-neutral substrate for that account.

Read:

- [Background](./concepts/background.md)
- [Managed-Agent Primitives](./concepts/managed-agent-primitives.md)
- [Prior Art / Existing Systems](./concepts/prior-art.md)

## 3. Goals And Non-Goals

The RFC aims to support stream-first clients, replayable agent activity,
materialized read models, restart-safe waits, claim-first side effects, protocol
adapters, and a strict split between durable identity and live ownership. It does
not require a particular database, transport, language, agent protocol, UI, or
sandbox technology.

Read:

- [Core Principle](./concepts/core-principle.md)
- [Alternatives Considered](./concepts/alternatives.md)
- [Extension Points](./reference/extension-points.md)

## 4. Terminology

The core vocabulary is:

```txt
Durable Log
Record / Envelope
Projection
Operator / Claimed Work Operator
Durable Claim
Live Resource
Session
Prompt / Turn
Agent Adapter
Host / Runtime
Client
Provider
Sandbox
Durable Promise / Awaitable
Orchestration
Harness
Tool
Resource
```

Read:

- [Terminology](./concepts/terminology.md)
- [Record Model](./reference/record-model.md)
- [Identity Model](./reference/identity-model.md)

## 5. System Planes

The architecture separates durable coordination from live process mechanics:

```txt
Application client plane
Durable log plane
Projection and query plane
Operator plane
Live runtime plane
Adapter / protocol plane
Provider / resource / sandbox plane
External integration and audit plane
```

Read:

- [Architecture](./internals/architecture.md)
- [Reference Architecture](./internals/reference-architecture.md)
- [Abstract Component Interfaces](./reference/abstract-interfaces.md)

## 6. Durable Log And Records

The durable log provides append, read, replay, ordering, cursors, durable
acknowledgement, EOF/live-tail behavior, and optional producer idempotency.
Records preserve logical fields for type, key or subject, value, and headers.
Headers carry schema, producer, correlation, causation, and related metadata.

Read:

- [Durable Log Requirements](./internals/durable-log.md)
- [Record Model](./reference/record-model.md)
- [Idempotency](./reference/idempotency.md)

## 7. Projections And Observation

Projections are rebuildable read models over the log, not alternate truth.
Projection-backed waits use snapshot-at-cursor followed by subscribe-after-cursor
so clients and operators do not miss terminal rows or scoped updates.

Read:

- [Projections and Durable Channels](./internals/projections-and-channels.md)
- [Client Model](./coding/client-model.md)
- [Observability](./operating/observability.md)

## 8. Operators And Claims

Operators consume records. Projection operators derive read models. Claimed-work
operators perform side effects only after replay, live-boundary detection,
durable claim append, and claim ownership observation. Domain operators own
eligibility, terminal records, retry behavior, and dead-owner policy.

Read:

- [Runtime and Operators](./internals/runtime-and-operators.md)
- [Conformance](./operating/conformance.md)
- [Implementation Guidance](./operating/implementation-guidance.md)

## 9. Clients

A stream-first client appends intents and observes projections. Normal
application clients do not open agent protocol transports directly for launch,
prompt, approval, or stop flows.

Read:

- [Client Model](./coding/client-model.md)
- [Relationship to Fireline](./reference/fireline-mapping.md)

## 10. Runtime And Live Ownership

The runtime owns live resources and side effects. Durable rows may record that
ownership was established at a point in time, but they do not prove that the
current process still owns a live handle after restart, failover, or lease
expiry.

Read:

- [Runtime and Operators](./internals/runtime-and-operators.md)
- [Restart Semantics](./operating/restart-semantics.md)

## 11. Sessions, Prompts, And Adapters

A session is durable conversation identity. A prompt is a request inside a
session. Adapter protocols such as ACP, stdio, HTTP, gRPC, vendor APIs, and
in-process agents translate between protocol wire semantics and durable
substrate semantics.

Read:

- [Sessions, Prompts, and Adapters](./internals/session-prompt-adapters.md)
- [Identity Model](./reference/identity-model.md)
- [Error Model](./reference/error-model.md)

## 12. Conductor And Middleware

Conductor and middleware layers are optional protocol-aware composition points.
Serializable middleware specs lower into runtime/topology components; they are
not hidden callback paths or alternate coordination systems.

Read:

- [Conductor and Middleware](./coding/conductor-middleware.md)
- [Choreography and Combinators](./concepts/choreography-and-combinators.md)

## 13. Providers, Resources, Sandboxes, And Tools

Providers provision resources needed to run agents. Sandboxes are scoped
execution environments. Resources are durable references to content or
capabilities. Tools are named capabilities invoked through the harness and
bounded by durable session, approval, topology, and adapter rules.

Read:

- [Providers, Resources, and Sandboxes](./coding/providers-resources-sandboxes.md)
- [Managed-Agent Primitives](./concepts/managed-agent-primitives.md)
- [Security Considerations](./operating/security.md)

## 14. Durable State, Channels, Awaitables, Approvals, And Timers

Durable state is represented by log records and projections. Durable channels
are typed record/projection/wait patterns. Awaitables reconstruct waits from the
log. Required actions and approvals are durable waits, not hidden callbacks.
Timers are restart-safe scheduled waits.

Read:

- [Durable State, Awaitables, Approvals, and Timers](./internals/durable-state-awaitables-approvals-timers.md)
- [Projections and Durable Channels](./internals/projections-and-channels.md)
- [Example Flows](./coding/example-flows.md)

## 15. Restart And Recovery

After restart, durable records, projection-rebuildable state, idempotency facts,
claims, waits, completions, and audit records survive. Live sessions, child
processes, sockets, provider handles, fibers, queues, and conductor connections
do not survive unless they are explicitly reattached or reacquired. Pending
suspensions require durable recovery decisions.

Read:

- [Restart Semantics](./operating/restart-semantics.md)
- [Runtime and Operators](./internals/runtime-and-operators.md)
- [Conformance](./operating/conformance.md)

## 16. Ordering, Terminals, And Idempotency

Append order or documented projection cursor order is authoritative. Operations
that can end once use first-valid-terminal-wins. Duplicate retries use
domain-level idempotency keys and conflict rules; local timeouts are not the
normal substitute for durable not-live, failed, denied, cancelled, or timed-out
facts.

Read:

- [Projections and Durable Channels](./internals/projections-and-channels.md)
- [Idempotency](./reference/idempotency.md)
- [Error Model](./reference/error-model.md)

## 17. Operating Concerns

The operating model covers observability, security, conformance, implementation
guidance, architecture drift, replay safety, and negative tests for common
anti-patterns.

Read:

- [Observability](./operating/observability.md)
- [Security Considerations](./operating/security.md)
- [Conformance](./operating/conformance.md)
- [Implementation Guidance](./operating/implementation-guidance.md)

## 18. Example Flows

The RFC includes short flows for local stdio launch, ACP prompt dispatch, human
approval, timers, restart with a stale session, and a minimal conforming example.

Read:

- [Example Flows](./coding/example-flows.md)

## 19. Fireline Mapping

The RFC is neutral. Fireline-specific names and compatibility contracts live in
Fireline profile pages or the Fireline mapping page, not in the neutral
substrate vocabulary.

Read:

- [Relationship to Fireline](./reference/fireline-mapping.md)
- [Fireline Record Model Profile](./reference/record-model.fireline.md)
- [Fireline Conformance Profile](./operating/conformance.fireline.md)

---

For the shortest implementation sequence, continue to [Start](./start.md).
