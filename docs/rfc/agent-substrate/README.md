# Stream-First Agent Substrate RFC

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

This folder contains the stream-first agent substrate RFC, split into a documentation structure modeled after TigerBeetle's docs layout: a short start page plus Concepts, Coding, Operating, Reference, and Internals sections.

```txt
Document: Stream-First Agent Substrate RFC
Version: 0.1-draft
State: Draft for review
Authors: Codex 1, with PO and cross-architect review input
Intended status: Architecture blueprint / reference model
Scope: Language-neutral, runtime-neutral, wire-protocol-neutral
Reference implementation family: Stream-first managed-agent systems
```

## Read First

- [Start](./start.md) gives the shortest path through the RFC.
- [Outline](./outline.md) gives a single-page map of the high-level concepts
  and links into the detailed sections.
- [Concepts](./concepts/) explains the background, primitive model, choreography posture, terminology, prior art, and alternatives.
- [Coding](./coding/) covers client behavior, middleware/conductor composition, providers/resources/sandboxes, and example flows.
- [Operating](./operating/) covers observability, restart/recovery, security, conformance, and implementation guidance.
- [Reference](./reference/) contains abstract interfaces, record and identity models, idempotency, errors, extension points, Fireline mapping, and references.
- [Internals](./internals/) contains the deeper architecture, durable log, projection/channel, runtime/operator, session/prompt/adapter, and durable wait details.

## Implementation Profiles

The RFC is neutral. Implementation-specific contracts live beside the neutral
page they qualify, using a sibling suffix pattern:

- `name.md` is the neutral RFC page.
- `name.fireline.md` is Fireline's implementation profile for that page.

Future implementations can use the same sibling pattern with their own suffix,
for example `name.example.md`. Every `*.fireline.md` file must have a sibling
neutral `*.md` file with the same base name.

The imported `*.fireline.md` files are retained as historical implementation
profiles from the source RFC. They do not describe the current
`fluent-firegrid` EffSharp/S2 implementation. New implementation profiles for
this repository should use a real implementation suffix, for example
`name.fluent.md`, and should cite the S2 substrate canon when they depend on S2
instead of the RFC's idealized substrate.

## Section Map

### Concepts

- [Background](./concepts/background.md)
- [Prior Art / Existing Systems](./concepts/prior-art.md)
- [Terminology](./concepts/terminology.md)
- [Core Principle](./concepts/core-principle.md)
- [Managed-Agent Primitives](./concepts/managed-agent-primitives.md)
- [Choreography and Combinators](./concepts/choreography-and-combinators.md)
- [Alternatives Considered](./concepts/alternatives.md)
- [Open Questions + Future Work](./concepts/future-work.md)

### Coding

- [Client Model](./coding/client-model.md)
- [Conductor / Middleware](./coding/conductor-middleware.md)
- [Providers, Resources, and Sandboxes](./coding/providers-resources-sandboxes.md)
- [Example Flows](./coding/example-flows.md)

### Operating

- [Observability](./operating/observability.md)
- [Restart Semantics](./operating/restart-semantics.md)
- [Security Considerations](./operating/security.md)
- [Conformance](./operating/conformance.md)
- [Implementation Guidance](./operating/implementation-guidance.md)

### Reference

- [Abstract Component Interfaces](./reference/abstract-interfaces.md)
- [Record Model](./reference/record-model.md)
- [Identity Model](./reference/identity-model.md)
- [Idempotency](./reference/idempotency.md)
- [Error Model](./reference/error-model.md)
- [Extension Points](./reference/extension-points.md)
- [Relationship to Fireline](./reference/fireline-mapping.md)
- [References](./reference/references.md)

### Internals

- [Architecture](./internals/architecture.md)
- [Durable Log Requirements](./internals/durable-log.md)
- [Projections and Durable Channels](./internals/projections-and-channels.md)
- [Runtime and Operators](./internals/runtime-and-operators.md)
- [Sessions, Prompts, and Adapters](./internals/session-prompt-adapters.md)
- [Durable State, Awaitables, Approvals, and Timers](./internals/durable-state-awaitables-approvals-timers.md)
- [Reference Architecture](./internals/reference-architecture.md)
