# Start

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

Use the path that matches your job.

If you want a single-page map of the RFC before choosing a path, start with the
[Outline](./outline.md).

## Reader / Reviewer Path

Read this path first if you are evaluating the architecture, reviewing a design,
or deciding whether a system is directionally conforming:

1. [Background](./concepts/background.md)
2. [Core Principle](./concepts/core-principle.md)
3. [Managed-Agent Primitives](./concepts/managed-agent-primitives.md)
4. [Choreography and Combinators](./concepts/choreography-and-combinators.md)
5. [Architecture](./internals/architecture.md)
6. [Durable Log Requirements](./internals/durable-log.md)
7. [Projections and Durable Channels](./internals/projections-and-channels.md)
8. [Runtime and Operators](./internals/runtime-and-operators.md)
9. [Sessions, Prompts, and Adapters](./internals/session-prompt-adapters.md)
10. [Restart Semantics](./operating/restart-semantics.md)
11. [Conformance](./operating/conformance.md)

## Builder / Implementer Path

Read this path first if you are building a conforming implementation. It
front-loads the durable substrate mechanics because higher-level sessions,
adapters, middleware, and choreography tools are correct only when records,
cursors, projections, waits, and terminal folds are already correct:

1. [Terminology](./concepts/terminology.md)
2. [Record Model](./reference/record-model.md)
3. [Identity Model](./reference/identity-model.md)
4. [Idempotency](./reference/idempotency.md)
5. [Durable Log Requirements](./internals/durable-log.md)
6. [Projections and Durable Channels](./internals/projections-and-channels.md)
7. [Durable State, Awaitables, Approvals, and Timers](./internals/durable-state-awaitables-approvals-timers.md)
8. [Runtime and Operators](./internals/runtime-and-operators.md)
9. [Managed-Agent Primitives](./concepts/managed-agent-primitives.md)
10. [Sessions, Prompts, and Adapters](./internals/session-prompt-adapters.md)
11. [Conductor and Middleware](./coding/conductor-middleware.md)
12. [Provider, Resources, and Sandboxes](./coding/providers-resources-sandboxes.md)
13. [Restart Semantics](./operating/restart-semantics.md)
14. [Conformance](./operating/conformance.md)

The shortest implementation sequence is:

```txt
define canonical record envelopes, keys, and idempotency scopes
implement durable append/read/cursor/EOF/live-tail
build replayable projections over the log
implement snapshot-at-cursor then subscribe-after-cursor waits
implement first-valid-terminal-wins folds
append durable intents
claim side-effecting work
prove live promptability before prompt dispatch
append chunks and terminal rows
recover after restart using declared adapter reattach profiles
```

For Fireline-specific names, read only [Relationship to Fireline](./reference/fireline-mapping.md). Normative substrate sections remain implementation-neutral.
