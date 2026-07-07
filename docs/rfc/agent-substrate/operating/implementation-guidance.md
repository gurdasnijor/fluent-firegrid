# 32. Implementation Guidance

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

## 32.1 Keep Protocol Adapters Below Runtime Semantics

Do not expose transport addresses to application clients unless the application is explicitly a low-level protocol tool.

ACP, stdio, HTTP, gRPC, vendor APIs, MCP-capable agents, and in-process agents are peer adapter targets. A stream-first client API **MUST NOT** couple to any specific agent protocol's wire shape.

## 32.2 Keep Operators Domain-Specific

Do not build a god-worker.

Shared operator mechanics may own:

```txt
replay/live boundary
claim append
claim observation
owner evaluation
in-flight suppression
supervision
```

Domain operators own:

```txt
row schemas
eligibility
side effects
terminal records
dead-owner policy
error mapping
```

Implementation PRs that touch replay, claims, adapter boundaries, public client surfaces, or projection ownership should name the applicable architecture guards and the exact local/CI commands that exercised them. If no executable guard exists, the PR should either add one or record a follow-up with the boundary being left unguarded.

## 32.3 Treat Local State as Cache Unless Proven Durable

Process-local maps, queues, handles, fibers, and connections are not durable truth.

Local caches are acceptable when they accelerate live work and can be rebuilt, reattached, or safely discarded. If a local cache becomes required for correctness after restart, it is no longer merely a cache and needs a durable record or projection owner.

## 32.4 Prefer Canonical Protocol IDs

When adapting ACP or any other protocol, preserve canonical identifiers.

Do not mint unnecessary substitute IDs.

## 32.5 Make Projection Ownership Explicit

For each projection family, define who writes or derives it.

Examples:

```txt
session rows
prompt terminal rows
chunk/update rows
permission rows
runtime rows
resource rows
```

Avoid multiple components claiming sole ownership of the same durable row family.

## 32.6 Model Middleware as Data

Middleware should be specified as serializable data that lowers into topology/runtime components. Avoid modeling approval, policy, or tool interception as unrecorded runtime closures. A middleware spec can be stored, audited, replayed, lowered in another language, and tested independently of one process.

Authoring helpers should be pure data builders. They should not perform IO, service discovery, credential lookup, or host inspection. The host preserves declared chain order and resolves references later under runtime policy.

## 32.7 Durable Coordination Patterns

Mailbox-like durable coordination should be built from canonical state rows and projection waits:

```txt
state.insert(...)
wait_for(state.changes(query).onInsert)
```

Do not introduce a mailbox class, queue, or side-channel as a second coordination source unless the implementation documents why the state-pattern composition is insufficient and how the abstraction remains log-derived.

When an async mailbox-like delivery leads to session work, make the bridge explicit: validate origin/payload/policy/state, derive a distinct session idempotency key, append or submit the chosen durable side effect, and ack/fail the async item only after that side effect is durably accepted or intentionally complete.

Keep log durability separate from queryability. SQL/search/archive sinks are projection consumers, not log-server features and not alternate truth.

## 32.8 Claim Only Where Needed, But Always Where Required

Multi-worker externally visible side effects **MUST** use durable claim records. Single-owner idempotent completion flows **MAY** skip durable claims if duplicate execution cannot affect external state and the implementation documents that invariant.

---

# 33. Anti-Patterns

A conforming system SHOULD avoid:

```txt
application clients opening agent transports directly for normal prompts
durable session row treated as live session ownership
one-shot protocol connections for every prompt when session ordering matters
hidden in-memory permission state
timer state not reconstructable after restart
tool approvals resolved by callback only
side effects during replay
work execution without durable claim
projection rows that cannot be rebuilt
transport URLs as product-level identities
local timeouts as normal runtime-unavailable signaling
protocol-specific assumptions at the client API boundary
stream-first client APIs coupled to one agent protocol wire shape
middleware represented only as runtime closures
approval handled only by private callbacks
mailbox classes that bypass canonical state + projection waits
mailbox-to-session bridges that turn async payloads into hidden prompt input
acking async work before the chosen session/prompt side effect is durably accepted
provider execute paths used as generic prompt dispatch seams
substrate-level lease APIs that hide domain claim rows
tool descriptors added, removed, or renamed after session initialization
middleware helpers that perform IO, service discovery, or secret resolution during authoring
live adapter metadata treated as durable recovery or query state
SQL/search/archive sinks treated as alternate truth instead of projections
hanging waiters indefinitely after runtime restart because adapter reattach capability was not declared
treating in-memory continuation state as durable when the adapter declares no-reattach
shipping a workflow orchestration SDK as part of the substrate, such as createFunction, step.run, or YAML DAGs
treating the substrate as a workflow engine
hand-coded sequence, branching, or parallelism in client code where the agent could decide through durable tools
making observability optional in a choreography-first implementation
hiding choreography traces from the agent that produced them
```

---
