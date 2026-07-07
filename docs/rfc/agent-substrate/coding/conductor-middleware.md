# 17. Conductor / Proxy Chain Model

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

Some protocols support proxy or middleware chains between client and agent.

A conductor is a live runtime component that routes messages through such a chain.

A conductor MAY provide:

```txt
ordered proxy chain
lazy initialization
proxy-vs-agent initialization
bidirectional routing
trace hooks
tool bridges
capability mutation
policy enforcement
```

A conductor can model a chain of components between client/application and agent, lazy initialization on first initialize, proxy-vs-agent initialization, and successor/predecessor routing.

This RFC does not require a conductor.

If a conductor exists:

```txt
conductor configuration SHOULD be durable or derivable from durable launch/session specs
conductor live connections are process-local resources
conductor-observed application events SHOULD be written to the durable log
```

## 17.1 Chain Composition

A conductor chain is an ordered list of components. Each component has a predecessor toward the client/application side and a successor toward the agent/provider side, unless it is at an end of the chain.

Composition rules:

```txt
chain order MUST be deterministic from a serializable chain spec
component identity MUST be deterministic from the topology spec
declared chain order MUST be preserved by the host
each component MUST receive initialization before normal routing, if it requires initialization
messages MUST route to exactly one successor or terminate with a durable/error outcome
components MUST NOT skip predecessors/successors except through an explicit routing rule
```

The chain spec **SHOULD** be durable or derivable from durable launch/session configuration. Runtime closures are not a portable chain spec.

Component identity derivation **MUST** distinguish multiple instances of the same component kind. For example, two approval gates in one topology need stable distinct component ids derived from their position, explicit name, scope, or declared id. Replaying the same topology spec **MUST** produce the same component ids, ordering, and capability mutations.

## 17.2 Lazy Initialization

Conductor components MAY initialize lazily on first use. If they do:

```txt
lazy init MUST be idempotent
concurrent messages MUST observe one initialized component instance or a deterministic failure
capability changes discovered during init MUST be recorded or projected if application-visible
init failure MUST fail the affected message/session with a typed error
```

Lazy initialization **MUST NOT** hide durable required actions, capability mutations, or policy decisions solely in process memory.

## 17.3 Capability Mutation Rules

Middleware and conductor components MAY add, remove, or transform capabilities. Capability mutation **MUST** be deterministic for the same chain spec and input capability set, except for explicitly documented live/provider checks.

If a component removes a capability for policy reasons, the system **SHOULD** expose that fact in a projection or audit record when it affects client behavior. If a component adds a tool, approval gate, or adapter-facing capability, its durable identity and scope **SHOULD** be visible in the launch/session spec or derived topology.

The visible tool descriptor set **MUST** be frozen before session initialization completes. Runtime context may bind handler state after session creation, but it **MUST NOT** add, remove, or rename tools in the live session. A capability change that requires a different tool set requires a new session or an adapter-defined mechanism that preserves cache/session invariants and makes the change explicit in durable records.

Tool attachment is a topology/conductor capability mutation. A component that
attaches a tool **MUST** declare that capability as part of the topology before
the visible catalog is materialized. The declaration owns component identity,
descriptor set, handler or transport binding, credential references, ordering,
and policy. Descriptor publication into durable rows is a materialization step
from that declaration, not the primary authoring API.

A component that attaches a tool **MUST** publish a descriptor into the visible
tool catalog without making the provider/sandbox execution path the descriptor
itself. Provider execution, transport binding, and credential resolution remain
handler-side concerns after descriptor publication. Direct transport or MCP
attachment APIs are compatibility/bridge internals unless wrapped by a declared
topology component, and owned tool components **SHOULD** pass through the shared
topology publication/materialization path.

## 17.4 Middleware as Serializable Specs

Middleware is specified as data, not runtime closures. A middleware spec is a serializable declaration such as:

```txt
approve({ scope: "tool_calls" })
  -> { kind: "approval_gate", scope: "tool_calls", policyRef: "...", options: [...] }
```

The lowering boundary is:

```txt
middleware spec
  -> topology/runtime component
  -> operators/adapters over the durable log
```

A conforming implementation **MUST NOT** make an in-memory approval proxy, callback, or closure the primary durable model. Approval middleware lowers to a named component that appends required-action records, observes resolution records, and resumes/denies/fails the waiting tool or prompt according to §21.

Middleware authoring **MUST** be pure. Authoring helpers may validate local input and construct serializable specs, but they **MUST NOT** start runtimes, open sockets, discover service endpoints, read local daemon state, perform approval decisions, or resolve credentials. Host materialization performs IO later, under explicit runtime configuration.

Middleware specs **SHOULD** include:

```txt
kind
scope
policy reference or inline policy
component identity
ordering constraints
capability mutations
audit labels
```

Middleware lowering **MUST** be deterministic. If the same launch/session spec is replayed, the same topology component identities and ordering **MUST** be produced.

Credential-bearing middleware **MUST** carry references, not inline secret values. Hosts resolve those references at materialization or call time and should redact resolved values from durable records and traces.

---
