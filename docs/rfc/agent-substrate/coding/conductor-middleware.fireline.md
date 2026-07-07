# Fireline Conductor / Middleware Profile

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

> Fireline-specific companion to the neutral Stream-First Agent Substrate RFC.
>
> **Frozen / historical (Rust Fireline).** This profile describes the legacy
> Rust `crates/fireline-*` implementation, not the current `fluent-firegrid`
> system; it is retained as reference archaeology. Current implementation
> profiles use the `.fluent.md` suffix — see
> [Implementation Profiles](../README.md#implementation-profiles).

## Tool Descriptor Publication

Fireline-owned tools are declared as topology/conductor tool components before
ACP session initialization. The shared Fireline publication/materialization path
validates and freezes the descriptor set, checks the live handler or transport
binding, optionally emits `tool_descriptor` projection rows, and attaches the
live MCP/tool surface. Helper APIs such as `publish_tool_component` are current
implementation details of that lowering path, not the canonical authoring model.

`tool_descriptor` envelope values contain only the agent-visible Anthropic
triple: `name`, `description`, and `inputSchema`. Transport references,
credential references, and provider execution details stay conductor-side and
must not leak into the descriptor value. If multiple attach sources publish the
same tool name, the first attach wins. [evidence:
`vault/canon/concepts/fireline-tool-publication-path.md:150-170`;
`vault/canon/concepts/fireline-tool-publication-path.md:199-230`;
`crates/fireline-substrate/src/tools/descriptors.rs:1-12`;
`crates/fireline-substrate/src/tools/descriptors.rs:36-53`;
`crates/fireline-substrate/src/tools/descriptors.rs:61-68`;
`crates/fireline-substrate/src/tools/surface.rs:1-31`]

## Durable Middleware Decisions

Fireline middleware specs are serializable. App-visible lowered decisions and
effects cross the stream/conductor boundary and are not retained as JavaScript
callback state. Context injection is ACP prompt middleware: it gathers fast-path
context sources, tolerates a missing workspace file as empty context, and keeps
the resulting decision at the stream/conductor boundary rather than in local
callback state. [evidence:
`vault/canon/concepts/middleware-composition.md:193-205`;
`crates/fireline-substrate/src/middleware/context.rs:3-10`;
`crates/fireline-substrate/src/middleware/context.rs:47-50`;
`crates/fireline-substrate/src/middleware/context.rs:209-215`]

## Mailbox Pattern Boundary

Fireline does not define mailbox as a separate persistent substrate primitive or
parallel wire protocol. Mailbox-like coordination targets state inserts plus
projection waits unless a future design separately justifies another primitive.
[evidence: `vault/canon/concepts/mailbox-plane.md:48-67`;
`vault/canon/concepts/mailbox-plane.md:90-117`;
`vault/canon/concepts/mailbox-plane.md:175-186`]
