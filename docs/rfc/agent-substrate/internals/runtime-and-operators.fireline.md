# Fireline Runtime And Operators Profile

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

## Compile-Time Boot Order

Fireline runtime construction is compile-time staged. The stage order is:
`with_identity` -> `with_streams` -> `with_state` -> `with_topology` ->
`with_conductor` -> `with_subscribers` -> `with_tools` ->
`with_launch_dispatcher`. Reordering these phases is intended to fail at compile
time rather than at runtime. [evidence: `src/context.rs:1-6`;
`src/context.rs:14-68`]

## Prompt Dispatcher Boundary

The Fireline prompt dispatcher owns replay, claim observation, eligibility,
liveness checks, and prompt terminal-row writes. Session/conductor code owns ACP
transport, framing, update parsing, and chunk projection. Raw ACP URLs must not
enter the dispatcher; dispatcher inputs are durable rows plus session
promptability/ownership checks. [evidence:
`crates/fireline-runtime/src/launch/prompt_dispatcher.rs:1-8`;
`crates/fireline-runtime/src/launch/execution.rs:57-63`]

## Sandbox Readiness Publication

Sandbox readiness publication is runtime orchestration. Tower/provider dispatch
is only the provider call path; readiness evidence includes listener/handshake
facts, persistent facts, projection observability, and provider descriptor
checkpoints. [evidence:
`crates/fireline-runtime/src/sandbox/readiness.rs:1-5`;
`crates/fireline-runtime/src/sandbox/readiness.rs:35-37`;
`crates/fireline-runtime/src/sandbox/readiness.rs:62-63`;
`crates/fireline-runtime/src/sandbox/readiness.rs:109-110`]
