# Fireline Durable Log Profile

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

## StreamSubstrate Ledger Adapter

Fireline embeds durable-streams server routes for local/runtime operation and
keeps `StreamSubstrate` as a thin ledger adapter. The adapter exists to preserve
upstream durable-streams wire and storage semantics rather than replacing them
with Fireline-specific log behavior. [evidence:
`crates/fireline-substrate/src/session/stream_host.rs:1-17`;
`crates/fireline-substrate/src/session/stream_substrate.rs:1-7`]

## SQL Persistence Boundary

Fireline treats log durability as upstream durable-streams storage configuration.
SQL/queryability is a Fireline-built projection consumer, not a durable-streams
server feature and not an alternate source of write truth. [evidence:
`vault/canon/concepts/sql-persistence.md:36-67`;
`vault/canon/concepts/sql-persistence.md:97-111`;
`vault/canon/concepts/sql-persistence.md:154-162`]

## Bounded Replay Completion Indexes

Fireline durable subscribers keep completion indexes keyed by semantic operation
identity so replay can remain bounded even when the retained replay buffer has
trimmed older records. Reader offsets remain stream coordinates and are tracked
out of band from timer, delivery, and completion ids. [executable contract:
`crates/fireline-substrate/src/promises/durable_subscriber.rs:1215`;
`crates/fireline-substrate/src/promises/durable_subscriber.rs:1252`;
`crates/fireline-substrate/src/promises/durable_subscriber.rs:1402`]
