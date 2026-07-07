# Fireline Client Model Profile

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

> Fireline-specific companion to the neutral Stream-First Agent Substrate RFC.

## Read Helper Contract

Fireline read helpers scope prompt requests, chunks, and permission rows by the
logical session/request identifiers exposed in materialized rows. Prompt request
helpers order by `startedAtMs`; chunk helpers order by `emittedAtMs`; permission
helpers order by `createdAtMs`. The helpers must not mix rows from other
sessions or prompt requests just because they share the same physical stream.
[executable contract: `packages/client/test/shape-e.test.ts`;
`packages/client/test/shape-e.test.ts`;
`packages/client/test/shape-e.test.ts`;
`packages/client/test/shape-e.test.ts`]

## Update Stream Contract

The stream-first Fireline client exposes session updates as replay-plus-live
session-scoped updates. Prompt updates are an ergonomic live-only filter for one
prompt request's chunks, not a replacement for session updates. `collectionRows`
uses collection `subscribeChanges` deltas rather than repeatedly replaying full
snapshots. [evidence: `packages/client/src/domain.ts:194-217`;
`packages/client/src/prompt.ts:56-74`;
`packages/client/src/projections.ts:44-61`]

## Known Projection Ordering Gap

Current TS chunk projection uses `emittedAtMs` as the best available stable sort
key, while the neutral RFC treats durable append order / projection cursor as
the ordering source of truth. This is a Fireline implementation gap to close in
the TS projection cleanup track, not a substrate-level RFC rule. [evidence:
`packages/client/src/materialize.ts`]
