# L1 Observation Vocabulary — Decision Record (Gate G2)

Doc-Class: canon
Status: active
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: S2

This is the decision record for architect gate **G2** (L1 observation
vocabulary), the expanded companion to the summary in the managed-sessions SDD
[§MS-C6](../../../sdds/managed-sessions-agent-ui-sdd.md). The decision was made
2026-07-06; WP D1 turns it into the schema, this record, and the initial fixture
corpus. Deviating from what is recorded here re-opens gate G2.

The vocabulary is cross-lane interface **I2** in the
[execution ledger](../../../execution/managed-sessions-lanes.md); changes to it
require gate G1.

## Problem

A managed session drives an external harness (Claude Agent SDK, Claude/Codex
ACP, a native protocol, a cloud agent) whose model loop Firegrid does not own
(see [`execution-models.md`](./execution-models.md), Model B). The harness I/O
boundary turns protocol traffic into **Layer 1 observation facts** appended to
the session's turn stream (see [`harness-io.md`](./harness-io.md)). Two consumers
fold those facts: the session history fold (WP A4) and the agent-ui event loop
(milestone MS-M4). We need one fact vocabulary those consumers fold **regardless
of which harness produced it**, or every harness multiplies the fold surface and
every UI re-learns a new format.

## Decision (G2)

The L1 observation vocabulary is an **ACP `session/update` superset**.

- **Base vocabulary = ACP `session/update` semantics**, mirrored faithfully:
  `user_message_chunk`, `agent_message_chunk` (message chunks),
  `agent_thought_chunk` (thought chunks), `tool_call` and `tool_call_update`,
  and `plan`.
- **Firegrid extensions are namespaced under `firegrid/` and additive:**
  `firegrid/usage` (token/cost facts), `firegrid/subagent` (parent-scoped
  attribution), `firegrid/native` (harness-specific passthrough, tagged with a
  harness id).
- **Every extension is ignorable-by-default.** A consumer that does not
  understand an extension MUST skip it, and the base fold's correctness never
  depends on it. Extensions enrich; they are never load-bearing.
- **The schema is versioned** (`L1_SCHEMA_VERSION`). Bumped only when a *base*
  variant is added, removed, or given breaking semantics — never for a new
  additive `firegrid/` extension.
- **Unrecognized `sessionUpdate` values are preserved verbatim and ignored by
  the base fold.** A future ACP variant, an unknown `firegrid/` extension, or a
  foreign namespace is a *foreign* record: kept as evidence, never fatal, never
  folded. This is what makes the schema forward-compatible.

**Rationale.** Future ACP harnesses lower near-trivially (the base variants are
already their native `session/update` shape), and every UI folds one format
regardless of harness. The compatibility gate is the D2 fixture corpus: an
adapter conforms iff it reconstructs the recorded L1 facts and the same folded
state.

### Alternatives considered

- **Per-harness vocabularies, folded by per-harness UI code.** Rejected: N
  harnesses × M consumers of fold logic; the current agent-ui `switch (m.type)`
  lowering that MS-M4 deletes is exactly this failure.
- **A bespoke neutral vocabulary invented here.** Rejected: it would re-derive
  ACP `session/update` with less review and no ecosystem; ACP already solved the
  message/thought/tool/plan streaming shape.
- **ACP `session/update` superset (chosen).** Faithful base + a small, namespaced,
  ignorable extension set carries the Firegrid-specific facts (usage, subagent
  scoping, native passthrough) that ACP does not standardize for our needs.

## Base Vocabulary

Each base variant is a faithful ACP `session/update` payload, discriminated on
`sessionUpdate`.

| `sessionUpdate` | Payload (ACP-aligned) | Base fold effect |
| --- | --- | --- |
| `user_message_chunk` | `content: ContentBlock`, `messageId?` | Appends to the current user message |
| `agent_message_chunk` | `content: ContentBlock`, `messageId?` | Appends to the current assistant message |
| `agent_thought_chunk` | `content: ContentBlock`, `messageId?` | Appends to the current thought block (never message text) |
| `tool_call` | `toolCallId`, `title`, `kind?`, `status?`, `content?`, … | Opens a tool-call entry keyed by `toolCallId` |
| `tool_call_update` | `toolCallId`, `status?`, `title?`, `content?`, … | Merges into the entry (status/title/kind overwrite, content appends) |
| `plan` | `entries: PlanEntry[]` | Replaces the current plan (last-write-wins) |

Chunks fold by identity: consecutive chunks of the same role concatenate into one
message; a role change, or a change of `messageId` when both are present, starts a
new message. The turn-stream envelope (record address, sequence, terminal marker)
is owned by interface **I1** (WP B1's `DurableLog`/Turn binding); this vocabulary
is the *payload* inside each turn-stream record.

## Firegrid Extensions

Each extension declares its fold behavior for a consumer that does not understand
it. In every case that behavior is **skip** — the base fold ignores it.

### `firegrid/usage`

Token and cost accounting for a turn (`inputTokens`, `outputTokens`,
`cacheCreationInputTokens`, `cacheReadInputTokens`, `totalTokens`, `costUsd`,
`model`) — all optional, because harnesses report different subsets. ACP recently
added a native `usage_update` variant, but the G2 decision **deliberately keeps
usage as a Firegrid extension**: token/cost accounting is a Firegrid concern that
must be ignorable-by-default for the base fold, not a base variant every consumer
must handle. A usage-aware UI (MS-M4) reads it to surface per-turn cost — a
current agent-ui defect this closes.

### `firegrid/subagent`

Parent-scoped attribution for subagent activity: `parentToolCallId` plus optional
`subagentId`, `label`, `model`. This record is **enrichment only**. Subagent
output is carried as `tool_call_update` content on its `parentToolCallId`, so the
base fold already attributes subagent work to the parent tool call and never
interleaves it into top-level turn text — *without reading this record*. A
subagent-aware UI reads it for richer rendering. This is the design that satisfies
the `harness.subagent-scoping` obligation while keeping the extension ignorable:
if it were load-bearing (subagent text emitted top-level, re-homed only by this
record), dropping it would corrupt the base fold.

### `firegrid/native`

Harness-specific passthrough tagged with the emitting `harness` id, an optional
`nativeType`, and an opaque `payload`. Carries data with no ACP equivalent (e.g.
a harness `system_init`); always ignorable-by-default.

## The Base Fold

`foldTurn(records)` is the canonical projection every UI shares and the reference
D2's fixture-replay harness compares adapter output against. It consumes only base
records; every extension and foreign record is skipped. Its output is **invariant
to the presence or absence of any non-base record** — the executable statement of
ignorable-by-default. Folding is deterministic (no clock, no entropy): messages
and thoughts fold by identity, tool calls merge by `toolCallId`, `plan` is
last-write-wins.

## Scope Boundary

This record (WP D1) fixes the **vocabulary**: the schema, the extension
namespaces, the ignorability and versioning rules, and the base fold. It does not
fix the **adapter contract** — `drive`, native-resume-artifact production, and the
declared interception capability (gateable vs. observe-only). Those, and the RFC
invariants *interception-capability declaration* and *resume-suppression
contract*, belong to the adapter contract (WP D2) and the first concrete adapter
(WP D3). They are noted here only to mark the boundary.

## Consequences

- **`@firegrid/l1-vocabulary`** (`packages/l1-vocabulary`) is the schema: the
  Effect-free data types, the decoder, and `foldTurn`. Public data types carry no
  Effect dependency (two-zone rule: the harness-adapter edge is TypeScript).
- **Fixtures** (`packages/l1-vocabulary/fixtures/*.json`) seed D2's fixture-replay
  harness with named turns as canonical L1 record sequences. D2 pairs them with
  recorded harness transcripts.
- **Proof** `l1-vocabulary.schema-conformance` exercises the surface in CI: every
  fixture decodes, is JSON round-trip stable, declares the current schema version,
  folds invariantly to stripping its extensions, and keeps subagent output under
  its parent tool call.
- **Consumers of I2**: A4 (session history fold) and MS-M4 (agent-ui UI fold) fold
  this one vocabulary. D3's Claude Agent SDK adapter emits it.

The RFC invariant *L1 vocabulary schema* (SDD §MS-C6) is proved by
`l1-vocabulary.schema-conformance`; mapping it into
[`conformance.md`](../../../rfc/agent-substrate/operating/conformance.md) is a
Lane F task (WP F2).

## Read Next

- [`harness-io.md`](./harness-io.md): where L1 facts are produced.
- [`execution-models.md`](./execution-models.md): why managed sessions record
  observations rather than replay a body.
- SDD [§MS-C6](../../../sdds/managed-sessions-agent-ui-sdd.md): the capability
  this vocabulary unblocks (MS-M4).
