# @firegrid/l1-vocabulary

The **L1 observation vocabulary** — the harness-agnostic fact vocabulary a
managed session appends to its turn stream, folded identically by every UI
regardless of which harness produced it. This is cross-lane interface **I2** in
the managed-sessions execution ledger; changes to it require architect gate G1.

## The decision (G2)

Recorded in
[`docs/canon/architecture/fluent/l1-observation-vocabulary.md`](../../docs/canon/architecture/fluent/l1-observation-vocabulary.md)
and the managed-sessions SDD §MS-C6: the vocabulary is an **ACP `session/update`
superset**.

- **Base vocabulary** mirrors ACP `session/update` exactly: `user_message_chunk`,
  `agent_message_chunk` (message chunks), `agent_thought_chunk` (thought chunks),
  `tool_call` + `tool_call_update`, and `plan`.
- **Firegrid extensions** are namespaced under `firegrid/` and additive:
  `firegrid/usage` (token/cost facts), `firegrid/subagent` (parent-scoped
  attribution), `firegrid/native` (harness-specific passthrough).
- Every extension is **ignorable-by-default**: a consumer that does not
  understand it skips it, and the base fold's correctness never depends on it.
- The schema is **versioned** (`L1_SCHEMA_VERSION`). Unrecognized `sessionUpdate`
  values are preserved verbatim and ignored by the base fold, so new variants
  never break an old fold.

## Surface

Effect-free, adapter-facing data types (two-zone rule: the harness-adapter edge
is TypeScript and its public data types carry no Effect dependency).

- `vocabulary.ts` — types, constants, and classifiers (`classifyUpdate`,
  `isBaseUpdate`, `isFiregridExtension`, `isForeignUpdate`, `isIgnorableByBaseFold`).
- `decode.ts` — `decodeStreamRecord` / `decodeStream`: structural validation that
  preserves records verbatim (validation, not transformation). Known variants are
  validated strictly; anything else is preserved as an ignorable foreign record.
- `fold.ts` — `foldTurn`: the canonical base fold into folded message state. It
  consumes only base records; every extension and foreign record is skipped.
- `fixtures.ts` — `l1Fixtures`: the initial fixture corpus (see below).

The turn-stream envelope (record address, sequence, terminal marker) is owned by
interface **I1** (WP B1's `DurableLog`/Turn binding), not by this package; this
vocabulary is the *payload* carried inside each turn-stream record.

## Fixtures

`fixtures/*.json` is the seed corpus for D2's fixture-replay harness — named turns
expressed as ordered L1 record sequences. D2 pairs these with recorded harness
transcripts and asserts an adapter reconstructs the same L1 facts and the same
`foldTurn` output. Add a scenario by dropping a JSON file and appending it to
`l1Fixtures` in `src/fixtures.ts`.

## Proof

`apps/proofs/proofs/l1-vocabulary-conformance.ts` (registry id
`l1-vocabulary.schema-conformance`) exercises this surface in CI: every fixture
decodes, is JSON round-trip stable, declares the current schema version, folds
invariantly to stripping its extensions (ignorable-by-default), and the subagent
fixture keeps subagent output under its parent tool call rather than in top-level
turn text.
