# Dispatch Brief: Phase A / G3 — land the streams packet (3 laws green)

Doc-Class: dispatch-brief
Date: 2026-07-09
Packet: PHASE A head (Phase 0 packets 0.4/0.5 DEFERRED post-Phase-A by
human directive 2026-07-09 — product momentum first)
Branch: EXISTING `g3/streams` (PR #121, draft — take it over; the old
session's agent is dead, the branch is intact)
Architect: this session — escalate on anything marked GATE
Merge gate: architect review

## Mission

Finish and land the halted G3 streams implementation: green
`t1.log-attach-byte-faithful`, `t1.three-read-grades`, `t1.cel-wait`.
This is PRODUCT code — the implementation lives in
`src/Firegrid.Durable/` (`InternalStreams.fs` per the packet file
discipline). Target scoreboard after promotion: **39 registered —
33 green, 6 expected-red, 0 errors**.

## Context you must absorb first

1. [`docs/handoffs/platform-greenmaking-handoff.md`](platform-greenmaking-handoff.md)
   Phase A item 1 — the packet's history and the delivered architect
   ruling that was MID-IMPLEMENTATION at the halt.
2. PR #121's body and the `g3/streams` branch state (3 laws were green
   locally pre-halt, against the OLD corpus runner).
3. The design note is already architect-approved: header-tagged L2 log
   justified by byte-faithfulness — do not re-litigate.
4. Since that branch was cut, Phase 0 replaced the verification estate:
   the three stream laws now live in `apps/proofs/StreamLawProofs.fs`
   (black-box, trace-evidenced, FROZEN bodies) and the harness runs
   proofs CONCURRENTLY against one shared s2 (PR #127).

## The work

1. **Rebase `g3/streams` onto current main.** The branch's edits to the
   old corpus tree (`src/Firegrid.Durable.Corpus/...`) drop away —
   resolve those conflicts by taking main's side wholesale (the corpus
   is gone; `apps/proofs` is the verifier now). The branch's PRODUCT
   code (`InternalStreams.fs` + contract-section bodies) survives and is
   the point.
2. **Complete the delivered ruling**: add the one-line
   `CelWatch.noteBasin` hook in `Wiring.runWorker` (in main's current
   `Internal.fs`) so worker-only processes evaluate CEL waits. REMOVE
   the interim client-surface seeding (ruled NOT mergeable). Retire the
   corresponding caveat in the CelWatch module comment and the PR body.
3. **Verify**: `proof run <id>` each of the three laws green under the
   new harness; existing green laws stay green (targeted t1 suite run,
   then one full `pnpm run check` — blocking foreground, ~4.5 min now).
4. **Promotion protocol (one PR)**: implementation + `targets.json`
   flips `red`→`green` for exactly the three law ids + the owning
   ledger row flip if a G3/WP row exists under `docs/execution/`
   (if no ledger row exists, say so in the PR body — do not invent one).
5. Mark PR #121 ready (it stays the packet's PR — force-push the rebased
   branch to it).

## Freezes and scope guards

- The three stream-law BODIES in `apps/proofs/StreamLawProofs.fs`:
  FROZEN. If the implementation cannot satisfy a law as-migrated and you
  believe the law is wrong — GATE, stop that law, escalate with
  specifics.
- `Firegrid.Durable.fs` contract SIGNATURES: frozen; only the packet's
  own section bodies may change (per the standing file discipline).
- All other red laws stay red; all green stay green; `targets.json`
  changes = exactly the three status flips.
- Harness files (`apps/proofs/*.fs` infra): untouched. If a stream law
  proves starvation-flaky under the concurrent pool, tag it serial in
  the Registry with evidence (allowed, record it) — do not touch pool
  internals.

## Operating rules

Fresh worktree onto the EXISTING branch; `git fetch` first;
`SKIP_SIMPLE_GIT_HOOKS=1`; never `git add -A`; Fable traps (inline
typeof / CompiledName / rec-namespace monomorphization); known
parked-signal flake → re-run before blaming; full checks BLOCKING
FOREGROUND, never background-and-stop. Push after your first
meaningful commit (the rebase force-push counts) and per milestone.

## Exit criteria

1. Full check green: 39 registered — 33 green, 6 expected-red, 0 errors.
2. The three laws green via the frozen migrated bodies; CelWatch ruling
   implemented; interim seeding gone; caveat retired.
3. PR #121 ready with: rebase summary, ruling-completion note,
   scoreboard tail, and the promotion flips. NOT merged.
