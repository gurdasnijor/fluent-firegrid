# Dispatch Brief: Phase A / G4 — children + eternal (final two laws → 22/22)

Doc-Class: dispatch-brief
Date: 2026-07-09
Packet: PHASE A item 3, FINAL product packet (G3 #121 and G2 #122 merged
→ 36 green; this packet closes t1 at 22/22)
Branch: NEW `g4/children-eternal` from current main (the old branch of
that name was killed at start and may be empty/stale — do not build on
it; delete the remote ref if it exists after your PR opens)
Architect: this session — escalate on anything marked GATE
Merge gate: architect review

## Mission

Green the last two platform laws and adopt the ruled one-way send:

1. **`t1.child-spawn`** — `CallChild` / `SpawnChild` /
   `ChildHandle.Await` over K1's `PerformChild` kernel primitive and its
   adapters. Fan-out results preserve declaration order; children
   execute exactly once (the frozen law body in
   `apps/proofs/FlowLawProofs.fs` pins this).
2. **`t1.eternal-continueasnew`** — `defineEternal` / `Eternal<'state>`
   over K1's ContinueAsNew rollover. Generation chain: each generation
   runs its step exactly once on a fresh journal excluding its
   predecessor's; **`Run.Result` and `Client.attach` MUST follow
   generation chains** (`<base>/gen/N` ids) to the chain's terminal.
3. **Ruled adoption**: `Step.Send` becomes a TRUE one-way send over K1's
   kernel one-way send (journaled, non-awaited, exactly-once-effective)
   — per the contract doc's ruling. The 20 already-green laws are the
   regression guard for this adoption.

New product code in `InternalChildren.fs` (packet file discipline);
contract entity/child/eternal section bodies only. Ceremony precedents:
PR #119 and #121 bodies. Target scoreboard: **39 registered — 38 green,
1 expected-red (t0.wiring-red only), 0 errors** — t1 at **22/22**.

## Kernel context (K1 debts — build on them as they are)

K1 (PR #118) landed the primitives you consume: rollover with
`<base>/gen/N` generation ids, child ids `<parent>/child/<opId>`,
one-way send with ack-guarded exactly-once. Three RECORDED debts you
must respect, not fix (they are Phase B items — fixing them here is
scope creep and a GATE):
- `WakeReason.ChildTerminal` wake is unwired — parent wake-up floor is
  inbox+sweep. The child-spawn law's timeout (180s) accommodates this;
  correctness first, latency later.
- Send handler failures are swallowed-and-acked (flagged, accepted).
- Rollover terminal is a History event.

## Freezes and scope guards

- Law/proof bodies in `apps/proofs/`: FROZEN (the two law bodies are
  your spec; if either cannot be satisfied as written — GATE with
  specifics).
- Contract SIGNATURES frozen; section bodies only.
- G3/G2 landed code untouched; harness infra untouched (serial Registry
  tag with evidence is the only allowed harness edit).
- `targets.json`: exactly 2 status flips.
- Kernel (`Internal.fs` K1 primitives): consume, don't rework. Small
  adapter-level hooks are fine; kernel semantics changes are a GATE.

## The work

1. Branch from main; implement `InternalChildren.fs` + contract section
   bodies; wire `Step.Send` adoption.
2. `proof run` each law to green; full t1 suite (regression guard for
   the Send adoption — watch `t1.signal-to-parked-across-restart` and
   the saga/cancellation laws especially); one full `pnpm run check`
   (blocking foreground, ~4.5 min).
3. Promotion: exactly 2 flips + implementation in one PR. No ledger row
   exists — say so.
4. Draft PR immediately after first commit (title: "G4: children +
   eternal — t1 at 22/22"); push per milestone; mark ready when done.

## Operating rules

Fresh worktree; `git fetch` first; `SKIP_SIMPLE_GIT_HOOKS=1`; never
`git add -A`; Fable traps (inline typeof / CompiledName / rec-namespace
monomorphization); parked-signal flake → re-run before blaming; full
checks BLOCKING FOREGROUND; never stop with a run in flight.

## Exit criteria

1. Full check green: 39 — 38 green, 1 expected-red, 0 errors; t1 22/22.
2. Both laws green via frozen bodies; `Run.Result`/`Client.attach`
   follow generation chains; Step.Send is true one-way with the 20
   green laws as regression evidence.
3. PR ready with scoreboard tail + regression-suite evidence. NOT
   merged.
