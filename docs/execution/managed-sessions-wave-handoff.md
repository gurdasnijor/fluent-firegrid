# Managed Sessions — Wave Handoff Summary

Doc-Class: execution
Status: complete
Date: 2026-07-07
Owner: Firegrid Architecture (wave coordinator handoff to human)

The managed-sessions build wave over
[`managed-sessions-lanes.md`](./managed-sessions-lanes.md) is **complete**. Every
non-E-lane work packet is `done`, every proof obligation maps to a named
CI-green conformance row, and there are no open PRs. What remains is **Lane E
(agent-ui production integration, E1–E5)** — a **G4 human decision**, never the
coordinator's.

## Outcome — the durable managed-sessions substrate is built and proven

| Milestone | Capability | WPs | Status |
| --- | --- | --- | --- |
| MS-C1 | Checkpointed fold (snapshot + rebuild, race, trim) | A1, A2 | ✅ done |
| MS-C2 | Turn streams: `Authority` (I5) + `DurableLog` (I1) + Turn + proofs | B1, B2 | ✅ done |
| MS-C3 | Wake path: shard stream + tail-driven router + timer index + proofs | C1, C2 | ✅ done |
| MS-C4 | State reads (strong/eventual) + session-history fold + thread index | A3, A4 | ✅ done |
| MS-C5 | Lifecycle authority (claim/cancel/timeouts) + fenced resume-artifact store | B3, B4 | ✅ done |
| MS-C6 | L1 vocabulary + adapter contract + Claude Agent SDK adapter | D1, D2, D3 | ✅ done |

Ports P1–P5 and lane-F F1/F3/F4 were already `done` at wave start; F2 is the
rolling conformance discipline (every capability PR carried its own rows).

**19 PRs merged this wave (#95–#113).** F#-native, EffSharp-free, Fable-safe
throughout; TS (D-lane) is Effect-shaped per LLMS.md.

## Proof / conformance state — F2 sweep clean

All 25 wave invariants (**INV-012 … INV-036**) are `ci-green` in the conformance
bridge, each bound to a named proof driven through the public surface:

- MS-C1 → INV-018/019/020 · MS-C2 → INV-015/016/017 · MS-C3 → INV-031/032/033
- MS-C4 → INV-034/035/036 · MS-C5 → INV-024/025/026/030 · MS-C6 → INV-021–023, 027–029
- Substrate/foundation ports → INV-001–004, 012–014.

The late-added laws are all bound: **poison-tolerance** (INV-033), **single-claim
Option-A reword** (INV-032), **claim-then-read** (INV-030), **StateReads**
(INV-034), **history-fold + EndCause preservation** (INV-035). INV-005–011 are
the documented pre-port legacy (`not-active`, historical references only).

## Cross-lane interfaces ratified

I1 (DurableLog + Turn), I2 (L1 vocabulary), I3 (wake record + shard naming),
I4 (Checkpoint/Snapshot), I5 (`Authority`) — plus **I6 newly registered**
(`SessionLifecycle.LifecycleFact` on `sessions/{s}/log`; writer = B3 holder,
consumer = A4 history fold; G1-gated). All changes to these gate G1.

## Consequential architect rulings during the wave (for the record)

- **B1/A1 surfaces** ratified with idempotent-per-holder `claim` and the
  monotonic-snapshots (`Regressed`) law.
- **B3 `TurnTerminal.TimedOut` (I1):** kept I1 unchanged — timeouts seal to
  `Cancelled`, the distinct cause lives as `EndCause` on the session log.
- **C1 poison-tolerance deviation:** approved on merits (Option A) — a deposed
  router may perform one harmless idempotent re-drive; it cannot advance the
  cursor. **Binding process rule** issued: surface deviations must
  amend-as-first-commit + stop + escalate, never fold in.
- **A4 Q1:** the history fold folds **B3's session log** (not a dedicated
  subject) — a separate log would dual-write and collapse the `EndCause`
  distinction; I6 registered; no-trim guard on the shared multi-fold subject.

## What's next — Lane E (your call, G4)

The substrate is ready for agent-ui integration. E-lane is flag-gated,
one-deploy-revert, human-signed-off per the SDD:

- **E1** (attach replaces the resumable store, MS-M1) — **unblocked** (B2 done).
- E2 (lifecycle on kernel authority) needs B3/B4 (done) + E1.
- E3 (history/threads as projections) needs A2/A4 (done) + E2.
- E4 (event loop becomes adapter) needs D3 (done) + E3.
- E5 (soak week + second-harness smoke) needs E4 + C2 (done).

Each E-WP deploys to the production home-observability agent-ui and is **your G4
decision** — the coordinator never dispatches E-lane. When you choose to start
E1, the platform primitives, proofs, and conformance evidence it builds on are
all in place.
