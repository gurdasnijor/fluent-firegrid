# Proof Estate Inventory & Consolidation Map (both repos)

Doc-Class: inventory / architect working note
Date: 2026-07-08
Status: PROPOSED — dispositions below are architect recommendations, not yet ratified
Feeds: Phase 0.1 (harness rebuild brief), 0.2–0.4 (migration/retirement packets)

## Headline

**113 proof/law records exist across the two repos today. The consolidated
target is ~46 properties in one harness** — 22 frozen corpus laws + ~13
kernel-tier properties + 5 substrate properties + 6 adapter-tier proofs —
with ~60 records retired or deduplicated under recorded dispositions and
~25 (eff-firegrid's own) staying where they are.

| Estate | Records | Wired | Disposition summary |
|---|---|---|---|
| fluent: `src/Firegrid.Durable.Corpus/` | 22 laws (14 green / 8 red) | yes (ratchet `t1-durable`) | ALL migrate in 0.2 (frozen; re-expression only) |
| fluent: `src/Firegrid.Foundation.Proofs/` | 28 properties / 15 files | yes (`pnpm run proofs`, not ratcheted) | consolidate → ~13 kernel-tier properties (0.3) |
| fluent: `apps/proofs/` wired TS | 16 | yes (`main.ts`, not ratcheted) | 10 substrate → 5 F# properties; 6 D-lane stay TS for now (0.4) |
| fluent: `apps/proofs/` dead TS (`store-*`) | 22 | **no** (confirmed unimported) | retire with disposition map (0.4) |
| eff-firegrid: `src/Proofs/` | 25 (all active) | yes (Registry/Runner) | STAY in eff-firegrid; harness infra + authoring patterns port, proofs don't |

## What system properties are already proven (property-family view)

Collapsing all 113 records by the invariant they actually establish:

1. **Replay determinism / exactly-once step execution across kill** —
   t1.replay-determinism-across-kill, t1.deterministic-currentTime,
   t1.bounded-loop-flat-stack, foundation.durable-replay, eff
   `durable-semantics-tier1`. Strongest form: the corpus laws (real
   SIGKILL). eff's is pure in-process.
2. **Fencing / single-writer / zombie deposition** — the most
   over-proven family in the estate: t1.entity-zombie-fenced +
   entity-exclusive-serialization (corpus, red);
   session.lifecycle-single-writer, lifecycle-deposed-producer,
   turn-crash-terminal, turn-idempotent-create, resume-artifact-fenced,
   wake.single-claim, state.checkpoint-race (foundation);
   store.object-live-fencing/stale-owner (dead TS);
   effect-s2/firegrid-log fence-semantics ×2 (wired TS); **six**
   layer-restatements in eff-firegrid. ≈15 records, one invariant shape.
3. **At-most-once side effects under crash/retry (publish-before-checkpoint
   windows)** — durable.continue-as-new, durable.child-workflow,
   durable.one-way-send, durable.parallel-kill-window (foundation K1/K2);
   four adapter restatements in eff-firegrid.
4. **Fold/rebuild equivalence & trim safety** —
   state.checkpoint-rebuild-equivalence, state.trim-safety,
   session.history-fold, foundation.state-view, foundation.kv-store,
   foundation.subject-history; eff's three Foundation proofs are the same
   shapes over that repo's kernel.
5. **Read grades (strong/eventual/through) & observable lag** —
   t1.three-read-grades (corpus, red), state.stateview-strong-read,
   session.projection-lag-observable, t1.entity-shared-read-nonblocking.
6. **Durable delivery: signals, timers, waits** —
   t1.signal-to-parked-across-restart, t1.timer-across-restart,
   t1.cel-wait, wake.timer-exactly-once, wake.tail-latency,
   durable.mailbox admission; eff signal/timer admission proofs; dead TS
   awakeable/timers-signals/state-wait/index-wait.
7. **Streams: byte-faithful attach, terminal ordering** —
   t1.log-attach-byte-faithful (corpus, red) vs session.turn-attach
   (foundation) — a direct two-layer duplicate.
8. **Composition semantics: fan-out/join, races, children, eternal** —
   t1.fanout-and-join, tagged-select-race, andbang-teaching, child-spawn,
   eternal-continueasnew, saga-compensation, recoverable-cancellation,
   declare-implement-roundtrip; durable.parallel-overlap/fault-isolation.
9. **Substrate capabilities (S2): atomic CAS append, read-after-append,
   cursor fold, fencing tokens** — 10 wired TS proofs, every property
   proven TWICE (upstream SDK + `@firegrid/log` wrapper); eff's
   `durable-s2-substrate` proves the same family in F#.
10. **Wire/format pinning & adapter reconstruction** —
    t1.golden-wire-fixtures; l1-vocabulary conformance,
    harness.fixture-replay/resume-suppression, claude-adapter ×3 (D-lane).

## Consolidation levers (the "dramatic" part)

**L1 — Retire the 22 dead `store-*` TS proofs.** Zero are imported by any
runner (grep-verified); README already flags them as porting references.
Every load-bearing property has a successor: object
serialization/cross-host/stale-owner/live-fencing → t1.entity-* laws;
awakeable/timers/signals/state-wait → t1 signal/timer/cel-wait laws;
delayed-send → G4's Step.Send adoption + the parked fluent-retirement
parity audit (Phase E.2 — note delayed-send parity there before deleting).
Disposition table below is the record the handoff's 0.4 asks for.

**L2 — Stop dual-proving the S2 substrate.** The 10 wired capability
proofs prove each property twice (SDK + wrapper). The upstream SDK's
behavior is S2's own verification burden (s2-verification exists for
exactly this). Keep ONE property per capability over `@firegrid/log` —
the product's actual dependency boundary — re-expressed in the F# harness:
5 properties (atomic-replay, read-after-append, cursor-fold,
CAS-contention, fence-semantics).

**L3 — Reorganize the 28 foundation proofs by invariant family, not by
module.** The current estate restates the same invariant per-module (the
same anti-pattern eff-firegrid exhibits at 6 layers). Target ~13
kernel-tier properties:
- one parameterized **fencing** property instantiated over {lifecycle,
  wake-claim, checkpoint-commit, resume-artifact, turn-takeover}
  (collapses ~7);
- one **crash-window at-most-once** property instantiated over
  {continue-as-new, child-result, one-way-send, parallel-batch, timer}
  (collapses ~5; these keep their white-box crash-window seeding — the
  black-box laws cannot force a kill between commit and dispatch);
- one **rebuild-equivalence** property over {checkpoint+trim,
  session-history, state-view, kv-store} incl. the poisoned-decode
  fail-closed variants (collapses ~5);
- read-grade/lag properties fold under t1.three-read-grades plus one
  white-box lag-bound property;
- keep distinct: replay-core (durable-replay/mailbox/processor),
  wake.tail-latency, wake.timer-exactly-once, parallel-overlap,
  parallel-fault-isolation.
White-box is legitimate here (crash-window seeding, latency bounds) but
lives declared in the harness tree per 0.3 — never in `src/`.

**L4 — Do not port eff-firegrid's 25 proofs.** They verify THAT repo's
kernel, which stays there. What ports is the harness infrastructure
(Proof/Property/ProofBuilder CEs, Runner/Registry/Reports,
S2Lite/ProcessHost, TraceSql/TraceProof/Verification, Expect) and the
authoring discipline (every property pairs in-process Expect with a chdb
trace query — 25/25 there). Their 5 near-duplicate clusters are the
cautionary tale L3 is designed against.

**L5 — Corpus stays 22; consolidation there is FORBIDDEN** (frozen laws,
architect gate). 0.2 adds strength, not count: negative control per
family, history-checking upgrade for t1.entity-exclusive-serialization
(s2-verification's constant-space hash-chain state machine as template).

**L6 — D-lane adapter proofs (6) stay a separate TS suite for now.** They
touch no kernel/s2 surface (pure fixture replay) and their contract is
Phase D's de-Effect target; migrating them during Phase 0 buys nothing.

## Harness-design findings that must shape the 0.1 brief

1. **eff-firegrid's fault-injection machinery is UNUSED there (0/25
   proofs).** `ProcessHost.fs`/`FaultController`/`KillHost` exist but no
   proof declares a processHost resource. Our corpus is kill-heavy
   (8+ laws kill or stop hosts). The port must treat ProcessHost/KillHost
   as unproven code — the 0.1 demonstration proof should be a
   KILL-exercising proof, not a happy-path one, to de-risk this first.
2. **Negative controls: 1/25 in eff-firegrid** (`durable-semantics-tier1`
   only). The `NegativeControlSpec` mechanism exists but is barely
   exercised; 0.2 requires one per family. Exercise it in 0.1's
   demonstration proof too.
3. **Dual evidence is universal there (25/25 Expect + TraceSql/chdb)** —
   matches 0.2's trace-backed-evidence goal; this repo already has the
   chdb driver (`packages/trace`). Port this discipline as a harness
   default, not an option.
4. **Ratchet protocol unchanged**: new runner emits `targets.json` JSONL
   per suite; `report.json` becomes evidence. Foundation + substrate +
   adapter tiers should ENTER the ratchet as suites when migrated (today
   only the corpus is ratcheted — 66 of 88 fluent records run outside the
   red/green scoreboard, and 22 don't run at all).

## Dead `store-*` disposition map (proposed, for 0.4 ratification)

| Dead proof | Successor / disposition |
|---|---|
| store.object-serialization, object-cross-host | t1.entity-exclusive-serialization |
| store.object-live-fencing, object-stale-owner | t1.entity-zombie-fenced |
| store.object-state, object-replay-state | t1.entity laws + state.checkpoint-rebuild-equivalence |
| store.object-state-wait, object-index-wait | t1.cel-wait |
| store.awakeable, timers-signals | t1.signal-to-parked-across-restart, t1.timer-across-restart |
| store.runtime-timer-sweep, workflow-schedule, runtime-schedule-sweep | t1.timer-across-restart + wake.timer-exactly-once (schedules proper: T2 scenario corpus) |
| store.object-delayed-send, service-delayed-send | G4 Step.Send adoption; parity noted for Phase E.2 fluent-retirement audit |
| store.event-log-cas | firegrid-log.capability-b.match-seq-num-contention (kept, F#-ported) |
| store.leases, host-tick, host-crash-restart, runtime-end-to-end, runtime-approval, run-lifecycle | superseded wholesale by the T1 corpus (kill-replay laws) — retire |
| store.object-handles | t1.status-and-result-query + entity admission (G2 reserved-segment) |

## Full per-record inventories

### A. fluent-firegrid — corpus (22, frozen)

| id | file | property | status |
|---|---|---|---|
| t1.replay-determinism-across-kill | CoreLaws.fs | step executes exactly once across SIGKILL; journal serves recorded result on restart | green |
| t1.fanout-and-join | CoreLaws.fs | let!/and!/all sequence vs fan-out; results in declaration order | green |
| t1.tagged-select-race | CoreLaws.fs | select resolves to first-finishing tagged branch with its payload | green |
| t1.signal-to-parked-across-restart | CoreLaws.fs | signal sent with zero workers alive still delivered on re-attach | green |
| t1.timer-across-restart | CoreLaws.fs | durable timer fires at-or-after deadline across full worker outage | green |
| t1.entity-exclusive-serialization | EntityLaws.fs | entity Decide calls serialize exclusively across hosts; no lost/double applies | red |
| t1.entity-zombie-fenced | EntityLaws.fs | paused deposed owner fenced on resume; never double-applies | red |
| t1.entity-shared-read-nonblocking | EntityLaws.fs | shared reads run concurrent with exclusive writer, see valid fold prefixes | red |
| t1.typed-step-failure | CoreLaws.fs | step failures typed in/out of workflow; retries policy-bounded; Step.terminal bypasses | green |
| t1.deterministic-currentTime | CoreLaws.fs | currentTime captured once, journal-served on replay | green |
| t1.status-and-result-query | CoreLaws.fs | status/result observable from any client by id; stable post-completion | green |
| t1.log-attach-byte-faithful | StreamLaws.fs | one attach: recorded prefix + live tail + terminal, byte-faithful, ordered | red |
| t1.three-read-grades | StreamLaws.fs | Latest linearizable; Eventual lagging valid prefix w/ lag as data; Through(v) ≥ v | red |
| t1.cel-wait | StreamLaws.fs | CEL wait: immediate-if-true, resumes only on satisfying change, replay-served | red |
| t1.saga-compensation-across-kill | FlowLaws.fs | mid-compensation SIGKILL: completed compensations un-repeated, rest completes | green |
| t1.recoverable-cancellation | FlowLaws.fs | cancel lands at bind boundary as catchable value; uncaught → typed Cancelled | green |
| t1.declare-implement-roundtrip | FlowLaws.fs | declared bodyless contract callable; implementation bound independently | green |
| t1.child-spawn | FlowLaws.fs | durable children exactly-once; fan-out preserves declaration order | red |
| t1.andbang-teaching | CoreLaws.fs | let! sequences; and! fans out (faster later branch may finish first) | green |
| t1.golden-wire-fixtures | StreamLaws.fs | derived wire encoding pinned to committed golden fixture | green |
| t1.eternal-continueasnew | FlowLaws.fs | ContinueAsNew fresh-generation journal; result follows chain | red |
| t1.bounded-loop-flat-stack | CoreLaws.fs | ≥500 guarded recursive iterations, flat stack, replay-convergent | green |

### B. fluent-firegrid — foundation proofs (28 across 15 files; all wired, none ratcheted)

| id | surface | property (short) | consolidation family |
|---|---|---|---|
| foundation.subject-history | SubjectHistory | OCC append guards stale writers; cursor ordered; foldTo = follower fold | rebuild-equivalence |
| foundation.state-view | StateView | deterministic fold; strong reads latest; decode failure poisons reads | rebuild-equivalence |
| state.stateview-strong-read | StateReads | strong linearizable; eventual monotonic lagging prefix | read-grades (→ t1.three-read-grades) |
| session.history-fold | SessionHistory | projection = fold-from-zero across checkpoint/restart | rebuild-equivalence |
| session.projection-lag-observable | SessionHistory | eventual reads lag-bounded by own strong reads; lag monotonic | read-grades |
| foundation.kv-store | KvStore | durable commit before local apply; poison-on-apply-failure | rebuild-equivalence |
| state.checkpoint-rebuild-equivalence | Checkpoint | snapshot+suffix ≡ fold-from-zero, incl. restart | rebuild-equivalence |
| state.trim-safety | Checkpoint.trim | never trim past committed checkpoint; floor rebuild equivalent | rebuild-equivalence |
| state.checkpoint-race | Checkpoint.commit | racing checkpointers: exactly one commits; stale → Regressed | fencing |
| foundation.durable-replay | Durable/Stepper | replay deterministic; stable positional op-ids; command-before-effect | replay-core (keep) |
| foundation.durable-mailbox | Mailbox | single-scan admission; source-provenance dedupe; cursor advance | replay-core (keep) |
| foundation.durable-processor | Processor.drive | commit precedes dispatch; provenance unforgeable; deposed never dispatches | replay-core (keep) |
| durable.continue-as-new | rollover | survives kill between terminal-commit and next-gen dispatch | crash-window |
| durable.child-workflow | children | child start + terminal delivery survive kill windows | crash-window |
| durable.one-way-send | send | journaled, non-awaited, exactly-once-effective across restart | crash-window |
| durable.parallel-overlap | K2 batches | true concurrency; fold by completion order, bind by op-id | keep |
| durable.parallel-kill-window | K2 batches | kill between publish and checkpoint loses/duplicates nothing | crash-window |
| durable.parallel-fault-isolation | K2 batches | thrower fails own tick only; retry heals only thrower | keep |
| session.turn-attach | DurableLog/Turn | byte-faithful prefix+tail+terminal for any attacher | DUPLICATE of t1.log-attach-byte-faithful |
| session.turn-crash-terminal | Turn recovery | deposed producer can't commit; recovery drives to observed terminal | fencing |
| session.turn-idempotent-create | Turn identity | same identity re-attaches (never forks); new identity deposes priors | fencing |
| session.lifecycle-single-writer | SessionLifecycle | one live turn per session; racing start fenced | fencing |
| session.lifecycle-durable-cancel | SessionLifecycle | cancel = durable mailbox send; resend folds once | delivery |
| session.lifecycle-deposed-producer | SessionLifecycle | post-takeover appends fail Deposed; recovery terminal observed | fencing |
| session.resume-artifact-fenced | ResumeArtifactStore | claim-then-read makes stale late store unobservable | fencing |
| wake.tail-latency | WakeShard/Router | wake reaches claimed handler within bounded latency | keep |
| wake.single-claim | WakeRouter | racing routers: exactly one advances durable cursor | fencing |
| wake.timer-exactly-once | TimerIndex | due timer fires once across restart; poison skipped w/o wedging | keep |

### C. fluent-firegrid — apps/proofs wired TS (16)

| id | surface | disposition |
|---|---|---|
| effect-s2.capability-a.atomic-replay | upstream SDK | drop (S2's burden) |
| firegrid-log.capability-a.atomic-replay | @firegrid/log | port → F# substrate property |
| effect-s2.capability-a.read-after-append | upstream SDK | drop |
| firegrid-log.capability-a.read-after-append | @firegrid/log | port |
| effect-s2.capability-a.cursor-fold | upstream SDK | drop |
| firegrid-log.capability-a.cursor-fold | @firegrid/log | port |
| effect-s2.capability-b.match-seq-num-contention | upstream SDK | drop |
| firegrid-log.capability-b.match-seq-num-contention | @firegrid/log | port |
| effect-s2.capability-b.fence-semantics | upstream SDK | drop |
| firegrid-log.capability-b.fence-semantics | @firegrid/log | port |
| l1-vocabulary.schema-conformance | @firegrid/l1-vocabulary | stay TS (D-lane suite) |
| harness.fixture-replay | @firegrid/harness-adapter | stay TS |
| harness.resume-suppression | harness resume | stay TS |
| harness.subagent-scoping | @firegrid/claude-adapter | stay TS |
| harness.claude.fixture-replay | claude adapter | stay TS |
| harness.claude.usage-facts | claude adapter | stay TS |

(Plus the 22 dead `store-*` proofs — disposition map above. The
`apps/proofs/test/*` files are harness unit tests, not proofs; superseded
by the rebuilt harness itself.)

### D. eff-firegrid — src/Proofs (25; stay in place, harness infra ports)

Infrastructure ported in 0.1: Proof.fs, ProofBuilder.fs, Property.fs,
Registry.fs, Runner.fs, Reports.fs, S2Lite.fs, ProcessHost.fs,
TraceSql.fs, TraceProof.fs, Expect.fs, TraceExpect.fs, ProofOperation.fs,
Verification.fs (+ DurableTestHost pattern as a shape reference).

Proofs (verify eff-firegrid's own kernel; NOT ported): 3 Foundation
(subject-history, state-view, kv-store), 13 durable-core white-box
(semantics, s2-substrate, stepper, host, command-dispatch,
activity-adapter, inbox-fold, activity-inbox, host-tick, timer-adapter,
client-admission, signal-admission, status), 8 facade black-box (runtime,
app-facade, test-host, environment-bootstrap, worker-loop, typed-status,
race-facade, firegrid-surface), 1 registry.

Notable stats: 0/25 use fault injection (ProcessHost unused); 1/25 has a
negative control; 25/25 pair Expect asserts with chdb trace queries;
near-duplicate clusters: 6-layer fencing restatement, 4-adapter
publish-before-checkpoint restatement, 10-proof "two-activity chain
completes" layer ladder, 7-proof signal-wait ladder, start/signal
duplicate-admission pair.
