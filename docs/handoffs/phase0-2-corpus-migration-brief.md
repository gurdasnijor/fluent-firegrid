# Dispatch Brief: Packet 0.2 — migrate the 22 corpus laws into the apps/proofs harness

Doc-Class: dispatch-brief
Date: 2026-07-08
Packet: PHASE 0.2 (serial queue head; P0.1 merged as #123, main green)
Branch: `p0/corpus-migration` · Draft PR immediately after first commit
Architect: this session — escalate on anything marked GATE
Merge gate: architect review AND a **human ratification read** (law bodies
are being re-expressed; the human ratified the originals in #117)

## Mission

Re-express the 22 T1 corpus laws (`src/Firegrid.Durable.Corpus/`) as
black-box `property { workload …; verify … }` proofs in the P0.1 harness
(`apps/proofs`), preserving each law's id, semantic content, and ratchet
status (14 green stay green, 8 red stay red), and adding what the old
corpus lacked: trace-backed evidence, a negative control per green
family, and the history-checking upgrade for
`t1.entity-exclusive-serialization`. Read first:

1. [`docs/handoffs/platform-greenmaking-handoff.md`](platform-greenmaking-handoff.md) — Phase 0.2 statement.
2. [`docs/proofs-inventory.md`](../proofs-inventory.md) — the 22-law table (section A) and consolidation rules (L5: corpus count is FROZEN — 22 in, 22 out).
3. [`targets-README.md`](../../targets-README.md) — suite protocol + strict drift rules; these are your safety net and your acceptance test.
4. `apps/proofs/` — the harness you are consuming (see `HarnessKillDemoProof.fs` for the authoring shape incl. negativeControl, ProcessHost, chdb checks).
5. Inspiration for style, NOT for scope: https://github.com/s2-streamstore/s2-verification — drive only public surfaces, collect operation histories, verify from evidence; their constant-space hash-chain state machine is the template for the entity-serialization upgrade.

## The one inviolable semantic rule

**Re-expression may only ADD strength, never remove it.** Every assertion
in an old law body must have an identifiable successor check in the new
property; new trace-backed checks are additive on top. The PR body MUST
contain a law-by-law correspondence table:

| law id | old assertion(s) | new check(s) | evidence source | delta |

where `delta` is `none` or an explicit `ADDITIVE: <what>` note. This
table is what the human ratification read reviews. A law whose table row
cannot honestly say "no weakening" is a GATE — stop and escalate that law.

## Rulings

- **Layout**: law files move into `apps/proofs` (keep the
  CoreLaws/EntityLaws/StreamLaws/FlowLaws grouping; `fixtures/` for the
  golden-wire law moves too). Registered in the harness `Registry`.
- **Suite tagging**: extend the registry so each proof belongs to a named
  suite (`p0-harness`, `t1-durable`); generalize
  `proof targets <suite>` to run exactly that suite's proofs and emit one
  `{id, pass}` line per registered law. Law ids UNCHANGED — exactly the
  22 ids in `targets.json`.
- **Red laws run red**: the 8 red laws must execute their workloads
  against the real public surface and FAIL as laws (reported
  `pass: false`), not crash the suite (suite still exits 0). A red law
  that cannot even be expressed against the current public surface is a
  GATE finding, not a skip.
- **Atomic swap at the end**: `targets.json`'s `t1-durable` suite command
  repoints to `node apps/proofs/dist/Main.js proof targets t1-durable`
  in the SAME commit that removes `src/Firegrid.Durable.Corpus/` from the
  build (project deleted; slnx + root scripts + `corpus` script
  repointed/retired). Never have both runners registered — duplicate ids
  are a ratchet error by design. Until that commit, the old suite keeps
  running on your branch.
- **Corpus support code**: the old corpus `Harness.fs`/`Node.fs`
  mechanics (child scenario hosts, s2-lite per law, kill/pause) are
  replaced by harness resources: laws declare `processHost`/`s2Lite` and
  use `ctx.Faults`. Where laws need faults the harness lacks —
  `t1.entity-zombie-fenced` needs SIGSTOP/SIGCONT — extend
  `FaultController` with `PauseHost`/`ResumeHost` following the KillHost
  pattern exactly (lifecycle spans `verification.host.pause/resume`,
  report-level fault events, undeclared-host rejection). Harness
  extensions are in scope; keep them minimal and canary-covered where
  cheap.
- **Negative controls**: one per family with at least one green law —
  families: core-replay {replay-determinism, currentTime, bounded-loop},
  composition {fanout, andbang, select}, delivery {signal-to-parked,
  timer-across-restart}, flow {saga, cancellation, declare-implement},
  observation {status-and-result}, wire {golden-fixtures}. Attach each
  control to a green law; a control must fail for the expected reason
  (e.g. a variant that skips the kill, replays from a truncated journal,
  or uses a knowingly-wrong fixture). All-red families (entity, streams,
  children/eternal) get their controls in their promotion packets — note
  this in the PR body, don't fake controls against failing positives.
- **History-checking upgrade** (pre-authorized law-body strengthening,
  human directive in the Phase 0 statement):
  `t1.entity-exclusive-serialization` upgrades from reply-set equality to
  operation-history checking — each of the 40 concurrent calls records an
  operation (ProofOperation spans); the entity's Decide fold carries a
  hash chained over applied operations (s2-verification's constant-space
  state machine); verification asserts the observed chain forms exactly
  one linear history containing every accepted call exactly once, and
  reply-set equality stays as a secondary check. The law stays RED (its
  implementation is G2's packet). Mark the row `ADDITIVE: history-check`.
- **Namespace**: migrated laws may take the harness namespace — they are
  being re-expressed anyway (unlike 0.1's zero-edit move).

## Milestones (push after FIRST commit; draft PR immediately; push per milestone)

- **M0** — suite tagging + `proof targets <suite>` generalization;
  `p0-harness` canary unaffected; full check green.
- **M1** — core + delivery + observation green laws migrated (≈9 laws)
  with their negative controls; verified locally via
  `proof run <law-id>` (old suite still wired for CI).
- **M2** — flow + wire green laws (≈5) + remaining green-family negative
  controls.
- **M3** — the 8 red laws re-expressed (entity incl. the history-checking
  upgrade + PauseHost/ResumeHost, streams, child-spawn,
  eternal-continueasnew); each verified to fail as a law, not crash.
- **M4** — atomic swap commit (suite repoint + corpus project removal +
  script/slnx cleanup); full `pnpm run check` green (expect scoreboard:
  25 targets, 16 green, 9 expected-red — identical to today); PR body
  gets the correspondence table + scoreboard tail; mark ready.

## Freezes and scope guards

- `src/Firegrid.Durable/` (contract + product): FROZEN (GATE). The laws
  already import only `Firegrid.Durable` — keep it that way.
- Law SEMANTICS frozen per the rule above; law COUNT frozen at 22 (no
  merging, no splitting, no new laws).
- `src/Firegrid.Foundation.Proofs/` bodies: untouched (Packet 0.3).
- `apps/proofs-legacy/`: untouched (Packet 0.4).
- `p0.harness-kill-demo` canary: keep green.
- If a law's workload cannot be expressed without reaching around the
  `Firegrid.Durable` public surface, that is a platform-gap finding —
  GATE, record it, escalate, continue with other laws.

## Operating rules

Same as P0.1: fresh worktree, `git fetch` first,
`SKIP_SIMPLE_GIT_HOOKS=1` commits, never `git add -A`, Fable traps
(inline typeof / attribute erasure→CompiledName / rec-namespace
monomorphization), full check ≈ 9–13 min (run fully at M1 and M4 minimum;
targeted `proof run` in between). Ceremony precedents: PR #117 (corpus
ratification), #123 (harness rebuild).

## Exit criteria

1. `pnpm run check` green: 25 targets, 25 reported, 16 green /
   9 expected-red, 0 errors — the SAME scoreboard as before the packet
   (any drift = you lost or duplicated a law).
2. `src/Firegrid.Durable.Corpus/` no longer exists; `apps/proofs` runs
   suite `t1-durable`; law ids byte-identical to the manifest.
3. Every green family has a negative control failing-as-expected,
   visible in report.json evidence.
4. `t1.entity-exclusive-serialization` carries the history-checking
   verification (still red).
5. PR body: correspondence table (22 rows), scoreboard tail, negative
   control summary, deviations. Marked ready — NOT merged (architect
   review, then human ratification read).
