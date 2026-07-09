# Dispatch Brief: Packet 0.3c — shared s2 instance + concurrent proof execution

Doc-Class: dispatch-brief
Date: 2026-07-09
Packet: PHASE 0.3c (serial queue; dispatches after P0.3b merges)
Branch: `p0/shared-s2-concurrency` · Draft PR immediately after first commit
Architect: this session — escalate on anything marked GATE
Merge gate: architect review
Origin: human directive — "bring up a single s2 server instance and have
every proof run against its own isolated stream instances … run all of
the proofs concurrently."

## Mission

Replace per-trial s2-lite boot/teardown (~54 cycles per check) with ONE
shared s2-lite per runner invocation + trial-scoped basin isolation, and
make the runner execute proofs concurrently with a bounded pool. Ground
truth enabling this (architect-verified): **no proof faults the s2
server itself** — all kill/pause faults target worker host processes
(`replay-host`, `saga-host`, `zombie-host`). Basin isolation is
therefore sufficient for every existing proof.

## Rulings

1. **Shared instance**: `proof run` / `proof targets` boots one s2-lite
   (random port, one readiness wait) before the first trial and tears it
   down after the last. `s2Lite` property declarations resolve to the
   shared instance. Lifecycle spans move to runner scope.
2. **Trial-scoped basins**: every basin created through the harness
   (`CorpusSupport.workloadBasin` and any other creation path) is
   prefixed with a sanitized trial id (respect S2 basin-name charset
   rules — lowercase/digits/hyphen; verify against the client's
   validator). No law body changes — the prefix is applied in harness
   support code. GATE if any proof creates a basin through a path you
   cannot prefix without touching a frozen body.
3. **`s2LiteDedicated` escape hatch**: keep a per-trial-instance
   resource spec (the current behavior, renamed) for future laws that
   fault s2 itself. Currently used by ZERO proofs — do not migrate
   anything onto it.
4. **Bounded concurrency**: the runner executes a suite's proofs in a
   worker pool. Size: `--concurrency N` flag > `PROOF_CONCURRENCY` env >
   default 4. `--concurrency 1` is the escape hatch and must reproduce
   today's serial behavior exactly.
5. **Serial tail bucket**: proofs can be tagged timing-sensitive in the
   Registry; tagged proofs run AFTER the pool drains, one at a time.
   Initial tag set (architect ruling): `wake.tail-latency` (asserts a
   latency bound), `t1.andbang-teaching` (asserts a fast branch beats a
   400ms-slow one), `durable.parallel-overlap` and
   `durable.parallel-fault-isolation` (observe true concurrency
   mid-flight). You may ADD tags if stress runs prove a law
   starvation-flaky — record every addition + evidence in the PR body.
   You may not REMOVE from the initial set.
6. **Protocol discipline under concurrency**: targets-mode result lines
   are printed only by the coordinator, one atomic line per completed
   proof (completion order is fine — the ratchet runner is
   order-insensitive; confirm by reading
   `scripts/targets/run-targets.mjs`). ALL trial diagnostics stay on
   stderr. Negative-control trials count against the same pool.
7. **Port-bind resilience**: concurrent trials spawn more processHosts —
   give ProcessHost (and the dedicated s2 path) a bind-collision retry
   (new random port, up to 5 attempts) instead of failing the trial.
8. **Trial artifacts unchanged**: per-trial dirs, `spans.jsonl`,
   `report.json`, replay commands all keep their shapes. Replay boots
   its own shared instance; `--trial-id` reuse semantics unchanged.
9. **Teardown ordering**: a trial's hosts stop before its verifiers run
   where they do today; the shared s2 outliving trials should eliminate
   the `ECONNREFUSED (retrying)` teardown noise — confirm it's gone in
   the stress runs and note it in the PR body.

## Milestones (push after FIRST commit; draft PR immediately; push per milestone)

- **M1** — shared instance + trial-prefixed basins, concurrency still 1.
  Full check green with scoreboard IDENTICAL to post-0.3b main
  (expected 39 registered — 30 green, 9 expected-red, 0 errors) and s2
  boots down from ~54 to ~1 per suite invocation.
- **M2** — pool execution + serial tail bucket + port-bind retries +
  coordinator-only stdout.
- **M3** — stress: THREE consecutive full `pnpm run check` runs at the
  default concurrency, all green, timed. Any flake → diagnose; if
  starvation, tag the law serial (recorded); if a real bug in the
  concurrency layer, fix it. Record before/after wall-clock.
- **M4** — full check (blocking foreground, wait in-session), PR body:
  timings table (serial baseline vs concurrent), boots-per-check count,
  serial-tag additions with evidence, ECONNREFUSED-noise confirmation,
  scoreboard tail. Mark ready.

## Freezes and scope guards

- The 22 corpus law bodies, the post-0.3b foundation family/kept bodies,
  `p0.harness-kill-demo`, t0, `apps/proofs-legacy/`, all `src/` product
  code: FROZEN (GATE).
- `targets.json`: ZERO changes — same suites, same ids, same statuses.
  This packet changes HOW proofs run, never WHAT is proven.
- Law/property `timeoutMs` values: unchanged (they are per-trial bounds
  and remain valid under the pool).

## Operating rules

Same as prior packets (fresh worktree, `git fetch`,
`SKIP_SIMPLE_GIT_HOOKS=1`, never `git add -A`, Fable traps, known
parked-signal flake → re-run before blaming). Final check: BLOCKING
FOREGROUND, never background-and-stop.

## Exit criteria

1. Three consecutive full checks green at default concurrency; wall-clock
   improvement quantified in the PR body.
2. One s2-lite boot per runner invocation (dedicated path exists but
   unused); trial basins prefix-isolated.
3. `--concurrency 1` reproduces serial behavior; scoreboard and
   `targets.json` byte-identical to main.
4. PR ready, NOT merged.
