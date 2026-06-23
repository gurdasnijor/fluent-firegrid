# Handoff: effect-s2-durable host — build step 4 (fenced ownership + claim-sweep)

Decomposes **SDD §7 / build-plan step 4** into safe, ordered sub-slices and pins
the load-bearing decisions. Design + rationale live in
`docs/sdds/effect-s2-durable-host-process-model-sdd.md`; this is the "how to build
it without subtle distributed bugs" companion. Steps 1–3 (the host surface +
dogfood) are done (PR #47, commits `4256d45`, `8950284`).

## Why step 4 must be decomposed (the coupling the SDD glosses)

The SDD lists step 4 as one lump: "fenced ownership + claim-sweep + lease
heartbeat + list+claim worker." Mapping the current drainer (`src/actor/object.ts`)
shows these are **coupled, not independent**:

- Single-writer today is **in-process only** — a per-owner-stream `Semaphore` lock
  + the `started` set + the `snapshots` cache (`object.ts:248-306`, `392-426`).
  Two *processes* have independent locks → both drain the same owner stream → both
  append `StateChanged`/`Journaled`/`Completed` for the same head ⇒ **double-apply
  across hosts**. Admission (`Accepted` CAS) is already cross-host safe; the *drive*
  is not.
- **Fence-alone is unsafe.** Adding a per-host fence token with no lease gating
  makes takeover *last-claimer-wins*: two live hosts serving the same key
  ping-pong stealing ownership (thrash). Fence (correctness: the displaced holder
  *stops*) and lease-gating (liveness: only steal from a *dead* holder) must land
  together.

So fence + lease are the keystone and ship as one slice; the proactive worker and
timer re-arm layer on top.

## Pinned decisions

1. **Host token** — a per-process opaque token minted at `InvocationStore` layer
   build via `effect/Random` (mirror `service.ts:247` `freshNonce`:
   `[nextInt×3].map(abs→base36).join("-")`), e.g. `host-<…>`. Stable for the
   process lifetime; identifies this host as a fence holder.
2. **What is fenced (carries `fencingToken: hostToken`)** — only the **drive**
   appends, i.e. everything `makeBackend` + `runOne` write while a head runs:
   `StateChanged` (set/delete), `Journaled` (state-read facts + run/sleep journal),
   the handler-side `signal.resolve`, and the `Completed` append.
   **NOT fenced (any host may append, first-write-wins via the fold):** `admit`'s
   `Accepted` CAS and the external `resolveSignal` `SignalResolved` — these are
   enqueue/ingress, not drive.
3. **Claim mechanics** — at drain entry, under the in-process lock, append
   `AppendRecord.fence(hostToken)` once (claim + lease refresh), but ONLY after the
   lease-gated decision (4) says we may own it.
4. **Lease detection** — `readFenceState(stream)` returns
   `{ holderToken: Option<string>, lastWriteMs }`. `lastWriteMs` from
   `checkTail` (`tail.timestamp: Date`). `holderToken` from a bounded tail read that
   **includes command records** (the latest `fence` record carries the token) —
   the drive read keeps `ignoreCommandRecords: true`, this one does not.
5. **Claim decision (pure policy)** — `decideClaim({holderToken, lastWriteMs},
   myToken, nowMs, leaseMs)`:
   - holder is me → `Own` (proceed; refresh).
   - no holder OR `nowMs - lastWriteMs ≥ leaseMs` → `Claim` (append fence, proceed).
   - a different, live holder → `Yield` (do NOT drive; the live holder owns it).
6. **Follow-on-mismatch** — any fenced drive append failing with `S2Conflict`
   (fencing) ⇒ a peer claimed mid-drive ⇒ abort this drain cleanly (stop driving;
   drop this stream's in-process `started`/`snapshots` entries so a later drain
   re-reads from truth). Surface as a benign "lost ownership", not an error.
7. **leaseDurationMs / heartbeat** — the one empirical knob (SDD §12). Default
   lease 30s, heartbeat every lease/3 (10s). A slow `run` step must re-fence within
   the lease or be preempted; the heartbeat fiber (slice S4.3) touches the stream.
8. **Service path stays as-is** — services keep the in-process `running` map +
   roster until step 5 (service-path unification).

## Sub-slices (each independently safe + testable; OUR logic, not S2's)

- **S4.1 — fenced append mechanics** (no behavior change; token defaults to
  undefined = today). `effect-s2`: add `guardedAppend(name, schema, value,
  options: AppendOptions)` so the JSON encoding stays single-source; refactor
  `conditionalAppend` to delegate. `effect-s2-durable/actor/log.ts`: `ActorLog`
  gains a token-carrying append/casAppend + `readFenceState`. Covered by S4.2's
  test (a standalone behavioral test here would just be re-proving S2's fence
  contract — don't).
- **S4.2 — lease/claim policy + drainer wiring (THE keystone).** Mint `hostToken`
  at layer build; `decideClaim` pure fn; wire into `drain`: compute decision →
  `Yield` returns without driving, `Claim`/`Own` fence + drive with the token;
  follow-on-mismatch aborts cleanly. **Test (our logic): two engines over ONE
  shared `s2 lite` contend on one object key → exactly one drives to completion,
  the other yields; the effect is applied once.** Needs a shared-s2lite, two-engine
  fixture — add `test/two-host-support.ts` (one `s2 lite`, two
  `DurableExecutionRuntime` layers over it).
- **S4.3 — lease heartbeat** for slow steps (knob default + a short-lease test with
  a deliberately slow `run` step; assert no preemption while heartbeating).
- **S4.4 — claim-sweep worker** — a host fiber: `listStreams({prefix:"obj/"})`,
  per stream read head + lease, lease-gated `Claim`+drive any pending un-`Completed`
  head. **Replaces** the current unconditional `objectBootRecover`
  (`Runtime.ts:502-516`, which forks a drain per key with no lease check — unsafe
  cross-host) and subsumes boot recovery (§5). Test: park a head on host A, stop A,
  B's sweep picks it up after lease expiry.
- **S4.5 — timer re-arm** — fold pending `sleep`/`clockWakeups` (`schema.ts:59`,
  `runtime/primitives.ts:147` — today firing is bound to handler re-run, no daemon)
  into the sweep so deadlines fire without a request.

## Verification (same gates as steps 1–3)

From `packages/effect-s2-durable/` and `packages/effect-s2/` (S4.1 touches both):
`pnpm exec tsc --noEmit`, `pnpm exec eslint . --max-warnings 0`,
`pnpm exec effect-language-service diagnostics --project tsconfig.json --strict`,
and from repo root `pnpm run lint:dead`. The `s2` binary IS on the build machine,
so `pnpm test` runs the S2-backed + new two-host tests live. If S4.1 changes
`effect-s2`'s public API, add a `.changeset/`.

## References (already gathered — don't re-fetch)

- S2 fencing is a documented first-party capability (`AppendRecord.fence(token)`,
  `AppendOptions.fencingToken`, `FencingTokenMismatchError`→`S2Conflict` with
  `expectedFencingToken`; `effect-s2/examples/10-command-records.ts`). Build on it;
  don't write tests re-proving it.
- `claimSharedGeneration`/`resumable-stream` is NOT vendored — port the *concept*
  (claim-before-drive, lease takeover, resume-from-seqnum), not code.
- `S2Client.listStreams({ prefix })`, `checkTail` (`tail.timestamp: Date`),
  per-record `timestamp` — all present in `effect-s2`.
