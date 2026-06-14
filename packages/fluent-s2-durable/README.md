# @firegrid/fluent-s2-durable

**Spike / draft** — replay-based durable execution for Effect on top of
[S2](https://s2.dev)'s two concurrency primitives, with **no** non-S2 infra and
**no** dependency on `@effect/workflow`. Implements the design in
[`docs/sdds/s2-durable-sdd.md`](../../docs/sdds/s2-durable-sdd.md).

> Throwaway-quality but correct. The goal is to de-risk the hypothesis: a
> stream-per-execution journal, with `fencing_token` as the executor lease and
> `match_seq_num` as the exactly-once append guard, is sufficient for
> Restate/Inngest/CF-Workflows semantics at single-worker scale. See
> [`FINDINGS.md`](./FINDINGS.md) for what the spike learned (incl. one real bug it
> surfaced about lease monotonicity).

## What it does

- `ctx.run` — durable step, exactly-once journaled, replay short-circuits.
- `ctx.sleep` — durable timer (suspend + resume across restarts).
- `ctx.waitForEvent` — durable await, resolved from an external inbox.
- Crash-safe replay with loud divergence detection.
- Single-process host loop (`Dispatch` + `TimerHeap`) rebuilt by folding S2 on boot.
- Snapshot-and-follow to bound replay cost.
- Everything behind Effect `Layer`s (`S2`, `Dispatch`, `TimerHeap`) so the
  multi-worker version drops in later.

## Layout

| File | Role |
|------|------|
| `src/s2.ts` | `S2` service — the seam over the S2 TS SDK (§5.1) |
| `src/s2InMemory.ts` | `s2-lite` emulation: fencing token + `match_seq_num` + follow + trim |
| `src/record.ts` | Record taxonomy (§4.3) + byte codec |
| `src/journal.ts` | `fold` → `Journal` (`byOp`/`tail`/`seed`/`input`/`status`) |
| `src/determinism.ts` | Seed-sourced deterministic `Clock`/`Random` Layers (§5.4) |
| `src/context.ts` | `Ctx` (`run`/`sleep`/`waitForEvent`) + defect-based `Suspend` |
| `src/dispatch.ts` / `src/timerHeap.ts` | In-memory ready-set and durable-timer arming |
| `src/runtime.ts` | Host loop: lease → fold → reconcile → run → suspend/complete |
| `test/demo.ts` | The §8 order workflow + idempotent external services |
| `test/harness.ts` | §9 fault-injection (named kill-points, process-death sim) |
| `test/*.test.ts` | M0/M1 units + acceptance criteria AC-1…AC-6 |

## Status vs the SDD

All milestones M0–M6 implemented; acceptance tests AC-1…AC-6 pass against
`s2-lite`. The one open item is the hosted-basin parity run (§9) — the environment
had no basin access, so the `S2` service is exercised only against the in-memory
emulation. See `FINDINGS.md` §"Substrate caveat".

## Running

```sh
pnpm --filter @firegrid/fluent-s2-durable typecheck
pnpm --filter @firegrid/fluent-s2-durable test
```
