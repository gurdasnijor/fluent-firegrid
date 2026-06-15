# @firegrid/fluent-s2-durable

**Spike / draft** — replay-based durable execution for Effect on top of
[S2](https://s2.dev), with **no** non-S2 infra and **no** dependency on
`@effect/workflow`. Implements the design in
[`docs/sdds/s2-durable-sdd.md`](../../docs/sdds/s2-durable-sdd.md), validated
against a **real `s2 lite` server** (not an emulation).

This is the Effect-native durable-primitive substrate for a
[`restate-sdk-gen`](https://github.com/restatedev/sdk-typescript/tree/main/packages/libs/restate-sdk-gen)-style
user API. In Restate terms: **S2 is our Bifrost**, `journal.ts` is the
**StateMachine**, records are **entries**, the fence is **leadership**, and
`Dispatch` is the **Invoker**. The structured-concurrency layer (`all`/`race`/
`spawn`) is left to Effect itself; this package only provides the journal-backed
primitives as plain `Effect`s. See [`FINDINGS.md`](./FINDINGS.md).

## What it does

- `ctx.run` — durable step, exactly-once journaled, replay short-circuits.
- `ctx.sleep` — durable timer (suspend + resume across restarts).
- `ctx.awakeable` — durable await, resolved from an external inbox.
- `ctx.state` / `ctx.call` / `ctx.send` — typed seams (SDD non-goals; for the API layer).
- Crash-safe replay with loud divergence detection.
- **Name-keyed entries** (not positional), so durable ops compose under Effect
  concurrency — this is what makes the user-facing combinator API buildable on top.
- Single-process host loop (`Dispatch` + `TimerHeap`) rebuilt by folding S2 on boot.
- Snapshot-and-follow to bound replay cost.
- Everything behind Effect `Layer`s (`S2`, `Dispatch`, `TimerHeap`).

## Layout

| File | Role (Restate analog) |
|------|------|
| `src/s2.ts` | `S2` service interface — the log seam (**Bifrost**) |
| `src/s2Live.ts` | real S2 TS SDK wrapper → `s2 lite` / hosted basin |
| `src/record.ts` | Schema-modeled entries + `Schema.fromJsonString` codec (**commands**) |
| `src/journal.ts` | `fold` → `Journal` (`byName`/`seed`/`completed`) (**StateMachine**) |
| `src/determinism.ts` | seed-sourced deterministic `Clock`/`Random` Layers |
| `src/context.ts` | `Ctx` (`run`/`sleep`/`awakeable` + seams) + defect-based `Suspend` |
| `src/dispatch.ts` / `src/timerHeap.ts` | in-memory ready-set (**Invoker**) + durable-timer arming |
| `src/runtime.ts` | host loop: lease → fold → reconcile → run → suspend/complete |
| `test/s2lite.ts` | boots a real `s2 lite` server as a Scope-managed `Layer` |
| `test/harness.ts` | §9 fault-injection (kill-points, crash = `Fiber.interrupt`) |
| `test/*.test.ts` | M0/M1 units + acceptance AC-1…AC-6 (against real s2-lite) |

## Status vs the SDD

All milestones M0–M6 implemented; AC-1…AC-6 pass against a real `s2 lite` server.
Open item: the hosted-basin parity run (`S2` is the seam). SDD non-goals
(multi-worker, `ctx.call`/`send`, VO state, framed snapshots, GC) are typed seams
or omitted; the `Dispatch`/`TimerHeap`/`S2` Layer seams keep them droppable-in.

## Running

Requires the `s2` CLI on `PATH` (`brew install s2` / the s2-lite binary). Tests
spawn their own server.

```sh
pnpm --filter @firegrid/fluent-s2-durable typecheck
pnpm --filter @firegrid/fluent-s2-durable test
```
