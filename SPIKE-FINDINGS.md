# SPIKE S0 — Findings: F# API ergonomics for `Firegrid.Durable`

Status: throwaway spike, branch `spike/s0-ergonomics`. Nothing here ships;
this document is the deliverable.

Probe target: the two samples in
[`docs/sdds/api-layering-sdd.md`](docs/sdds/api-layering-sdd.md) — the
`checkout` sample ("L2 — the public API") and the firegrid `drive` loop
("Worked example") — compiling and running against the real L1 kernel
(`src/Firegrid.Store/Foundation/Durable/`) under Node/Fable.

What was built (spike-quality, disposable):

- `src/Firegrid.Store/FiregridDurable.fs` — minimum `Firegrid.Durable` L2
  slice: `workflow { }` builder (guarded `Delay`/`Run`, `and!`, `match!`,
  `try/with`, `while`), zero-codec `Step.define` / `Signal.define<'t>` /
  `Workflow.define/declare` / `Worker.implement/run` / `Client.connect` /
  `Run<'o>` handles, `Rollover` (ContinueAsNew value), derived JSON codec.
- `src/Firegrid.Foundation.Proofs/SpikeS0.fs` — driver: in-memory Stepper
  drives (T1/T2/T3), s2-lite + child-process SIGKILL (T4), generation
  rollover loop (T5).
- `src/Firegrid.Foundation.Proofs/spike-fixtures/spike-s0-checkout-journal.golden.txt`
  — T3 golden journal bytes.

Run it: `pnpm run fable:build && pnpm --filter @firegrid/foundation-proofs build`
then `node dist/SpikeS0.js` from `src/Firegrid.Foundation.Proofs`
(s2 binary required at `~/.s2/bin/s2` or `S2_BIN`).

Result: **19/19 checks PASS** (representative output below per test).

---

## Verdicts at a glance

| Test | Verdict | One-liner |
| --- | --- | --- |
| T1 — CE feature set | **PASS** (with documented sample deltas) | Both SDD sample bodies compile as written and run to completion on the kernel replay path |
| T2 — bounded-loop flat stack | **PASS** | 500 and 2,000 iterations validated under Node/Fable; fresh replay of a 4,000-record journal in 74 ms; no stack growth. Kernel replay is O(n²)-per-plan — a perf finding, not a stack one |
| T3 — zero-codec typed descriptors | **PASS** (with a load-bearing finding) | No codec parameters anywhere; journal bytes golden-stable. BUT decode required a type-directed reflection walker — naive `JSON.parse` cannot feed Fable pattern matching |
| T4 — kill/replay through new surface | **PASS** | Real child-process SIGKILL after step 1 committed; restart host journal-serves step 1 (side-channel count stays 1); workflow completes correctly via the SDD call sample lines |
| T5 — ContinueAsNew probe | **PASS as prototype / kernel gap CONFIRMED** | Kernel has no rollover primitive. Fresh-instance-per-generation prototype above the kernel chains 3 generations with carried state and per-generation journals of exactly one step. Durable handoff needs ~200 lines of kernel |

**Final verdict — "the ratified ergonomics are buildable as specified":
YES, WITH THESE CHANGES** (see last section).

---

## T1 — CE feature set: PASS

Acceptance was "both SDD samples' bodies compile". They do, verbatim
(handlers' elided `(* … *)` bodies filled in; domain types defined app-side
per doctrine). Evidence — `SpikeS0.fs` `SpikeSamples` contains both samples
character-for-character in their orchestration bodies; run output:

```
[T1] PASS — checkout sample compiles and completes (signal-approved path) :: { Confirmed = true; Reference = res-ord-1 }
[T1] PASS — while + ref journals three sends; try/with catches continuation exception :: caught:kaboom
[T1] PASS — and! lowers to kernel PerformAll and joins both values :: performAll=true value=sim:left|sim:right
[T1] PASS — drive-loop sample compiles and completes via recursive return! :: { Summary = done:4 }
```

Builder shape that made it work (`FiregridDurable.fs`, `WorkflowBuilder`):
`Delay` returns the thunk (guarded), `Run` forces one layer,
`Combine`/`While`/`TryWith` consume thunks. `match!` needs no builder
support. `and!` (`MergeSources`) pattern-matches both sides: two `Perform`
nodes (step calls have pure decode continuations) rewrite into the kernel's
`PerformAll`; anything else falls back to sequential bind (deterministic,
not fanned out). `try/with` wraps continuations across suspension points so
exceptions in resumed pure code route to the handler.

### Exact deltas against the SDD samples

1. **`notify.Send` is journaled but NOT fire-and-forget.** The kernel's only
   call shape is `Perform` (call + await completion). Send maps to Perform
   with output discarded — the workflow still waits for the handler before
   the next bind. True one-way needs a kernel node (or lowering onto the
   Processor's `Intent.Send` path).
2. **`reg counter` / `Entity.define` not built** — entities were out of the
   spike's minimum slice (kernel `Processor`/Decider exists at L1; this is
   surface plumbing, not a semantic unknown).
3. **`client.Logs` / `client.Read` not built** — same reasoning
   (DurableLog/StateReads exist at L1).
4. **`Worker.run` returns a handle; the caller ticks it**
   (`runUntilIdle`/`runForever`). The SDD sample reads as if `Worker.run`
   also starts the loop — the SDD should say which.
5. **`turnDecl.CallChild` compiles and journals but cannot execute** — the
   kernel has no child-workflow primitive. See kernel gaps.
6. **`Timestamp` is `float` (epoch ms), not `int64`** — under Fable, int64
   is BigInt and `JSON.stringify` throws on BigInt, so int64 must not appear
   in payload types. Kernel-side timer deadlines remain int64 internally.
7. **`try/with` cannot observe step-handler failures** — the kernel journals
   no `ActivityFailed` record; a throwing handler fails the whole host tick
   (`ActivityCommandAdapterFailure.HandlerFailed`). The SDD's "typed step
   failure" corpus row requires a kernel record first.
8. `MergeSources3+` not defined: three-way `and!` nests pairs, so only the
   innermost pair fans out. Cosmetic; `Workflow.all` covers homogeneous
   fan-out.

## T2 — bounded-loop flat stack under Fable: PASS

`let rec drive` loop of `Step.call`s via `return! drive (n + 1)`, driven
through `DurableStepper.plan` to completion and then replayed fresh:

```
[T2] PASS — 500-iteration recursive loop … :: value=125250 plans=1001 history=1000 driveMs=2059
[T2] PASS — single fresh replay over the full journal … :: replayed=125250 replayMs=9
[T2] PASS — 2000-iteration recursive loop … :: value=2001000 plans=4001 history=4000 driveMs=101796
[T2] PASS — single fresh replay over the full journal … :: replayed=2001000 replayMs=74
```

**Max validated: 2,000 iterations (4,000 history records).** No stack
overflow anywhere; a single full replay is milliseconds — the guarded-Delay
+ iterative-Stepper claim in the SDD's loop-discipline paragraph holds.

**Perf finding (not stack):** the drive-to-completion pattern (re-plan after
every commit, which is what a real host does per tick) is O(n²) per plan —
`History.completed` is a linear list scan per op and `History.append` is
`events @ [event]` — so 2,000 iterations cost ~102 s of pure replay CPU.
Recommendation: index history by `OpId` (Map) and use a reversed/appendable
structure before the red corpus pins any timing; and the SDD's
"few hundred journal records replay cheaply" wording is accurate only for
journal *length*, so the bounded-loop law should pin record counts, not
wall-clock.

## T3 — zero-codec typed descriptors: PASS, with the spike's second-biggest finding

The checkout sample compiles with **zero codec parameters** and no type
annotations beyond `(order: Order)` / the handler input annotations that the
SDD itself shows. Journal bytes are stable:

```
[T3] PASS — journal bytes identical across two in-process runs :: 493 bytes
[T3] PASS — journal bytes stable against golden fixture :: matches committed golden fixture
[T3] PASS — DU payload round-trips through derived codec :: ["FollowUpAt",123.5,"later"]
[T3] PASS — DU with record-list payload round-trips :: ["Spawn",[{"Prompt":"a"},{"Prompt":"b"}]]
[T3] PASS — record payload round-trips through derived codec :: {"Accepted":true,"Approver":"human"}
```

Golden fixture (committed): `src/Firegrid.Foundation.Proofs/spike-fixtures/spike-s0-checkout-journal.golden.txt` — e.g. first line
`in|event.activity-called|1:014:orders/reserve19:["OrderId","ord-1"]`.

**Finding: "derived serialization" is buildable but is NOT free-of-mechanism.**

- **Encode is free**: Fable's own `toJSON` shapes are deterministic and
  name-tagged — records → `{ field: … }` in declaration order, DUs →
  `["CaseName", …fields]` (case-NAME-tagged: rename-sensitive, reorder-safe
  — good wire properties), lists → arrays. `JSON.stringify` is the derived
  encoding.
- **Decode is not**: `JSON.parse` yields plain JS data that Fable pattern
  matching cannot consume (unions are tag-numbered class instances; lists
  are cons cells). First run failed exactly here
  (`Cannot read properties of undefined (reading '0')`).
- Fix in the spike: a ~50-line type-directed decoder over Fable's
  `FSharp.Reflection` (records via `MakeRecord`, unions matched by case
  name, lists rebuilt), with `inline` capture at the `define` call sites so
  generic erasure never bites (descriptors store their decoders; interface
  and registry paths use the stored closures).
- What the SDD must decide: the derivation mechanism for the real surface —
  inline reflection walker (this spike), a `Thoth.Json.Auto`-style library,
  or compile-time codegen — plus a payload-type doctrine: **no `int64`/BigInt
  in payloads** (stringify throws), and coverage rules for `option`, `Map`,
  `Set`, `Result` in payload position (untested here; the samples don't need
  them).

## T4 — kill/replay through the new surface: PASS

Technique (per the kernel proof style, upgraded to a real process kill):
parent starts s2-lite and spawns a child Node process running the same
compiled module (`node dist/SpikeS0.js t4-child`). The child registers
`[ reg reserve; reg notify; reg checkout ]` via `Worker.run`, starts
instance `spike-checkout-1`, and ticks. Handlers bump side-channel count
files (paths passed by env). After step 1 (`orders/reserve`) has executed
and its completion journaled, the child writes a marker and keeps ticking;
the parent `SIGKILL`s it mid-flight. The parent then starts a **fresh host
(new fence)** and runs the SDD call sample literally:

```fsharp
let client = Client.connect basin
let! run = checkout.Start client order (Id "spike-checkout-1")
do! run.Signal approved { Accepted = true; Approver = "human" }
let! receipt = run.Result
```

```
[T4] PASS — step 1 executed exactly once in the killed child host :: reserve executions at kill: 1
[T4] PASS — after restart, step 1 is journal-served (never re-executed) :: reserve executions total: 1
[T4] PASS — notify executed exactly once across kill + restart :: notify executions total: 1
[T4] PASS — workflow completes correctly after kill + restart + signal :: { Confirmed = true; Reference = res-ord-1 }
```

Also exercised for real on this path: duplicate `Start` dedupe (both hosts
start the same instance; mailbox highwater dedupes), CurrentTime journaling,
signal-vs-48h-timer race, signal delivery to a parked race, loser-timer
cancellation records.

## T5 — ContinueAsNew probe: kernel gap CONFIRMED; smallest viable mechanism prototyped and green

**The kernel lacks ContinueAsNew, as suspected.** Precisely:

- `Durable<'a>` (`Semantics.fs`) has no rollover/terminal-with-payload node;
- `StepRecord` (`Stepper.fs`) has no terminal record — a journal's only
  terminal state is "replay returns `Done`";
- `DurableHost`/`Runtime`/`App` have no rollover loop; `InstanceId` has no
  generation dimension; `getStatus` can only say Completed.

**Prototype (surface + driver only, no kernel edits):** the workflow returns
a rollover *value* — `Rollover<'state,'o> = ContinueAsNew of 'state | Finish of 'o`
— and the driver, on observing `ContinueAsNew next`, starts generation N+1
as a **new instance** (`spike-eternal-g{N+1}`, i.e. a fresh S2 stream pair)
with the carried state. Fresh stream ⇒ prior journal is not replayed, by
construction. Worker discovery picks the new generation up automatically
(new `/in` stream).

```
[T5] PASS — rollover value chains three generations to completion with carried state :: total=3 generations=3
[T5] PASS — each generation journals exactly one step call (prior journal NOT replayed) :: [1; 1; 1]
[T5] PASS — step executed once per generation (fresh execution, not journal-served from gen 0) :: tick executions: 3
```

**What's missing for the real thing (estimated scope):**

1. **Durable handoff — the actual gap.** The spike's "observe Finish → start
   next gen" hop lives in the driver; a crash between generation completion
   and next-gen start loses the chain. Fix: a terminal StepRecord
   (`WorkflowContinuedAsNew of nextInput`) committed under the holder's
   fence, plus an adapter that dispatches a deduped `StartWorkflow` to the
   next generation's inbox. The kernel already owns exactly this machinery
   (`Outgoing Command` + `CommandDispatchCheckpoint` dedupe, as used by
   ActivityAdapter/TimerAdapter) — this is a new command case + one more
   adapter. ~150–250 lines incl. codec + host wiring.
2. **Identity/status:** generation-aware `InstanceId` (or a blessed naming
   convention) and `getStatus` reporting `ContinuedAsNew(next)` instead of
   `Completed`; `Run.Result` should follow the chain client-side. ~100 lines.
3. **Authoring surface:** either a dedicated `Workflow.defineEternal`
   (factory returns the rollover value, as in the spike) or a
   `Workflow.continueAsNew : 'i -> Durable<'o>` pseudo-op lowered to the
   terminal record. The *value-returning* shape worked well and keeps the
   free monad untouched — recommend it to the SDD.
4. Rejected alternative: same stream + generation marker with
   replay-from-marker — touches Stepper history slicing and collides with
   Checkpoint/trim work (A1/A2); fresh-stream-per-generation composes with
   what exists.

## Kernel gap inventory surfaced by the spike (for the T1 red corpus)

| Gap | Where it bites | Note |
| --- | --- | --- |
| No ContinueAsNew (node/record/host path) | T5, SDD loop discipline | Prototype proves shape; durable handoff needs kernel (above) |
| No child workflows | `turnDecl.CallChild`, `spawn_all` | Needs child-start intent + parent park on child-completion (mailbox-delivered, like activity completions) + `Workflow.all` over children |
| No typed step failure (`ActivityFailed`) | `try/with`, retry policies row | Handler throw currently fails the host tick wholesale |
| No fire-and-forget step | `Step.Send` | Send blocks until completion today |
| Replay data structures O(n²) | T2 at 2k iterations (~102 s drive CPU) | Index history by OpId; append-friendly history |
| Fence churn: `runUntilIdle` re-claims (appends a fence record) every tick | log noise | cosmetic today |

## Verdict: buildable as specified? — YES, with these changes to the SDD before its red corpus is written

1. **Specify the codec mechanism.** "Serialization derived from your types at
   compile time" is achievable with zero codec parameters, but decode needs a
   named mechanism (inline reflection walker / Thoth-auto / codegen) and a
   payload doctrine: no `int64`-BigInt in payloads, `Timestamp = float` (or a
   wrapped type with a pinned codec), and explicit corpus coverage for
   `option`/`Map`/`Set` payloads. The union wire shape `["CaseName", …]` is
   case-NAME-coupled — renames are wire-breaking; say so next to the
   "names land in journals" clause.
2. **ContinueAsNew**: adopt the value-returning shape
   (`Rollover`/`ContinueAsNew` terminal value) and add the kernel terminal
   record + deduped next-generation start dispatch; make generation part of
   instance identity. This is the one place the SDD promises something the
   kernel cannot yet make durable.
3. **`Step.Send`** semantics: either respecify as "journaled call, result
   ignored" (what's buildable today) or add the kernel one-way node.
4. **`Workflow.callChild`**: schedule the kernel child primitive; it is the
   largest missing semantic for the worked example (all five firegrid
   primitives except `spawn` are green on today's kernel).
5. **`Worker.run`**: specify whether it starts the tick loop.
6. **Typed step failure + DU retry policies** need an `ActivityFailed`
   journal record first; today failure is not part of workflow semantics.
7. Bounded-loop law: pin journal length (validated to 4k records), not
   wall-clock; schedule the history-indexing kernel cleanup.

Ergonomic wins worth keeping exactly as sampled: `let rec drive … return!`
recursion reads and replays perfectly; `and!` → fan-out is real; dot-member
descriptors (`step.Call`, `signal.Await`, `run.Signal/Result`) all inferred
with zero annotations; `Error Timeout` matching falls out of a plain DU.
