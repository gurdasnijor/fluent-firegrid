# SDD: API Layering — Target Surfaces per Layer

**The product is a durable-execution platform over S2 streams** — Durable-
Functions-semantics orchestrations (deterministic replay over journaled
histories, per Burckhardt et al., OOPSLA 2021), addressable entities, and
at-least-once activities, hosted Pulsar-Functions-style as functions attached
to streams. **Agent sessions are application #1 on that platform, not the
platform.** Every layering decision below follows from that sentence.

Doc-Class: sdd
Status: draft — architect-authored; **pending human ratification** (nothing in
the convergence plan dispatches until this document is ratified)
Date: 2026-07-07
Owner: Firegrid Architecture
Companion: [`fsharp-fable-effsharp-evaluation-sdd.md`](./fsharp-fable-effsharp-evaluation-sdd.md)
(F#-zone internal doctrine), [`managed-sessions-agent-ui-sdd.md`](./managed-sessions-agent-ui-sdd.md)
(capability semantics)

## Why this SDD exists — the 2026-07-07 audit

The managed-sessions wave shipped a proven F# session kernel (Authority,
DurableLog/Turn, SessionLifecycle, Checkpoint, StateReads, SessionHistory, the
wake path) with green proofs at every layer it defined. It defined no layer for
the consumer. Findings, verified on `main`:

- `Firegrid.Store/Exports.fs` — the single Fable-emitted seam every ratified
  surface claimed to "ride" — exports A3's five generic `StateReads`
  pass-throughs and pre-wave legacy (`ObjectState`, `WorkflowLog`). The session
  kernel's consumer API (`SessionLifecycle.start/cancel/drive`,
  `DurableLog`/`Turn` attach, `SessionHistory` reads) is unreachable from TS.
- The exported functions return Fable `Async`, not `Promise` — not directly
  consumable from idiomatic TS despite "Promise-first" claims in ratified
  surface text.
- `packages/log`, named by P4 as the "idiomatic TS facade," contains no source.
- Package naming leaks spec coordinates: `@firegrid/l1-vocabulary` names an
  interface id from an execution ledger, not a thing any consumer would search
  for or understand. Descriptions lead with "(MS-C6, I2 consumer)".
- The G6 Validation Gates ("ergonomic sample, emitted-TS sample") were
  satisfied wave-wide by *assertion* ("rides P4's seam, Fable-safe by
  construction") rather than by artifact. Five consecutive surface reviews —
  architect included — accepted the phrasing.
- **The platform itself is invisible.** The P3-ported kernel already contains
  a complete Durable-Functions-style engine: `Durable<'a>` (a free monad —
  program-as-data orchestrations with `Perform`/`PerformAll`/`Await`/
  `WhenAny`/`CurrentTime`), the `durable { }` computation expression
  (`Api.fs`), the Azure-DF-shaped `Workflow.call/all/waitForSignal/sleepUntil/
  any` vocabulary, and `Stepper`'s journaled `StepRecord` replay — proven by
  `FoundationDurableKernelProof.fs`. None of it is exported at any seam, named
  in any package, or presented in any SDD as the product. The wave built and
  reviewed *application* capabilities while the platform stayed anonymous
  plumbing — the deepest instance of the layering gap.
- The adapter contract (`@firegrid/harness-adapter`) and its Claude
  implementation carry a **hard `effect@4.0.0-beta.87` dependency in their
  exported types** (`Context.Service`, `Effect.Effect`, `Data.TaggedError`) —
  directly contradicting the ratified canon
  ([`language-and-targets.md`](../canon/architecture/fluent/language-and-targets.md)),
  which decided Promise-first public facades with Effect as an *optional thin
  wrapper* ("forcing Promise consumers through Effect embeds" the dependency).
  Provenance of the drift: the lanes doc's ground rules compressed the canon
  to "lanes D/E are TS (Effect shapes per LLMS.md)"; the repo `CLAUDE.md`
  instructs Effect idioms via `LLMS.md`; the D2 surface review verified
  against `LLMS.md` house style instead of the canon's layering decision.
  (`@firegrid/l1-vocabulary` is dependency-free — the one explicit "Effect-free
  data types" ruling held.)

Root cause: **no contract stated what each layer's outward surface must be.**
G6 checked internal shape and laws; the evaluation SDD checked F# style; the
proofs checked semantics. Nothing checked *reachability by the intended
consumer* — the exact failure mode the Proof-Driven Development doctrine names
("a production module must be usable by a consumer who has never read its
proofs") applied one altitude up.

## The layer model

Every package and module in this repo belongs to exactly one layer. Every
capability WP declares its layer. The binding rule at the bottom of this
section is what prevents the audit findings from recurring.

| Layer | Name | Audience | Doctrine | Current state |
| --- | --- | --- | --- | --- |
| L0 | Substrate (S2 streams, basins, fencing) | Platform internals only | s2-substrate canon | Good |
| L1p | **Platform kernel** (domain-free F#): orchestrations (`Durable<'a>`, `durable { }`, `Workflow.*`, `Stepper` replay), entities (Processor/Mailbox/Handler), activities, durable timers + wake path, Authority, DurableLog, Checkpoint, StateReads/projections | Platform developers, proofs, L1a | Evaluation SDD (F# API doctrine) | Good — proven; **anonymous and unexposed** |
| L1a | **Applications in F#** (domain modules over L1p): today, *sessions* (`Turn`, `SessionLifecycle`, `SessionHistory`) | Application developers, proofs | Same F# doctrine; **never inside platform namespaces** | Good code, wrong placement (lives inside `Firegrid.Store` beside the kernel) |
| L2 | Emitted seam (Fable emission + `Exports.fs`) | **Exactly one consumer: L3.** Not a public API. | Complete and mechanical: everything L3 needs, nothing more; `Async` is acceptable here | **Nearly empty — the gap** |
| L3 | **The platform facade, singular: `@firegrid/durable`** — activities, orchestrations, entities, durable timers/signals, plus the stream-native primitives this platform legitimately owns (sealed durable logs with attach, checkpointed projections/reads) | Any TS developer | This SDD: Promise-first, human names, quickstart-able, Effect-free | **Missing — the engine exists at L1p unexposed** |
| L4 | Applications & adapters: **agent sessions (a reference application over L3, not a platform package)**, `claude-adapter`, agent-ui | App developers; harness implementors; end users | Platform ships zero domain nouns (the Restate/DF/Pulsar precedent); apps own their vocabulary | Adapter good; sessions currently mis-homed as kernel-resident F#; app blocked on L3 |

**Binding rules (in force on ratification):**

1. Every capability WP names its layer in the ledger row.
2. An L1 (kernel or application) WP is not *wave-complete* until its L3
   exposure ships or a ledger row explicitly defers it. "Done" on a kernel row
   means kernel-done; the wave tracks consumer reachability as its own
   deliverable.
3. L2 exists to serve L3 and is never documented, named, or linked as a public
   API. Consumers who import from `dist/` are off-contract.
4. Spec coordinates (I2, MS-C6, L1-as-in-vocabulary, WP ids) never appear in
   package names, in the first sentence of package descriptions, or in any
   L3/L4 exported identifier. They belong in docs and code comments.
5. **Placement follows the platform/application split.** Domain modules never
   live in platform namespaces or directories; a new domain (e.g. a second
   application) arrives as its own L1a module space over L1p's public
   surfaces, exactly as sessions bind Authority/DurableLog/Checkpoint today.
   Platform modules never import application modules.

## Naming doctrine and the rename table

A package name answers "what do I do with this?" in the consumer's vocabulary,
not "which spec section ratified it?".

| Current | Target | Rationale |
| --- | --- | --- |
| `@firegrid/l1-vocabulary` | `@firegrid/session-events` | It is the typed vocabulary of session update events (an ACP superset) a UI decodes and folds. "L1" and "vocabulary" are ledger-speak. |
| *(none)* | `@firegrid/durable` | **The platform facade — the only `@firegrid/*` L3 package.** Activities, orchestrations, entities, timers/signals, durable-log attach, projections over S2. The engine exists at L1p; the package does not. |
| *(planned `@firegrid/sessions` — cancelled)* | agent-sessions **reference application** | The platform ships no domain nouns (Restate has no "session"; neither do we). Sessions are the reference app built *on* `@firegrid/durable`; its scenarios double as the platform's integration acceptance corpus. Lives app-side (examples/ or with agent-ui), never as a platform package. |
| `@firegrid/harness-adapter` | *(keep name)* | Audience is harness implementors; the name is already in their vocabulary. Description rewritten consumer-first (spec refs move to the end). |
| `@firegrid/claude-adapter` | *(keep name)* | Same. |
| `@firegrid/log` (empty stub) | *(absorbed)* | Delete the stub; the store seam supersedes P4's intent. If a standalone log facade is ever needed it gets its own surface stop. |

Descriptions for every `@firegrid/*` package are rewritten to lead with the
consumer sentence; spec coordinates may appear parenthetically at the end.

## Dependency doctrine (restates canon; now binding at every layer)

The kernel is F#/Fable-native; the consumer surface is plain TypeScript.
Concretely:

1. **Exported surfaces at L3/L4 are Effect-free**: `Promise`, `AsyncIterable`,
   plain interfaces, tagged-union errors (`{ _tag: … }` plain objects). No
   `effect` (or other framework) types in any exported signature, and no hard
   `effect` dependency in any contract or facade package.
2. **Effect is an optional side wrapper, never the substrate** — exactly the
   canon's `@firegrid/log/effect` pattern: a thin, separate entry point that
   wraps the plain facade for consumers who want Effect composition. Wrapping
   Promise in Effect is trivial; the reverse embeds the dependency.
3. The legacy Effect-first packages (`fluent`, `trace`, `core`) ride the
   frozen TanStack path and are exempt until their own migration SDD.
4. `LLMS.md` guidance is rescoped to those legacy packages; `CLAUDE.md`'s
   pointer is amended accordingly (S6).

## Target surfaces

### L2 — seam completeness (`Exports.fs`)

The seam exports what the **platform facade** needs, as mechanical
pass-throughs (Fable `Async` is fine here; L3 converts), demand-driven by
greening the platform corpus:

- Orchestrations: `Durable`/`durable { }` program values, `Workflow.*`,
  `Stepper` drive/replay, host/registry entry points, signal raising.
- Entities: Processor/Mailbox drive + admission (the virtual-object regime).
- Activities: registration + completion plumbing.
- `DurableLog` (domain-free): create/append/seal/attach — the byte-faithful
  sealed-log path (chunks then terminal). `Turn` is application vocabulary
  and is NOT a seam mandate.
- `Checkpoint` (`latest`, `resumeFrom`) and `StateReads` (existing exports
  stay) — the projection/read primitives.
- Application modules (`SessionLifecycle`, `Turn`, `SessionHistory`) are
  exported only if and when the reference application demands them during
  greening — never as platform API.
- The wake path is **not** exported: platform plumbing. Posting wakes from TS
  is off-contract until a consumer demands it (surface stop then).

### L3 — `@firegrid/durable` (the platform facade; **G6 surface stop**)

Promise-first, tagged-union errors, `AsyncIterable` for streams, zero S2 or
codec assembly required of the consumer. Its target shape is specified by the
platform SDD + red corpus (T1); its scope is the DF trio **plus** the
stream-native primitives this platform owns: define/register activities,
author/start/signal/query orchestrations (`ctx.call/all/waitForSignal/
sleepUntil/any/currentTime` mirroring the F# `durable { }` CE), keyed entity
operations, durable-log create/append/seal/**attach**, and checkpointed
projections/reads. Error doctrine: every kernel DU error surfaces as a tagged
union (`{ _tag: "Deposed" } | …`), never thrown strings; genuinely
exceptional transport failures may throw.

### L4 — agent sessions: the reference application (not a platform package)

The platform ships **zero domain nouns** — Restate has no first-class
"session" and neither do we. Sessions are the reference application built on
`@firegrid/durable`, owned app-side (an `examples/` app in this repo, later
migrating toward agent-ui's home). Indicatively, in platform vocabulary:

```ts
import { DurableClient } from "@firegrid/durable"
// Application code below — "session"/"turn" are THIS APP's words, not the platform's.

const client = await DurableClient.connect({ basin })
const session = client.entity("session", "sess-123")      // keyed entity (virtual object)

await session.send("startTurn", { turnId: "turn-1", holder: "api-pod-7" })
await session.send("cancel", { turnId: "turn-1" })         // cancel = just a command

// Turn output is a sealed durable log; attach is a PLATFORM primitive:
for await (const ev of client.logs.attach(["sessions", "sess-123", "turns", "turn-1"])) {
  // ev: { kind: "chunk", … } | { kind: "terminal", … }
}

const history = await client.projections.read(historyProjection, { read: "eventual" })
```

Consequences:

- **The E1 dry-run scenarios become the platform's integration acceptance
  corpus** (T2): they are written against `@firegrid/durable`'s public
  surface plus thin app code. Anywhere greening them requires reaching
  *around* the platform, that is a platform gap → a new platform WP. The
  reference app is how the platform proves itself.
- The wave's F# session modules (`Turn`, `SessionLifecycle`,
  `SessionHistory`) are re-labeled L1a **application code that predates the
  platform surface** — kept working for continuity, progressively
  re-expressed over the platform facade as the loop demands, never exported
  as platform API.
- D-lane packages (`session-events` vocabulary, harness adapters) are
  application-ecosystem packages (L4) and keep their plan unchanged.

### L3 — `@firegrid/session-events` (rename of `l1-vocabulary`)

Content unchanged (the schema is ratified I2); the rename is mechanical:
package name, imports in `harness-adapter`/`claude-adapter`/proofs, docs
references. The decision-record page gains one line noting the rename.

### L4 — adapters

No shape changes. Descriptions rewritten per the naming doctrine.

## Validation gates — amended enforcement (supersedes assertion-based passes)

The evaluation SDD's gates stand; their *evidence* requirement changes:

1. **Emitted-TS sample = artifact.** Every F#-zone surface or impl PR ships a
   compiled TS sample under `samples/`, type-checked in CI. The sentence
   "Fable-safe by construction" no longer satisfies the gate. (Applied
   retroactively by S4 below for the wave's existing kernel modules — the
   facade's samples cover them.)
2. **Consumer test at L3.** A capability is consumer-reachable when its L3
   quickstart sample runs in CI. The ledger tracks this per the binding rules.
3. **Architect enforcement.** Surface reviews check the artifact exists in the
   PR file list, not that the text claims it.

## Execution model: top-down red/green (supersedes prose-first surfaces for S-lane)

Ratified 2026-07-07 direction from the human: define the API layers **as code
plus failing tests**, then converge implementation — not bottom-up kernel
work hoping surfaces emerge. Mechanics:

1. **A target surface is ratified as three artifacts together**: a surface
   package (real exported signatures; bodies `throw NotImplemented("<WP>")`),
   a **red corpus** (consumer tests against those exports — compiling,
   running, failing), and a short prose companion for laws code cannot
   express. The dependency doctrine makes this cheap: plain-TS facades have
   self-contained types, so surfaces + tests compile before any wiring
   exists.
2. **The ratchet.** A manifest (`targets.json`: test → WP → `red`|`green`)
   drives a target suite beside the blocking suite. Strict semantics: CI
   fails on a green regression **or on a red test unexpectedly passing** —
   an xpass forces an explicit promotion commit (manifest + ledger flip).
3. **Definition of done**: a WP's tests move from manifest-red to the
   blocking suite **with zero edits to test bodies**. Red tests are frozen at
   ratification; editing one is a gate (G5 analog). This is the structural
   fix for the proof-driven-development failure mode.
4. **Demand-driven layers.** L2 seam exports, kernel adjustments, and module
   re-placement (S9) are built only as green-making demands them — no
   speculative surface area.
5. **Roles.** The architect authors surface packages + red corpora (T-rows
   below); **the human ratifies each corpus before merge**; workers take
   green-making WPs via the coordinator; coordinator merge authority =
   "reds flipped green, zero test edits, manifest + ledger in the same PR."

| T-WP | Deliverable | Absorbs | Gate |
| --- | --- | --- | --- |
| T0 | Ratchet infrastructure: manifest runner, strict-xpass target suite in CI | — | None (mechanical) |
| T1 | **`@firegrid/durable` surface package + red corpus + platform SDD** (DF acceptance laws through the facade: replay determinism across host kill, fan-out/fan-in, WhenAny, signals, durable timers across restart, entity serialization, log attach, projections) | S8 | **Human ratifies corpus + SDD** |
| T2 | **Agent-sessions reference app red corpus, written against `@firegrid/durable`** (the E1 scenarios: start/cancel-from-second-process/attach byte-faithful/terminals-with-cause/history-with-lag). Greening gaps found here become platform WPs — this corpus is the platform's integration acceptance. | S3, S4 (recast app-side) | **Human ratifies corpus** |
| T3 | Plain-TS adapter contract surface + red fixture-replay conformance corpus | S7 | **Human ratifies corpus** |

The platform corpus (T1) is authored and ratified **first**; the sessions
scenarios (T2) are then written in its vocabulary. This costs the E-lane path
one design cycle relative to sessions-first and is the deliberate trade: the
platform surface is the load-bearing contract, and its first application must
consume it, not bypass it.

S2 (seam), S5 (renames), S6 (doctrine edits), S9 (re-placement) execute as
demanded by or alongside the T-rows. Green-making WPs are cut per corpus once
ratified.

## Convergence plan (ledger rows on ratification — Lane S, "Consumer Surface")

| WP | Deliverable | Depends | Gate |
| --- | --- | --- | --- |
| S1 | This SDD ratified by the human | — | **Human** |
| S2 | L2 seam completion (`Exports.fs` per target list) + Fable build green | S1 | None (mechanical; shapes are ratified L1 surfaces) |
| S3 | `@firegrid/sessions` Target Surface (TS zone) | S1 | **G6 surface stop — architect** |
| S4 | Facade implementation + E1 dry-run sample executing in CI | S2, S3 | None (impl vs ratified surface) |
| S5 | Rename `l1-vocabulary` → `session-events`; delete `log` stub; rewrite all package descriptions | S1 | None (mechanical) |
| S6 | Gate-enforcement amendment + dependency doctrine folded into lanes doc, dispatch pack, and `CLAUDE.md` (rescope `LLMS.md` to legacy packages); layer column added to ledger | S1 | None (docs) |
| S7 | De-Effect the D2 adapter contract: exported types become plain TS (interfaces, `Promise`, tagged unions); `effect` dropped from `harness-adapter`/`claude-adapter` deps; D3 impl follows; optional `/effect` wrapper only if a consumer asks | S1 | **G1 — architect-driven surface amendment** (contract shape change) |
| S8 | **Platform SDD + `@firegrid/durable` facade**: semantics documented against the DF model (deterministic replay, journaled history, entity serialization); L2 exports for `Durable`/`Workflow`/`Stepper`/host; plain-TS SDK (define activities, author/start/signal orchestrations, entity ops); `durable { }` CE documented as the F#-native authoring surface; parity checklist vs frozen `@firegrid/fluent` | S1 | **Architect-authored SDD → human ratification → G6 facade surface stop** |
| S9 | Module re-placement per binding rule 5: `Turn`/`SessionLifecycle`/`SessionHistory` move out of platform namespaces into a sessions module space (mechanical F# move; after in-flight kernel work lands to avoid churn) | S1; in-flight L1 work merged | None (mechanical; no shape change) |

Sequencing: S2, S3, S5, S6 dispatch in parallel after S1; S4 follows S2+S3;
S7 runs in the D lane in parallel (its shape amendment is architect-authored
in the surface, worker-implemented). S8's SDD authoring starts immediately
after S1 (architect work, no worker dispatch); its facade follows the sessions
facade unless the human re-prioritizes. S9 batches after the current kernel
wave merges.
In-flight kernel work (A4 impl, C2, B4 impl) is unaffected and continues — it
is L1 work and remains correct at its own altitude; S-lane is where the wave's
missing top layer gets built.

## The platform and its applications

The platform is not a future direction — **it is already implemented and
proven at L1p**; what is missing is its name, its seam exports, and its
facade. The corrected picture:

```
        L4   agent-sessions reference app ─── agent-ui      other apps
             (app code: "session"/"turn" are its words)        │
                 │                                             │
        L3   @firegrid/durable — THE platform facade, singular:
             activities · orchestrations · entities · timers/signals
             · durable-log attach · checkpointed projections
                 │
        L2   emitted seam (Exports.fs)
                 │
        L1a  applications (F#): sessions (Turn, SessionLifecycle,
             SessionHistory) — first domain expressed on the platform
                 │
        L1p  PLATFORM KERNEL (F#, domain-free):
              orchestrations: Durable<'a> free monad, durable { } CE,
                Workflow.call/all/waitForSignal/sleepUntil/any,
                Stepper journaled-replay (DF semantics, OOPSLA 2021)
              entities: Processor / Mailbox / Handler (single-writer admission)
              activities: Execute intents + ActivityAdapter
              time & liveness: durable timers, wake shards/router
              storage primitives: Authority, DurableLog, Checkpoint,
                StateReads / projections
                 │
        L0   S2 streams

  [frozen, parallel]:  @firegrid/fluent → vendored TanStack runtime
                       (same authoring vocabulary, old substrate; retired
                        when @firegrid/durable reaches parity)
```

Sessions demonstrate the intended pattern for *every* application: session =
entity, turn = sealed log, cancel = mailbox send, timeout = durable timer —
domain policy binding platform primitives, adding no new authority or drive
loop. The frozen `@firegrid/fluent`/TanStack stack is the platform's
predecessor wearing the same authoring vocabulary (`Workflow.call` appears in
both); it retires when `@firegrid/durable` reaches parity, which is S8's
design cycle — semantics documented against the Durable Functions model
(deterministic replay, journaled histories) with the F# `durable { }` CE as
the F#-native authoring surface and a plain-TS SDK over the seam. Sessions'
facade ships first only because the wave's paying consumer (agent-ui) needs
it; that is sequencing, not primacy.

## Non-goals
- UI components, React hooks, or anything above L4.
- Multi-language facades (TS only; other targets are future surface stops).
- Resharding, retention, and other kernel-altitude deferrals recorded
  elsewhere.
