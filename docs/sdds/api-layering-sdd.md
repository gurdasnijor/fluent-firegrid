# SDD: API Layering — Target Surfaces per Layer

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
| L0 | Substrate (S2 streams, basins, fencing) | Kernel internals only | s2-substrate canon | Good |
| L1 | Kernel (F# `src/`: `Firegrid.Store`, `Firegrid.Foundation`) | F#-zone developers, proofs | Evaluation SDD (F# API doctrine) | Good — proven, ergonomic *at F# altitude* |
| L2 | Emitted seam (Fable emission + `Exports.fs`) | **Exactly one consumer: L3.** Not a public API. | Complete and mechanical: everything L3 needs, nothing more; `Async` is acceptable here | **Nearly empty — the gap** |
| L3 | Consumer facade (TS packages a developer imports) | agent-ui developers; external TS consumers | This SDD (below): Promise-first, human names, quickstart-able | **Missing for sessions; misnamed for events** |
| L4 | Adapters & applications (`claude-adapter`, agent-ui) | Harness implementors; end users | D2 adapter contract; app SDDs | Adapter good; app blocked on L3 |

**Binding rules (in force on ratification):**

1. Every capability WP names its layer in the ledger row.
2. An L1 (kernel) WP is not *wave-complete* until its L3 exposure ships or a
   ledger row explicitly defers it. "Done" on a kernel row means kernel-done;
   the wave tracks consumer reachability as its own deliverable.
3. L2 exists to serve L3 and is never documented, named, or linked as a public
   API. Consumers who import from `dist/` are off-contract.
4. Spec coordinates (I2, MS-C6, L1-as-in-vocabulary, WP ids) never appear in
   package names, in the first sentence of package descriptions, or in any
   L3/L4 exported identifier. They belong in docs and code comments.

## Naming doctrine and the rename table

A package name answers "what do I do with this?" in the consumer's vocabulary,
not "which spec section ratified it?".

| Current | Target | Rationale |
| --- | --- | --- |
| `@firegrid/l1-vocabulary` | `@firegrid/session-events` | It is the typed vocabulary of session update events (an ACP superset) a UI decodes and folds. "L1" and "vocabulary" are ledger-speak. |
| *(none)* | `@firegrid/sessions` | The consumer entry point for the session kernel. Does not exist today. Defined below. |
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

The seam must export, as mechanical pass-throughs (Fable `Async` is fine here;
L3 converts):

- `SessionLifecycle`: `start`, `append`, `complete`, `cancel`, `drive`,
  `logSubject`, `inboxSubject`, the `Timeouts`/`EndCause`/`Progress` types.
- `DurableLog`/`Turn`: `create`/attach/read cursors with `Turn.codec` and
  `Turn.address` — the byte-faithful attach path (chunks then terminal).
- `SessionHistory` (when A4 impl lands): `Turns.make`, `rebuild`,
  `checkpoint`, `startReader`, `readEventual`/`readLatest`.
- `Checkpoint`: `latest`, `resumeFrom` (already consumed internally; exported
  for L3's cold-start path).
- Existing `StateReads` exports stay.
- The wake path is **not** exported: it is kernel plumbing (L1), not consumer
  API. Posting wakes from TS is off-contract until a consumer demands it
  (surface stop then).

### L3 — `@firegrid/sessions` (the consumer entry point; **G6 surface stop**)

Promise-first, tagged-union errors, `AsyncIterable` for streams, zero S2 or
codec assembly required of the consumer. Target shape (signatures indicative;
the surface stop refines them):

```ts
import { FiregridClient } from "@firegrid/sessions"

const client = await FiregridClient.connect({ basin })       // one connect call
const session = client.session("sess-123")                   // address, not I/O

// Single-writer start (idempotent per holder; AlreadyLive is a typed error).
const turn = await session.startTurn("turn-1", {
  holder: "api-pod-7",
  timeouts: { idleMs: 60_000, maxMs: 900_000 },
})
await turn.append({ text: "…" })                              // Deposed is a typed error
await turn.complete()

// Durable cancel from ANY process — no authority required.
await session.cancel("turn-1", { source: "ui", seq: 1 })

// Attach from anywhere: byte-identical prefix, live tail, then terminal.
for await (const event of session.attach("turn-1")) {
  // event: { kind: "chunk", … } | { kind: "terminal", terminal, cause? }
}

// History / thread index (A4 projection; strong or eventual).
const history = await session.history({ read: "eventual" })   // lag exposed as data
```

Error doctrine: every kernel DU error surfaces as a tagged union
(`{ _tag: "Deposed" } | { _tag: "AlreadyLive"; turnId } | …`), never thrown
strings; genuinely exceptional transport failures may throw.

**Definition of done for the facade includes the E1 dry-run sample:** the
agent-ui attach path — start → append → cancel from a second "process" →
attach observes chunks then a durable `cancelled` terminal — written in ≤ 30
lines of consumer TS in a version-controlled `samples/` directory, compiled,
type-checked, and *executed* in CI. This sample is the consumer test made
artifact.

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

Sequencing: S2, S3, S5, S6 dispatch in parallel after S1; S4 follows S2+S3;
S7 runs in the D lane in parallel (its shape amendment is architect-authored
in the surface, worker-implemented).
In-flight kernel work (A4 impl, C2, B4 impl) is unaffected and continues — it
is L1 work and remains correct at its own altitude; S-lane is where the wave's
missing top layer gets built.

## Relationship to the workflow stack (why workflow authoring is deferred, not dropped)

The L1 kernel serves **two consumer branches**, of which this SDD specifies
one:

```
        L4   agent-ui (E-lane)              other apps / services
                 │                                 │
        L3   @firegrid/sessions             workflow-authoring API
             (this SDD: S3/S4)              (Restate-style; FUTURE SDD)
                 │                                 │
        L2   emitted seam (Exports.fs) ────────────┘
                 │
        L1   F# kernel
              ├─ session capabilities (Authority, DurableLog/Turn,
              │   SessionLifecycle, Checkpoint, StateReads, SessionHistory, wakes)
              └─ durable actor kernel (P3 port: Processor, Mailbox, timers,
                  Send/Execute intents — the workflow-engine core)
                 │
        L0   S2 streams

  [frozen, parallel]:  @firegrid/fluent → vendored TanStack runtime
```

The P3-ported durable kernel is deliberately the broker machinery a
Restate-style workflow SDK needs; managed sessions are the *first domain*
expressed on it (session = actor, turn = sealed log, cancel = mailbox send,
timeout = durable timer). A workflow is the *second domain* on the same
kernel — a handler actor whose steps are journaled `Execute` intents. The
endgame is a future SDD that re-grounds the Restate-style authoring
ergonomics on this kernel and retires the frozen TanStack lowering;
`@firegrid/fluent` rides TanStack today only because that re-pointing must be
its own surface-stop design (authoring CE-vs-SDK choice, replay-determinism
rules, versioning), not a side effect of this one. That future SDD inherits
this document's layer model, dependency doctrine, and artifact gates.
Sessions ship first because the wave's paying consumer (agent-ui / E-lane)
needs sessions.

## Non-goals
- UI components, React hooks, or anything above L4.
- Multi-language facades (TS only; other targets are future surface stops).
- Resharding, retention, and other kernel-altitude deferrals recorded
  elsewhere.
