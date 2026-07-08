# SDD: The Firegrid Platform — API Layers & Target Surfaces

Doc-Class: sdd
Status: draft — architect-authored; **pending human ratification** (nothing
in the execution plan dispatches until ratified)
Date: 2026-07-07
Owner: Firegrid Architecture
Companions: [`fsharp-fable-effsharp-evaluation-sdd.md`](./fsharp-fable-effsharp-evaluation-sdd.md)
(F#-zone internal style), [`managed-sessions-agent-ui-sdd.md`](./managed-sessions-agent-ui-sdd.md)
(kernel capability semantics — now an application-history document)

## The product

**Firegrid is a durable-execution platform over S2 streams.** Semantics
follow the Durable Functions model (Burckhardt et al., OOPSLA 2021):
orchestrations as deterministic programs replayed over journaled histories;
addressable single-writer **entities**; at-least-once **activities** with
journal-served results; durable timers and signals. Hosting follows the
Apache Pulsar Functions spirit: functions attached to streams, run by
namespace-scoped host workers. Being stream-native, the platform also offers
what those systems don't: **sealed durable logs with byte-faithful attach**
(replay prefix → live tail → terminal) and **checkpointed projections**.

**The platform ships zero domain nouns.** There is no first-class "session",
"agent", or "turn" — exactly as Restate has no "session". Agent sessions are
the platform's first *application*, and their vocabulary belongs to that
application.

**The primary interface is an F#/Fable library** — `Firegrid.Durable`, the
curated public API over the kernel, authored and consumed in idiomatic F#
(the `workflow { }` CE is the orchestration surface). TypeScript is a
**future cross-compilation target**: every L3 module stays Fable-green so a
`@firegrid/durable` emission + thin plain-TS wrapper can ship when a TS
consumer is prioritized — a build decision then, not a rewrite.

## End state

```
 L4  APPLICATIONS & ADAPTERS — their own vocabulary, zero platform nouns
       agent-sessions reference app (F#) · harness adapters (TS)
       · agent-ui (TS — consumes the future emission)
        │
 L3  Firegrid.Durable — THE platform library (F#/Fable, public API)
       activities · orchestrations (workflow { }) · entities
       · timers/signals · log attach · projections · table state & waits
        │
        ├─ FUTURE: Fable emission seam → @firegrid/durable (plain-TS wrapper)
        │
 L1  platform kernel (F#, domain-free, sans-IO, proven; internal)
       Durable<'a> · Workflow.* · Stepper replay · Processor/Mailbox
       entities · activities · timers · wakes · Authority · DurableLog
       · Checkpoint · StateReads
        │
 L0  S2 streams
```

| Layer | What lives there | Audience | Rule |
| --- | --- | --- | --- |
| L4 | Applications (reference app, agent-ui) and harness adapters | App developers, end users | Own vocabulary; consume L3 only |
| L3 | `Firegrid.Durable` — the F#/Fable platform library | F#/Fable developers (any TS developer, once the emission ships) | Idiomatic F#; Fable-green always; the platform's one public API |
| L2 | Emission seam (Fable → `Exports.fs` → future `@firegrid/durable`) | **Dormant** until a TS consumer is prioritized | Private, mechanical; importing `dist/` is off-contract |
| L1 | F# platform kernel | Platform developers, proofs | Domain-free; internal — consumers use L3; never imports applications |
| L0 | S2 | L1 only | Never leaks upward |

## Target surfaces

### L3 — `Firegrid.Durable` (the platform library)

Idiomatic F# per the evaluation SDD: `Async` + `Result` + DU-typed errors,
zero S2 assembly required of the consumer, EffSharp-free, and **Fable-green
as a standing build gate** (the TS option must never rot). The authoring
surface is the `workflow { }` CE over the kernel's program-as-data monad
(the kernel's `durable { }` builder re-exported; the old name kept as a
deprecated alias). Indicative shape (T1's corpus refines and freezes it):

```fsharp
open Firegrid.Durable   // the platform's one public API

// Durable operations: TYPED DESCRIPTORS, defined once as values.
// Names are two-segment ("service/handler") from day one — names land in
// journals and instance addresses, so the scheme freezes before the first
// record is written. Codecs are explicit (reflection JSON is Fable-unsafe).
let reserve : Step<OrderId, Reservation> =
    Step.defineAsync "orders/reserve" Codec.orderId Codec.reservation reserveAsync
let notify : Step<OrderId, unit> =
    Step.defineAsync "orders/notify" Codec.orderId Codec.unit notifyAsync
let approved : Signal<Decision> =
    Signal.define "orders/approved" Codec.decision

// Orchestrations: workflow {} — let! sequences, and! fans out, no arbitrary
// task binds. External effects go through Step.call; deterministic local
// work through Workflow.local; waits time out as Results, not exceptions.
let checkout =
    Workflow.define "orders/checkout" Codec.order Codec.receipt (fun order -> workflow {
        let! reservation = Step.call reserve order.Id
        and! ()          = Step.call notify order.Id            // independent → fan-out
        match! Wait.signal approved |> Wait.timeout (Duration.hours 48) with
        | Ok d when d.Accepted -> return Receipt.confirmed reservation
        | _                    -> return Receipt.rejected order.Id })

// Entities: the Decider shape — initial / evolve / decide (the kernel
// Handler, Equinox-proven). Typed command + event DUs; state = fold of own log.
module Counter =
    type Command = Add of int
    type Event   = Added of int
    let initial = 0
    let evolve state (Added n) = state + n
    let decide (Add n) _state = [ Added n ]
let counter =
    Entity.define "app/counter" Codec.counterEvent
        { Initial = Counter.initial; Evolve = Counter.evolve; Decide = Counter.decide }

// Host: registration is DATA — a flat list of definitions. (A service {}
// grouping CE can arrive later as sugar emitting the same registrations.)
let worker =
    Worker.run basin "prod"
        [ Worker.step reserve
          Worker.step notify
          Worker.workflow checkout
          Worker.entity counter ]

// Client: typed handles from the same descriptors — no stringly addressing.
let client = Client.connect basin
let! instance = Client.start client checkout order (InstanceId "order-42")  // Instance<Receipt>
do! Client.signal client instance approved decision
let! receipt = Instance.result instance                                     // typed result

// Stream-native primitives (beyond the DF trio):
let log = Logs.openLog client [ "invoices"; "2026-07" ]        // sealed durable log
do! Logs.attach log observe                                    // prefix → tail → terminal
let! view = Projections.read client myFold ReadGrade.Eventual  // lag = data
```

#### Authoring-ergonomics decision record (2026-07-07)

Sources: the evaluation SDD's DurableFunctions.FSharp lessons (typed
descriptors, the CE feature table, no-arbitrary-binds, Result-shaped waits),
DurableFunctions.FSharp itself (define-once activities, list-pipeline
fan-out, DU retry policies, `ContinueAsNew` eternal orchestrations — all
adopted), and Equinox's Decider pattern (entities as
`initial`/`evolve`/`decide`, structurally identical to the kernel Handler).
Decided:

1. **Typed descriptors with explicit codecs.** `Step<'i,'o>` / `Signal<'t>`
   / `Workflow.define` carry codecs explicitly — reflection-based JSON is
   Fable-unsafe and banned. This is the deliberate tax of the typed layer.
2. **Two-segment names (`service/handler`) from day one.** Names land in
   journals and instance addresses — the one durable commitment in this
   section. Freezing the scheme now keeps future grouping sugar a no-op on
   persisted data.
3. **Flat registration list.** The `service { }` custom-ops CE (the
   evaluation SDD's sketch) is deferred to the service-RPC parity work —
   additive later, expensive to design now. The builder-pipe host is
   rejected as dominated.
4. **`workflow { }`**, aligned with the kernel's journaled vocabulary
   (`WorkflowStarted`/`WorkflowName` in `Stepper` records); `durable { }`
   remains a deprecated alias.
5. **`and!` = fan-out** (doctrine-ratified applicative independence); T1's
   corpus includes a teaching test pinning that `let!` sequences and `and!`
   parallelizes.
6. Retry policies as DUs; eternal orchestrations as returned
   `ContinueAsNew` values.

**Future TS target (dormant until a TS consumer is prioritized):** the
library's Fable emission plus a thin `@firegrid/durable` wrapper — plain TS
only (Promise, `AsyncIterable`, tagged unions mirroring the DU errors; an
async/await `ctx` mirroring `workflow { }`, DF-SDK style). No `effect` in its
exported types or dependencies; an optional `/effect` subpath may wrap it.
Shipping it is a build-and-wrap decision, not a redesign — that is what the
standing Fable-green gate buys.

#### State materialization & consistent reads

The platform carries forward the proven semantics of the frozen stack's state
layer ([`fluent-firegrid-state-materialization-sdd.md`](./fluent-firegrid-state-materialization-sdd.md),
[`fluent-firegrid-finish-line-sdd.md`](./fluent-firegrid-finish-line-sdd.md)
— together the parity bar for retiring it), with one structural
simplification and one authoring change:

- **Entity state is the fold of the entity's own log.** Table-shaped
  authoring survives in F# (a `Table` schema record owning codec + primary
  key; `State.get`/`State.set`/`State.delete` inside entity handlers):
  mutations lower to journaled state-change facts, `get` reads the fold.
  Because handler state is a pure function of the log prefix,
  read-modify-write is replay-deterministic **by construction** — the old
  `StateReadJournaled` machinery is subsumed, not ported. Cross-subject
  reads from orchestrations journal as activities (result replay-served).
  Idempotent mutations and the crash/replay no-double-apply law carry over
  as corpus laws; owner fencing is `Authority` epoch fencing (stronger than
  the old lease scheme).
- **Reader-side materialization = checkpointed projections** (`Checkpoint` +
  `StateReads`, kernel-proven): `Projections.read client fold grade` with
  three read grades — `Eventual` (local fold; staleness is `AppliedTail`
  lag-data, never hidden), `Latest` (linearizable via the check-tail
  barrier), `Through v` (read-your-writes for a writer holding its append
  ack). Latest-value-per-`(table, key)` is one fold among many.
- **Durable predicate waits** port as a platform capability:
  `State.waitFor invoices "invoice-1" (Cel "row.status == 'paid'")` —
  serializable CEL predicates (keyed and indexed) registered as durable
  facts, evaluated on relevant state change, resumed via the wake/signal
  path, replay-served from the recorded resolution. The finish-line SDD's
  design carries over nearly verbatim because it already banned closures.
- The finish-line SDD's "Effect-native generators" authoring direction is
  **superseded**: the authoring surface is the F# `workflow { }` CE
  (program-as-data; determinism from journaling). The future TS emission
  authors async/await + `ctx` (DF-SDK/Restate-SDK style) — never `effect`
  generators.

### L4 — the agent-sessions reference application

An F# application in the platform's vocabulary: session = keyed entity,
cancel = a command, turn output = a sealed log (attach is the platform
primitive), history = a checkpointed projection. It lives app-side
(`examples/`, F#), and its scenario corpus **is the platform's integration
acceptance** — anywhere it must reach around `Firegrid.Durable`, that is a
platform gap and becomes a platform work packet. (agent-ui, being TS,
consumes the future emission — see L2.)

### L2 — the emission seam (dormant)

Materializes only when the TS target is prioritized: mechanical Fable
pass-throughs (`Async` acceptable; the wrapper converts to Promises), grown
demand-driven by the TS corpus mirror — orchestration drive/replay +
host/registry, entity admission, activity plumbing, sealed-log
create/append/seal/attach, checkpoint + read primitives. Application modules
are never seam mandates. Wake plumbing is not exported. Until then the
Fable-green build gate on L3 is what keeps this layer cheap to open.

## Doctrine (binding on ratification)

1. **Zero domain nouns** in L0–L3 — names, exports, subjects, docs.
2. **The platform surface is idiomatic F#/Fable** (`Async`/`Result`/DU
   errors per the evaluation SDD; EffSharp-free), and **every L3 module
   keeps the Fable build green** so TS cross-compilation stays a build
   decision, not a rewrite. TS-zone exported surfaces (the future emission
   wrapper; the harness-adapter contract) are **plain TS**: Promise,
   AsyncIterable, tagged unions — no `effect` in exported types or hard
   deps; an optional `/effect` subpath may wrap. Pre-platform Effect
   packages are exempt until retired.
3. **Names in consumer vocabulary**; spec coordinates (interface ids,
   milestone ids, WP ids) never appear in package names or exports.
4. **Placement**: platform never imports applications; domain modules never
   live in platform namespaces.
5. **Contracts are artifacts**: a target surface = package skeleton (bodies
   `throw NotImplemented`) + **frozen red corpus** (consumer tests that fail)
   + short prose companion. Prose-only or "by construction" surface claims
   are rejected at review.
6. **Kernel work is not wave-complete** until reachable through L3 or
   explicitly deferred as its own ledger row.

## Execution: the top-down red/green loop

1. The architect authors each surface package + red corpus; **the human
   ratifies the corpus before merge** (the contract is read as runnable
   code). Red tests are frozen at ratification — editing one is an architect
   gate.
2. A manifest (`targets.json`: test → work packet → `red`/`green`) drives a
   target suite beside the blocking suite, strict in both directions: CI
   fails on a green regression **or** on a red test passing without an
   explicit promotion commit (manifest + ledger flip together).
3. Workers take green-making work packets via the coordinator. Merge
   authority: reds flipped green, **zero edits to test bodies**, manifest and
   ledger flipped in the same PR.
4. Everything below L3 is built demand-driven by greening — no speculative
   seam exports, no speculative kernel surface.

| WP | Deliverable | Gate |
| --- | --- | --- |
| T0 | Ratchet: manifest runner + strict target suite in CI, spanning **both** runners (F# corpus project + TS suite) | None (mechanical) |
| T1 | **`Firegrid.Durable` library skeleton (F#) + red corpus (F# consumer tests) + platform prose companion.** Corpus: replay determinism across a host kill (activities not re-executed); fan-out/fan-in; `any` races; signal to a parked orchestration across restart; durable timer across restart; entity op serialization; typed activity failure; deterministic `currentTime`; status/result query; log attach (prefix/tail/terminal, byte-faithful); **entity table state** (get/set/delete; crash after mutation → no double-apply; deposed writer's state write fenced); **three read grades** (eventual with observable lag, latest linearizable, read-your-writes through an ack version); **CEL table wait** (immediate-if-true; park → mutate → resume; unrelated row does not resume an indexed wait; replay serves the recorded resolution); **`and!`-vs-`let!` teaching test** (sequencing vs fan-out semantics pinned); **typed-descriptor round-trip** (explicit codecs; two-segment `service/handler` name scheme enforced at registration) | **Human ratifies** |
| T2 | Reference-app red corpus (F#) against `Firegrid.Durable`: start turn / cancel from an unprivileged second client / duplicate-cancel idempotence / single-writer with typed rejection / deposed-writer rejection / attach semantics / history with cause and lag | **Human ratifies** |
| T3 | Harness-adapter contract as plain TS + red fixture-replay conformance corpus | **Human ratifies** |
| T4 | **(Dormant — schedule when a TS consumer is prioritized, e.g. agent-ui integration.)** Fable emission seam + `@firegrid/durable` plain-TS wrapper + TS mirror of the T1 corpus | **Human schedules + ratifies** |

T1 before T2 by design: the first application consumes the platform, it does
not bypass it. The agent-ui integration path now sits behind T4 — activating
it is a scheduling decision the human makes, not a default.

## Getting there from today (the only section where legacy names appear)

| Today on `main` | Disposition |
| --- | --- |
| `src/Firegrid.Store` Foundation modules (`Durable/*`, `Authority`, `DurableLog`, `Checkpoint`, `StateReads`, wake path) | **Platform kernel (L1). Keep** — proven this wave; gains seam + facade exposure as T1 greens. |
| `SessionLifecycle.fs`, `Turn.fs`, `SessionHistory.fs` (inside `Firegrid.Store`) | **Application code predating the platform surface.** Move out of platform namespaces; re-express over the facade as T2 greening demands; never platform API. |
| `Exports.fs` (StateReads + legacy exports) | **Dormant until T4** — becomes the emission seam only when the TS target is scheduled; legacy `ObjectState`/`WorkflowLog` exports retire with the TanStack path. |
| `@firegrid/l1-vocabulary` | Rename → `@firegrid/session-events` (L4 app-ecosystem package; content unchanged). |
| `@firegrid/harness-adapter`, `@firegrid/claude-adapter` | Keep names; exported contract de-Effected via T3; descriptions rewritten consumer-first. |
| `@firegrid/fluent` + `@firegrid/runtime` (TanStack) | Frozen. Retire when `Firegrid.Durable` reaches parity — the parity bar is the proven inventory in `fluent-firegrid-state-materialization-sdd.md` + `fluent-firegrid-finish-line-sdd.md` (table state, read grades, CEL waits, awakeables, delayed sends, send handles, idempotency keys), tracked as a checklist in T1's prose companion. Its TS *consumers* migrate when T4 ships the emission. |
| agent-ui (E-lane, TS) | Integration re-sequenced behind T4 (the TS emission) — a human scheduling decision. The F# reference app (T2) proves the platform semantics agent-ui will consume in the meantime. |
| `@firegrid/log` (empty stub) | Delete. |
| In-flight kernel WPs (A4 impl, C2, B4) | Finish as-is — L1 altitude, unaffected; they make greening cheaper. |
| Managed-sessions ledger + wave process | Continues for in-flight work only; all new surface work flows through T-rows. |

## Appendix: the 2026-07-07 audit (why this SDD exists)

The managed-sessions wave shipped a proven kernel with no consumer layer.
Verified findings: the seam exported almost nothing (the session kernel was
unreachable from TS); exports returned Fable `Async`, not Promises, despite
"Promise-first" surface claims; the P4 facade package was an empty stub; the
complete DF-style engine (`Durable<'a>` free monad, `durable { }` CE,
`Workflow.*`, `Stepper` journaled replay — kernel-proven) was unexported and
unnamed in any document; package names leaked ledger jargon; and the adapter
contract carried a hard `effect@4.0.0-beta.87` dependency in its exported
types, contradicting the ratified Promise-first canon — the drift entered
through a lanes-doc summary line ("Effect shapes per LLMS.md") and was then
enforced by review. Root cause: validation gates accepted *assertions*
("rides the seam, by construction") instead of artifacts, and no contract
defined each layer's outward surface. This SDD is that contract; the
red/green loop is its enforcement.
