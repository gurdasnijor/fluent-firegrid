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

// ── Define ───────────────────────────────────────────────────────────────
// Serialization is DERIVED from the types at compile time (records/DUs of
// plain data just work; Fable-safe inline derivation — no runtime
// reflection). Pinned codecs are an opt-in (`Step.defineWith`) for
// evolution-sensitive types. Names are two-segment ("service/handler") and
// land in journals — the one durable commitment, frozen from day one.
let reserve  = Step.define "orders/reserve" (fun (id: OrderId) -> async { (* … *) })
let notify   = Step.define "orders/notify"  (fun (id: OrderId) -> async { (* … *) })
let approved = Signal.define<Decision> "orders/approved"

// ── Orchestrate ──────────────────────────────────────────────────────────
// workflow {} is the ONLY place durable ops bind: a stray Async.Sleep or
// DateTime.Now does not compile (the restate-sdk-gen lesson — its generator
// DSL exists because raw async "gets awkward as concurrency multiplies";
// restricted bind is the point). Descriptors expose their ops as methods,
// so discovery is dot-completion. Fan-out (`and!`, `Step.all`, tagged
// `select`) is the concurrency vocabulary — see T1's corpus.
let checkout = Workflow.define "orders/checkout" (fun (order: Order) -> workflow {
    let! reservation = reserve.Call order.Id
    do! notify.Send order.Id                          // journaled fire-and-forget
    match! approved.Await (Duration.hours 48) with    // Ok decision | Error Timeout
    | Ok d when d.Accepted -> return Receipt.confirmed reservation
    | _                    -> return Receipt.rejected order.Id })

// ── Entities: the Decider shape — initial / evolve / decide ─────────────
module Counter =
    type Command = Add of int
    type Event   = Added of int
    let initial = 0
    let evolve state (Added n) = state + n
    let decide (Add n) _state = [ Added n ]
let counter =
    Entity.define "app/counter"
        { Initial = Counter.initial; Evolve = Counter.evolve; Decide = Counter.decide }

// ── Run: registration is data ────────────────────────────────────────────
let worker = Worker.run basin "prod" [ reg reserve; reg notify; reg checkout; reg counter ]

// ── Call: typed handles from the same descriptors ────────────────────────
let client = Client.connect basin
let! run = checkout.Start client order (Id "order-42")    // Run<Receipt>
do! run.Signal approved decision
let! receipt = run.Result

// ── Stream-native primitives (beyond the DF trio) ────────────────────────
let log = client.Logs [ "invoices"; "2026-07" ]           // sealed durable log
for ev in log.Attach() do observe ev                       // async seq: prefix → tail → terminal
let! view = client.Read myFold ReadGrade.Eventual          // lag = data
```

#### Authoring-ergonomics decision record (2026-07-07)

Sources: the evaluation SDD's DurableFunctions.FSharp lessons (typed
descriptors, the CE feature table, no-arbitrary-binds, Result-shaped waits),
DurableFunctions.FSharp itself (define-once activities, list-pipeline
fan-out, DU retry policies, `ContinueAsNew` eternal orchestrations — all
adopted), Equinox's Decider pattern (entities as `initial`/`evolve`/
`decide`, structurally identical to the kernel Handler), and
**restate-sdk-gen** (DESIGN/guide/README — the surface this engine's
lineage deliberately mirrors). Decided:

1. **Serialization is derived; codecs are opt-in.** Wire formats are
   derived from `'in`/`'out` at compile time via inline derivation
   (Thoth-`Auto`-style — Fable-safe; what is banned is *runtime*
   reflection serialization). Users never see a codec. `defineWith` pins an
   explicit codec for evolution-sensitive types (journaled wire formats
   couple to type shape); the corpus pins **golden wire fixtures** so a
   refactor that silently changes a derived format goes red.
2. **Two-segment names (`service/handler`) from day one.** Names land in
   journals and instance addresses — the one durable commitment in this
   section. Freezing the scheme now keeps future grouping sugar a no-op on
   persisted data.
3. **Flat registration list** via a single `reg` function. The
   `service { }` custom-ops CE is deferred to the service-RPC parity work;
   the builder-pipe host is rejected as dominated.
4. **`workflow { }` — settled by the lineage itself.** restate-sdk-gen
   exists because raw async+ctx "gets awkward as concurrency patterns
   multiply"; its generator DSL (yield* as the only bind, factory-per-run =
   our Delay/Run) is a CE in JavaScript clothing. The restricted bind is
   the product: a stray `Async.Sleep`/`DateTime.Now` is a compile error,
   not a replay corruption discovered weeks later. Aligned with the
   kernel's journaled vocabulary (`WorkflowStarted`); `durable { }` remains
   a deprecated alias. Raw async+ctx is rejected as the model sdk-gen was
   built to escape.
5. **Dot-method descriptors.** Ops hang off the descriptor
   (`reserve.Call`, `notify.Send`, `approved.Await`, `checkout.Start`,
   `run.Result`) so discovery is dot-completion; module functions remain
   underneath as the stable core (members are thin sugar).
6. **Cancellation semantics (lifted from sdk-gen verbatim).** Cancellation
   is observed at bind boundaries, never mid-statement, and is
   *recoverable*: catch the durable-cancelled failure inside `workflow {}`,
   perform more journaled work (compensation), return any value; uncaught →
   the typed `Cancelled` terminal. Step implementations receive
   cancellation through `Async`'s native `CancellationToken`.
7. **Tagged `select`** over the kernel's `WhenAny`, returning a
   caller-supplied DU consumed by `match!`; `Await`-with-timeout returning
   `Result` stays as the simple binary case. `and!` = applicative fan-out
   (teaching test in T1); `and!` is concurrency vocabulary, not quickstart
   material.
8. **Log attach is an async sequence** (`AsyncSeq` semantics on .NET; a JS
   async iterable under the future emission) — prefix → live tail →
   terminal as one `for … do` loop.
9. Retry policies as DUs; eternal orchestrations as returned
   `ContinueAsNew` values.
10. **Deferred, recorded, not designed here:** the sdk-gen
    `Operation`/`Future` split — `spawn` returning held, memoized futures
    with per-routine `interrupt`, `any`/`allSettled`, and abandon/join
    on-exit policies. A v2 kernel-semantics extension (our lazy
    `Workflow<'a>` programs already are the `Operation` half); adopt when
    a use case demands it, copying the proven design.
11. **Why the entity contract is a Decider, not an actor framework**
    (evaluated against Fable.Actor, MailboxProcessor, Akka/Akkling).
    In-memory actor libraries isolate at the *process*; entities isolate at
    the *stream* — their guarantees end at the crash/host boundary where
    ours begin (the engine's own history: in-process serialization ⇒ lost
    updates across a crash; single-writer must be a durable fenced inbox).
    Deeper: replay requires `state = fold(events)` — receive loops hold
    state in closures (unrecoverable) and reducer actors transition state
    without an event log (snapshot-only, no fenced atomic append+intents).
    Every actor system that adds durability converges on decide/evolve
    (Akka Persistence, Orleans JournaledGrain, Equinox): the Handler *is*
    the durable actor, not a hand-rolled alternative to one. Sanctioned
    reuse: in-memory actor libs as L1-shell plumbing (invisible) and freely
    inside L4 applications for ephemeral concurrency; never as the entity
    authoring contract.
    **The "S2-backed mailbox platform" variant** (plugging S2 in under an
    actor library's platform seam, e.g. Fable.Actor `Platform.fs`) was
    evaluated and is understood precisely: the seam abstracts transport
    between *live processes* and has no slots for the five things
    durability consists of — state (closure-held, never crosses the seam ⇒
    recovery = full re-execution with unjournaled duplicated effects),
    identity (Pid = live process; no claim/fence vocabulary ⇒ split brain),
    **selective receive** (skip-and-return is incompatible with a durable
    FIFO without persisted skip sets — why durable systems use explicit
    event keys, and why even BEAM/OTP has no durable actors: supervision
    restarts with lost state and defers persistence to a store),
    ref-correlated replies (assume a live caller; durable replies are
    journaled results attached by id), and provenance-less sends
    (at-least-once redelivery duplicates them). Threading those five
    through the seam rebuilds `Processor`/`Mailbox`/`Host` — the kernel
    *is* the S2 actor platform, with the authoring contract narrowed to
    the programs recovery can honor. **Sanctioned as a possible L4
    ecosystem utility:** durable-*delivery* actors — the actor programming
    model over an S2 inbox for stateless/idempotent ephemeral consumers
    (work queues, fan-in, adapter glue; at-least-once, no claims), a small
    package over `client.Logs` + a cursor.

#### Stress-test findings (2026-07-07 — Restate tutorial + choreography-agent mapping)

Working the Restate SDK tutorial use cases and the choreography-first agent
substrate (gurdasnijor/firegrid) through the sketch surfaced four surface
additions and two corpus laws; all are adopted:

1. **Contract/impl split**: `Step.declare` / `Workflow.declare` (name +
   codecs only — the descriptor a remote client imports) and
   `Worker.implement declaration impl`; `define` remains the fused
   convenience. (Restate ifaces; the old fluent `iface`/`implement`.)
2. **Instance cancel**: `Client.cancel instance` → typed `Cancelled`
   terminal, observable via attach — generalizes the session kernel's
   proven durable-cancel machinery; closes what the finish-line SDD had to
   defer indefinitely.
3. **Sends**: `Client.send` (one-way, provenance-deduped by the kernel
   mailbox) and `Client.sendAfter delay` (timer + send intents).
4. **Sub-orchestrations**: `Workflow.callChild childDef input` over the
   kernel's send-intent + `ChildTerminal` wake; fan-out spawn via
   `Step.all` of child calls.
5. Corpus law — **saga**: compensation via `try/with` + accumulated
   `Step.call`s; host killed mid-compensation → recovery completes the
   remaining compensations, none re-run.
6. Corpus law — **timeout race**: `Step.callWithTimeout` is a library
   combinator over the kernel's `WhenAny` (activity vs timer), returning
   `Result` — no new kernel surface.

Deliberately **not** adopted: Restate's hold-a-promise-await-later style —
arbitrary futures break the no-arbitrary-binds replay discipline; static
fan-out (`and!`, `Step.all`, `Workflow.any`) is the supported shape (the
DurableFunctions trade). Restate's *shared* (concurrent read) handlers map
to `Projections.read` grades instead of entity handlers — reads never
contend with the writer. HTTP ingress is an L4 transport concern (the old
`serveFluentS2` precedent), not platform surface. The choreography mapping
(wait_for/wait_until/spawn/execute → waits/timers/children/steps, with the
wake path as the coordination bus) feeds the reference app's scenario list.

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
  authoring survives in F# (a `Table` schema record owning identity + primary
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

Indicatively — note every domain word ("session", "turn") is defined *here*,
and everything on the right-hand side is a platform op:

```fsharp
// The session entity: single-live-turn policy as an ordinary Decider.
module Session =
    type Command = StartTurn of TurnId * HolderId | CancelTurn of TurnId
    type Event   = TurnStarted of TurnId | TurnEnded of TurnId * EndCause
    type State   = { Live: TurnId option }
    let initial = { Live = None }
    let evolve state = function
        | TurnStarted t   -> { Live = Some t }
        | TurnEnded (t,_) -> if state.Live = Some t then { Live = None } else state
    let decide cmd state =
        match cmd, state.Live with
        | StartTurn (t,_), Some live when live <> t -> []       // AlreadyLive: policy, not mechanism
        | StartTurn (t,_), _                        -> [ TurnStarted t ]
        | CancelTurn t,    Some live when live = t  -> [ TurnEnded (t, Cancelled) ]
        | CancelTurn _,    _                        -> []       // idempotent
let session = Entity.define "agent/session" { Initial = Session.initial; Evolve = Session.evolve; Decide = Session.decide }

// A turn's execution: an orchestration; its output: a sealed durable log.
let driveHarness = Step.define "agent/drive-harness" (fun (p: Prompt) -> async { (* … *) })
let runTurn = Workflow.define "agent/turn" (fun (input: TurnInput) -> workflow {
    let! artifact = driveHarness.Call input.Prompt
    return artifact })

// The UI path: attach to the turn log and fold history — platform primitives.
for ev in (client.Logs [ "agent"; sid; "turns"; tid ]).Attach() do render ev
let! history = client.Read sessionHistoryFold ReadGrade.Eventual
```

### L2 — the emission seam (dormant)

Materializes only when the TS target is prioritized: mechanical Fable
pass-throughs (`Async` acceptable; the wrapper converts to Promises), grown
demand-driven by the TS corpus mirror — orchestration drive/replay +
host/registry, entity admission, activity plumbing, sealed-log
create/append/seal/attach, checkpoint + read primitives. Application modules
are never seam mandates. Wake plumbing is not exported. Until then the
Fable-green build gate on L3 is what keeps this layer cheap to open.

When it opens, a seam export is a mechanical pass-through and the wrapper is
a thin conversion — nothing more:

```fsharp
// L2 (F#, generated posture): mechanical, Async is fine here.
let stepCall (step: Step<'i,'o>) (input: 'i) : Async<'o> = …
```
```ts
// Future @firegrid/durable (plain TS): Promise + tagged unions, no effect.
const receipt = await checkout.start(client, order, { id: "order-42" }).result()
```

### L1 — the kernel contracts L3 curates (all real on `main` today)

L3 adds ergonomics; it adds no semantics. These are the internal contracts
it composes — quoted near-verbatim from merged code:

```fsharp
// Orchestration semantics (Foundation/Durable/Semantics.fs): a FREE MONAD —
// the program is data; the interpreter (Stepper) replays it over a journal.
type Durable<'a> =
    | Return of 'a
    | Perform     of Activity * k: (Value -> Durable<'a>)        // one step
    | PerformAll  of Activity list * k: (Value list -> Durable<'a>) // fan-out
    | Await       of EventKey * k: (Value -> Durable<'a>)        // Timer | Signal
    | WhenAny     of RaceTask list * k: (RaceResult -> Durable<'a>) // races
    | CurrentTime of k: (int64 -> Durable<'a>)                   // deterministic time
// Stepper journal (StepRecordCodec): WorkflowStarted | HistoryEvent |
// Command (CallActivity/ScheduleTimer/…) | SignalDelivered | checkpoints.
// Replay = re-run the program; completed ops return journal values.

// Entity semantics (Foundation/Durable/Processor.fs): the Decider, natively.
type Handler<'state,'msg,'record,'terminal> =
    { Initial:    'state
      Fold:       'state -> StoredRecord<'record> -> 'state       // = evolve
      OnAdmitted: 'state -> Admitted<'msg> -> Decision<…>         // = decide (command)
      OnWake:     'state -> WakeReason -> Decision<…> }           // = decide (timer/child)
// Decision = { State; Append: 'record list; Intents; Seal } — committed
// atomically under the holder's fence; Intents = SetTimer | Send | Execute.

// Storage primitives (Foundation/*.fs) — every write path is fenced:
module Authority   = // claim : … -> HolderId -> Async<Result<Holder<'r>, ClaimError>>
                     // idempotent per epoch; different holder ⇒ epoch+1 deposal
module DurableLog  = // append/seal → Result<_, Deposed|Sealed|Failed>; attach: reader, no authority
module Checkpoint  = // latest / resumeFrom / commit (monotonic, race-safe via Authority)
module StateReads  = // readEventual / readThrough v / readLatest — the 3 read grades
// Wake path (WakeShard/WakeRouter): pointer records, FencedOwner routers —
// the liveness accelerator; sweeps remain the correctness floor.
```

### L0 — the substrate contract (everything above is built from exactly this)

```fsharp
// S2 (Firegrid.Log). Ordered durable streams — the platform's only dependency.
module S2 =
    type Basin                                    // a namespace of streams
    val ensureStream  : string -> Basin -> Async<unit>
    val append        : records -> StreamRef -> Async<AppendAck>   // ordered, durable
    val tryAppendWith : AppendOptions -> records -> StreamRef -> _ // matchSeqNum CAS + fencing
    val checkTail     : StreamRef -> Async<Tail>                   // linearizable barrier
    val readWith      : ReadOptions -> StreamRef -> _              // from-seq reads, with-wait tailing
// AppendAck.End.SeqNum is an EXCLUSIVE upper bound — the version convention
// every layer above inherits. Fencing + CAS on append is the mechanism under
// Authority; checkTail is the mechanism under strong reads.
```

### Composition: one signal, five layers

```
do! run.Signal approved decision                                   (L4 app / L3 handle)
 │   L3: encodes Decision via the derived codec; addresses the run's mailbox
 ▼
 L1: mailbox ADMISSION (provenance-deduped) → holder's Stepper resumes the
     journaled program → the parked `Await(Signal "orders/approved", k)` is
     matched → `SignalDelivered` is journaled → k decision continues
 ▼
 L0: every journal append lands via `tryAppendWith` UNDER THE HOLDER'S FENCE
     — a deposed holder computes but cannot commit
```

Recovery is the same picture in reverse: a fresh host re-runs the *same*
program; `Perform`/`Await` return journal values instead of executing;
execution resumes at the frontier. An L3 user never sees any of this — and
an L4 author never sees L1 at all: the layering rules are enforced by what
each layer exports, not by convention.

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
| T1 | **`Firegrid.Durable` library skeleton (F#) + red corpus (F# consumer tests) + platform prose companion.** Corpus: replay determinism across a host kill (activities not re-executed); fan-out/fan-in; `any` races; signal to a parked orchestration across restart; durable timer across restart; entity op serialization; typed activity failure; deterministic `currentTime`; status/result query; log attach (prefix/tail/terminal, byte-faithful); **entity table state** (get/set/delete; crash after mutation → no double-apply; deposed writer's state write fenced); **three read grades** (eventual with observable lag, latest linearizable, read-your-writes through an ack version); **CEL table wait** (immediate-if-true; park → mutate → resume; unrelated row does not resume an indexed wait; replay serves the recorded resolution); **`and!`-vs-`let!` teaching test** (sequencing vs fan-out semantics pinned); **golden wire fixtures** (derived serialization pinned — a refactor that changes a wire format goes red; two-segment `service/handler` name scheme enforced at registration); **saga compensation across a mid-compensation host kill**; **recoverable cancellation** (observed at a bind boundary, never mid-statement; caught → journaled compensation → return; uncaught → typed `Cancelled` terminal observed via attach); **step timeout race** (`WhenAny` activity-vs-timer as `Result`); **tagged `select`** (caller-DU race, `match!`-consumed); **declare/implement split** (remote client drives a workflow through the declaration only); **child spawn** (`callChild` + `ChildTerminal` completion, incl. fan-out) | **Human ratifies** |
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
