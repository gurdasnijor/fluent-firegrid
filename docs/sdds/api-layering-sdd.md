# SDD: The Firegrid Platform — API Layers & Target Surfaces

Doc-Class: sdd
Status: draft — architect-authored; **pending human ratification** (nothing
in the execution plan dispatches until ratified)
Date: 2026-07-07
Owner: Firegrid Architecture
Companions: [`api-layering-decisions.md`](./api-layering-decisions.md) (all
rationale, rejected alternatives, and audit history — deliberately kept out
of this document), [`fsharp-fable-effsharp-evaluation-sdd.md`](./fsharp-fable-effsharp-evaluation-sdd.md)
(F# style doctrine)

## The product

**Firegrid is a durable-execution platform over S2 streams.** Semantics
follow the Durable Functions model (OOPSLA 2021): **workflows** as
deterministic programs replayed over journaled histories, addressable
single-writer **entities**, at-least-once **steps** with journal-served
results, durable **timers and signals**. Hosting follows the Pulsar
Functions spirit: functions attached to streams, run by namespace-scoped
workers. Being stream-native, it also offers what those systems don't:
**sealed durable logs with byte-faithful attach** and **checkpointed
projections with graded reads**.

The platform ships **zero domain nouns** (no "session", "agent", "turn" —
exactly as Restate has no "session") and its primary interface is an
**F#/Fable library**, `Firegrid.Durable`. TypeScript is a future emission
target, kept open by a standing Fable-green build gate.

## The layers

```
 L3  APPLICATIONS & ADAPTERS — own vocabulary, zero platform nouns
       agent systems (worked example below) · harness adapters (TS)
       · agent-ui (TS; consumes the future emission)
        │  imports L2 only
 L2  Firegrid.Durable — THE public library (F#/Fable)
       steps · workflows · entities · signals & timers          ─┐ future branch:
       · sealed logs (attach) · projections & reads              │ Fable emission →
       · table state & waits                                     │ @firegrid/durable
        │  curates L1; adds ergonomics, never semantics          │ (plain-TS wrapper)
 L1  KERNEL (F#, internal, proven)                              ─┘
       free-monad programs + journaled replay (Stepper)
       · Decider processors + deduped mailboxes · fenced authority
       · sealed logs · checkpoints · read grades · wake path
        │  the only layer that touches S2
 L0  S2 — ordered durable streams
       append · CAS/fenced append · check-tail · tailing reads
```

| Layer | What it is | Audience | Rule |
| --- | --- | --- | --- |
| L3 | Applications and adapters | App developers, end users | Own vocabulary; import L2 only |
| L2 | `Firegrid.Durable`, the public library | F#/Fable developers (TS developers once the emission ships) | Idiomatic F#; Fable-green always; adds ergonomics, never semantics |
| L1 | The kernel | Platform developers, proofs | Domain-free; internal; never imports upward |
| L0 | S2 streams | L1 only | Never leaks upward |
| — | TS branch: Fable emission + `@firegrid/durable` plain-TS wrapper | Future TS consumers | **Dormant** until a TS consumer is scheduled; plain TS only (Promise, tagged unions, no `effect`) |

## L2 — the public API

Idiomatic F#: `Async` + `Result` + DU errors, EffSharp-free, Fable-green.
Serialization is **derived from your types at compile time** (Fable-safe
inline derivation; pinned codecs are an opt-in for evolution-sensitive
types). Names are two-segment (`service/handler`) and land in journals —
the one durable commitment, frozen from day one. `workflow { }` is the only
place durable operations bind: a stray `Async.Sleep` or `DateTime.Now` does
not compile. Indicative shape (T1's red corpus refines and freezes it):

```fsharp
open Firegrid.Durable

// ── Define ────────────────────────────────────────────────────────────
let reserve  = Step.define "orders/reserve" (fun (id: OrderId) -> async { (* … *) })
let notify   = Step.define "orders/notify"  (fun (id: OrderId) -> async { (* … *) })
let approved = Signal.define<Decision> "orders/approved"

// ── Orchestrate ───────────────────────────────────────────────────────
let checkout = Workflow.define "orders/checkout" (fun (order: Order) -> workflow {
    let! reservation = reserve.Call order.Id
    do! notify.Send order.Id                          // journaled fire-and-forget
    match! approved.Await (Duration.hours 48) with    // Ok decision | Error Timeout
    | Ok d when d.Accepted -> return Receipt.confirmed reservation
    | _                    -> return Receipt.rejected order.Id })

// ── Entities: the Decider — initial / evolve / decide ────────────────
module Counter =
    type Command = Add of int
    type Event   = Added of int
    let initial = 0
    let evolve state (Added n) = state + n
    let decide (Add n) _state = [ Added n ]
let counter =
    Entity.define "app/counter"
        { Initial = Counter.initial; Evolve = Counter.evolve; Decide = Counter.decide }

// ── Run: registration is data ─────────────────────────────────────────
let worker = Worker.run basin "prod" [ reg reserve; reg notify; reg checkout; reg counter ]

// ── Call: typed handles from the same descriptors ─────────────────────
let client = Client.connect basin
let! run = checkout.Start client order (Id "order-42")    // Run<Receipt>
do! run.Signal approved decision
let! receipt = run.Result                                  // run.Cancel also exists

// ── Stream-native primitives ──────────────────────────────────────────
let log = client.Logs [ "invoices"; "2026-07" ]           // sealed durable log
for ev in log.Attach() do observe ev                       // async seq: prefix → tail → terminal
let! view = client.Read myFold ReadGrade.Eventual          // staleness = data
```

Also in the surface (details frozen by T1's corpus): `Step.declare` /
`Workflow.declare` + `Worker.implement` (share the contract, not the impl);
`Workflow.callChild` (durable sub-workflows); `Client.send` / `sendAfter`
(one-way and delayed, deduped); tagged `select` and `Step.all` / `and!`
fan-out; DU retry policies; `ContinueAsNew` eternal workflows; recoverable
cancellation (observed at bind boundaries; catch → journaled compensation →
return; uncaught → typed `Cancelled` terminal).

### State materialization & consistent reads

- **Entity state is the fold of the entity's own log.** Table authoring
  (`State.get/set/delete` over a `Table` schema) lowers to journaled
  state-change facts; because handler state is a pure function of the log
  prefix, read-modify-write is replay-deterministic *by construction*.
  Cross-subject reads from workflows journal as steps.
- **Reader-side materialization = checkpointed projections** with three
  read grades: `Eventual` (local fold; lag exposed as data), `Latest`
  (linearizable via check-tail), `Through v` (read-your-writes from an
  append ack).
- **Durable predicate waits**: `State.waitFor invoices key (Cel "row.status
  == 'paid'")` — serializable CEL predicates registered as durable facts,
  evaluated on change, resumed via the wake path, replay-served.

The proven inventory of the frozen fluent stack
([`fluent-firegrid-state-materialization-sdd.md`](./fluent-firegrid-state-materialization-sdd.md),
[`fluent-firegrid-finish-line-sdd.md`](./fluent-firegrid-finish-line-sdd.md))
is the parity bar for retiring it.

## Worked example — building a firegrid (choreography-first agents) on L2

The system that motivated this platform: durable primitives that let the
*model* own control flow at runtime — every wait, spawn, and tool call
durable, replayable, observable; sessions that coordinate only through the
durable core. Its five primitives are one platform op each:

| firegrid primitive | Platform expression |
| --- | --- |
| `execute(tool)` | `Step.call` — journaled, replay-served |
| `wait_for(event, prompt?)` | `Signal.Await` / CEL predicate wait — parks durably, pins no process |
| `wait_until(t, prompt)` / `sleep` | durable timer; the self-prompt rides app state |
| `spawn` / `spawn_all` | `Workflow.callChild` / `Workflow.all` of children |
| publish → others await | append to shared logs; wake path delivers — the choreography bus |

```fsharp
// ── The app's vocabulary (none of this is platform surface) ───────────
type ModelSays =
    | ToolUse of ToolCall | NeedsApproval of Request
    | FollowUpAt of Timestamp * prompt: string
    | Spawn of TurnInput list | Done of Outcome

let callModel = Step.define "agent/model" (fun (m: ModelState) -> async { (* harness *) })
let runTool   = Step.define "agent/tool"  (fun (t: ToolCall)  -> async { (* sandbox *) })
let approval  = Signal.define<Decision> "agent/approval"
let turnDecl  = Workflow.declare<TurnInput, Outcome> "agent/turn"   // for recursion

// ── One turn: the MODEL decides; each decision point is durable ───────
let turn = Worker.implement turnDecl (fun input -> workflow {
    let rec drive model = workflow {
        match! callModel.Call model with
        | Done outcome -> return outcome
        | ToolUse call ->                          // execute()
            let! result = runTool.Call call
            return! drive (Model.feed model result)
        | NeedsApproval req ->                     // wait_for(): human-in-the-loop,
            match! approval.Await (Duration.days 7) with   // costs nothing while parked
            | Ok d          -> return! drive (Model.feed model (Approved d))
            | Error Timeout -> return! drive (Model.feed model ApprovalTimedOut)
        | FollowUpAt (t, prompt) ->                // wait_until(t, prompt): self-prompt
            do! Workflow.sleepUntil t
            return! drive (Model.feed model (SelfPrompt prompt))
        | Spawn subtasks ->                        // spawn_all(): durable children
            let! results = Workflow.all [ for s in subtasks -> turnDecl.CallChild s ]
            return! drive (Model.feed model (ChildResults results)) }
    return! drive (Model.start input.Prompt) })

// ── The session entity: which turn is live; cancel is just a command ──
module Session =
    type Command = Prompt of TurnId * string | Cancel of TurnId
    type Event   = TurnStarted of TurnId * string | TurnEnded of TurnId * EndCause
    (* initial / evolve / decide: single-live-turn policy, ~15 lines *)
let session = Entity.define "agent/session" Session.decider

// ── Choreography: sessions never call each other ───────────────────────
// A finished turn's outcome is appended to a shared log; any other session
// wait_for's it (signal or CEL predicate). Plans emerge from published
// outputs — the wake path is the coordination bus.

// ── Observability: the trace IS the schedule ───────────────────────────
for ev in (client.Logs [ "agent"; sid; "turns"; tid ]).Attach() do render ev
let! history = client.Read sessionHistoryFold ReadGrade.Eventual
```

Two structural properties fall out for free: a parked wait is journal state
(no process pinned; the sweep + wake floor guarantees resume), and the
journal *is* the trace — including the hours a session was suspended with
no process alive. This example doubles as the reference application: its
scenario corpus (T2) is the platform's integration acceptance, and any
place it must reach around L2 is by definition a platform gap.

## L1 and L0 — the contracts underneath (quoted near-verbatim from `main`)

L2 adds ergonomics; it adds no semantics. What it curates:

```fsharp
// L1: workflow semantics — a FREE MONAD; the program is data; the Stepper
// replays it over a journal (WorkflowStarted | HistoryEvent | Command | …).
type Durable<'a> =
    | Return of 'a
    | Perform     of Activity * k: (Value -> Durable<'a>)
    | PerformAll  of Activity list * k: (Value list -> Durable<'a>)
    | Await       of EventKey * k: (Value -> Durable<'a>)     // Timer | Signal
    | WhenAny     of RaceTask list * k: (RaceResult -> Durable<'a>)
    | CurrentTime of k: (int64 -> Durable<'a>)

// L1: entity semantics — the Decider, natively.
type Handler<'state,'msg,'record,'terminal> =
    { Initial:    'state
      Fold:       'state -> StoredRecord<'record> -> 'state    // = evolve
      OnAdmitted: 'state -> Admitted<'msg> -> Decision<…>      // = decide (command)
      OnWake:     'state -> WakeReason -> Decision<…> }        // = decide (timer/child)
// Decision = { State; Append; Intents; Seal } — committed atomically under
// the holder's fence. Plus: Authority (fenced claim/epoch deposal),
// DurableLog (sealed logs; Deposed|Sealed errors), Checkpoint (monotonic
// snapshots), StateReads (3 grades), the wake path (liveness accelerator;
// sweeps are the correctness floor).

// L0: S2 — everything above is built from exactly this.
//   append (ordered, durable) · tryAppendWith (CAS matchSeqNum + fencing)
//   · checkTail (linearizable barrier) · readWith (from-seq, with-wait)
// AppendAck.End.SeqNum is an EXCLUSIVE upper bound — the version convention
// every layer inherits.
```

### Composition: one signal, four layers

```
do! run.Signal approved decision                     (L3 app via L2 handle)
 │   L2: derived codec encodes Decision; addresses the run's mailbox
 ▼
 L1: mailbox ADMISSION (provenance-deduped) → the holder's Stepper resumes
     the journaled program → parked `Await(Signal "orders/approved", k)`
     matches → `SignalDelivered` journaled → k continues
 ▼
 L0: every journal append lands via fenced CAS — a deposed holder computes
     but cannot commit
```

Recovery is the same picture: a fresh host re-runs the *same* program;
`Perform`/`Await` return journal values instead of executing; execution
resumes at the frontier. An L3 author never sees any of it.

## Doctrine (binding on ratification)

1. **Zero domain nouns** at L0–L2 — names, exports, subjects, docs.
2. **L2 is idiomatic F#/Fable and always Fable-green** (TS stays a build
   decision). TS-zone exported surfaces (the future wrapper; adapter
   contracts) are plain TS — Promise, tagged unions, no `effect` in
   exported types or deps; optional `/effect` subpath may wrap. Legacy
   Effect packages exempt until retired.
3. **Names in consumer vocabulary**; spec coordinates never in package
   names or exports.
4. **Placement**: the platform never imports applications; domain modules
   never live in platform namespaces.
5. **Contracts are artifacts**: surface skeleton + frozen red corpus +
   short prose. "By construction" claims are rejected at review.
6. **Kernel work is not wave-complete** until reachable through L2 or
   explicitly deferred as its own ledger row.

## Execution: the top-down red/green loop

1. The architect authors each surface skeleton + red corpus; **the human
   ratifies the corpus before merge** (the contract is read as runnable
   code). Red tests are frozen at ratification — editing one is a gate.
2. A manifest (`targets.json`) drives a target suite beside the blocking
   suite, strict both ways: CI fails on a green regression **or** a red
   test passing without an explicit promotion commit (manifest + ledger
   flip together).
3. Workers take green-making packets via the coordinator. Merge authority:
   reds flipped green, **zero edits to test bodies**, manifest + ledger in
   the same PR.
4. Everything below L2 is built demand-driven by greening — no speculative
   surface.

| WP | Deliverable | Gate |
| --- | --- | --- |
| T0 | Ratchet: manifest runner + strict target suite in CI (F# + TS runners) | None (mechanical) |
| T1 | `Firegrid.Durable` skeleton + red corpus + prose companion. Corpus: replay determinism across a host kill; fan-out/fan-in; races (incl. tagged `select`); signal to a parked workflow across restart; durable timer across restart; entity op serialization; typed step failure; deterministic `currentTime`; status/result query; log attach (byte-faithful, prefix→tail→terminal); entity table state (no double-apply; fenced stale writer); three read grades; CEL table wait; saga compensation across a mid-compensation kill; recoverable cancellation; declare/implement round-trip; child spawn; `and!`-vs-`let!` teaching test; golden wire fixtures | **Human ratifies** |
| T2 | Reference-app red corpus (the worked example above) against L2 — the platform's integration acceptance | **Human ratifies** |
| T3 | Harness-adapter contract as plain TS + red fixture-replay corpus | **Human ratifies** |
| T4 | **(Dormant)** Fable emission + `@firegrid/durable` plain-TS wrapper + TS corpus mirror — scheduled only when a TS consumer (e.g. agent-ui) is prioritized | **Human schedules + ratifies** |

## Getting there from today (the only section with legacy names)

| Today on `main` | Disposition |
| --- | --- |
| `src/Firegrid.Store` Foundation modules (`Durable/*`, `Authority`, `DurableLog`, `Checkpoint`, `StateReads`, wake path) | **Kernel (L1). Keep** — proven; gains L2 exposure as T1 greens. |
| `SessionLifecycle.fs`, `Turn.fs`, `SessionHistory.fs` (inside `Firegrid.Store`) | Application code predating L2. Move out of platform namespaces; re-express over L2 as T2 greening demands. |
| `Exports.fs` | Dormant until T4 (the emission seam); legacy `ObjectState`/`WorkflowLog` exports retire with TanStack. |
| `@firegrid/l1-vocabulary` | Rename → `@firegrid/session-events` (L3 app-ecosystem package). |
| `@firegrid/harness-adapter`, `@firegrid/claude-adapter` | Keep names; contracts de-Effected via T3; descriptions rewritten consumer-first. |
| `@firegrid/fluent` + `@firegrid/runtime` (TanStack) | Frozen; retire at L2 parity (bar: the two fluent SDDs' proven inventory, tracked in T1's prose companion). |
| `@firegrid/log` (empty stub) | Delete. |
| agent-ui (E-lane, TS) | Integration sits behind T4 — a human scheduling decision; the F# reference app proves the semantics meanwhile. |
| Managed-sessions ledger + wave process | Continues for in-flight kernel work only; new surface work flows through T-rows. |
