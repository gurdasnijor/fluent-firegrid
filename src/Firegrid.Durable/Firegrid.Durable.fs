/// ═══════════════════════════════════════════════════════════════════════
/// Firegrid.Durable — the platform's public API.
///
/// THIS FILE IS THE CONTRACT. It is written to be read top to bottom; every
/// body is `notYet` (implementation follows the surface, never the reverse).
/// If a signature here can't be implemented over the kernel, that is a
/// kernel work packet — the surface does not bend to internals.
///
/// Reading order:
///   1. Payloads & duration        — what your data must look like
///   2. Steps                      — durable calls to the outside world
///   3. Signals                    — durable external events
///   4. workflow { }               — durable programs (the only place these bind)
///   5. Entities                   — durable keyed state (decide/evolve)
///   6. Worker                     — hosting: registration is data
///   7. Client                     — start, signal, cancel, observe
///   8. Logs & Projections         — stream-native reads
///   9. Errors                     — every failure is a value you can match
///
/// Mechanical ceremony (T1; no public name, parameter order, or type shape
/// changed): the namespace is `rec` so the contract keeps its reading order
/// while sections reference each other (e.g. `EntityDef.State` uses
/// `ReadGrade` from section 9); `and` chains that followed a `module` are
/// re-rooted as `type` (identical declarations, legal F#).
///
/// Ratified amendments (architect ruling on PR #117 — internal
/// inconsistencies the T1 corpus exposed):
///   • `DurableLog.Seal`         — a terminal was promised; nothing could seal
///   • `Append : Async<Version>` — `Through v` requires the writer to HOLD an ack
///   • `Step.terminal`           — `StepError.Terminal` was unreachable from handlers
///
/// Implementation ceremony (T1 green-making; no public name, parameter order,
/// or member type shape changed — every delta is forced by Fable mechanics
/// and flagged in the promotion PR):
///   • contract types gained INTERNAL backing fields (records with `internal`
///     fields; members unchanged) — a body cannot exist without state
///   • `define`/`declare`/`attach` are `inline`: Fable erases generics, so
///     the derived-codec type capture (`typeof<'t>`) must happen at the call
///     site (spike/s0-ergonomics finding, T3)
///   • `WireNameAttribute` is an abbreviation of `CompiledNameAttribute`:
///     Fable erases custom attributes from reflection info entirely, and
///     `CompiledName` is the one case-name pin its reflection honors
///   • `DerivedInternal` (end of file) is public plumbing for the inline
///     capture sites — F# requires inline-reachable code to be public; it is
///     NOT part of the ratified surface
/// ═══════════════════════════════════════════════════════════════════════
namespace rec Firegrid.Durable

open Firegrid.Log
open Firegrid.Durable.Internal

[<AutoOpen>]
module private Impl =
    let notYet<'a> : 'a = failwith "Firegrid.Durable: surface only — not implemented"

// ── 1. Payloads & duration ────────────────────────────────────────────────

/// Payload doctrine (serialization is DERIVED from your types — you never
/// write a codec):
///   • records and unions of plain data just work
///   • int64/BigInt are NOT payload types (use int or float) — enforced at
///     definition time
///   • union case NAMES are the wire format: renaming a case is a breaking
///     change, caught by golden-fixture tests, pinned with [<WireName>]
type WireNameAttribute = Microsoft.FSharp.Core.CompiledNameAttribute

/// A span of time for timeouts, timers, and delays.
type Duration =
    internal { Ms: float }
    static member seconds (n: float) : Duration = { Ms = n * 1_000.0 }
    static member minutes (n: float) : Duration = { Ms = n * 60_000.0 }
    static member hours   (n: float) : Duration = { Ms = n * 3_600_000.0 }
    static member days    (n: float) : Duration = { Ms = n * 86_400_000.0 }

/// Wall-clock instant (ms since epoch). `float` by doctrine (no int64).
type Timestamp = float

// ── 2. Steps — durable calls to the outside world ─────────────────────────

/// Why a step failed, as a value. `Terminal` failures stop retrying.
type StepError =
    | Failed of message: string          // retries exhausted
    | Terminal of message: string        // the step itself said "don't retry":
                                         // raised via `Step.terminal message` —
                                         // one attempt, retries bypassed
                                         // whatever the policy says

/// Retry policy for a step. Applied by the platform, journaled with the step.
type Retry =
    | NoRetry
    | Fixed of attempts: int * every: Duration
    | Backoff of attempts: int * first: Duration * factor: float

/// A durable operation: defined ONCE as a value, called from workflows.
/// The name is two-segment ("service/handler"); it lands in journals and is
/// the step's permanent identity.
type Step<'input, 'output> =
    internal
        { StepName: string
          EncIn: 'input -> string
          DecIn: string -> 'input
          EncOut: 'output -> string
          DecOut: string -> 'output
          RunFn: 'input -> Async<'output>
          RegisterInto: RegBag -> RegBag }

    /// Call the step from a workflow. Journaled: executed at most once per
    /// workflow position; replay serves the recorded result.
    member step.Call (input: 'input) : Workflow<'output> = Wiring.stepCall step NoRetry input
    /// Fire-and-forget from a workflow: journaled dispatch, the workflow does
    /// NOT wait for completion. (One-way delivery; failures surface in ops
    /// telemetry, not in the calling workflow.)
    member step.Send (input: 'input) : Workflow<unit> = Wiring.stepSend step input
    /// As Call, with a retry policy.
    member step.CallWith (retry: Retry) (input: 'input) : Workflow<'output> = Wiring.stepCall step retry input

and Workflow<'a> = internal Workflow of WfNode<'a> // the durable-program type; built only by `workflow { }` and the members on Step/Signal/defs

[<RequireQualifiedAccess>]
module Step =
    /// Define a step: name + an ordinary async function. Serialization for
    /// 'input/'output is derived from the types.
    let inline define (name: string) (run: 'input -> Async<'output>) : Step<'input, 'output> =
        DerivedInternal.mkStep name run typeof<'input> typeof<'output>
    /// Define with pinned codecs, for types whose wire format must survive
    /// refactoring (see payload doctrine).
    let defineWith (codecs: Codecs<'input, 'output>) (name: string) (run: 'input -> Async<'output>) : Step<'input, 'output> =
        Wiring.mkStepWith name run codecs
    /// Declare a step you don't implement here — share the contract with a
    /// process that only *calls* it. `Worker.implement` binds the body.
    let inline declare<'input, 'output> (name: string) : Step<'input, 'output> =
        DerivedInternal.declareStep name typeof<'input> typeof<'output>
    /// Mark a failure TERMINAL from inside a step implementation: raising the
    /// returned exception fails the step as `StepError.Terminal message`
    /// after this one attempt — retries are bypassed regardless of policy.
    /// (Ratified amendment: `Terminal` was otherwise unreachable.)
    let terminal (message: string) : exn = TerminalStepSignal message :> exn

type Codecs<'i, 'o> = { EncodeIn: 'i -> string; DecodeIn: string -> 'i; EncodeOut: 'o -> string; DecodeOut: string -> 'o }

// ── 3. Signals — durable external events ──────────────────────────────────

/// A named, typed event a workflow can wait for and any process can send.
/// Parked waits are journal state: they pin no process and survive restarts.
type Signal<'payload> =
    internal
        { SignalName: string
          SigEnc: 'payload -> string
          SigDec: string -> 'payload }

    /// Wait for the signal, up to `timeout`. Returns `Error Timeout` — never
    /// throws — so timeout handling is ordinary `match!`.
    member signal.Await (timeout: Duration) : Workflow<Result<'payload, Timeout>> = Wiring.signalAwaitTimeout signal timeout
    /// Wait forever (human-in-the-loop without a deadline).
    member signal.Await () : Workflow<'payload> = Wiring.signalAwait signal

and Timeout = Timeout

[<RequireQualifiedAccess>]
module Signal =
    let inline define<'payload> (name: string) : Signal<'payload> =
        DerivedInternal.mkSignal name typeof<'payload>

// ── 4. workflow { } — durable programs ────────────────────────────────────
//
// The ONLY place durable operations bind. A stray `Async.Sleep`,
// `DateTime.Now`, or un-journaled call DOES NOT COMPILE inside this block —
// that is the point: replay corruption becomes a type error.
//
//   let! x  = step.Call input        // sequence
//   and! y  = other.Call input       // independent → fan-out, joined
//   do!  s  = step.Send input        // one-way
//   match! signal.Await t with …     // Result-shaped waits
//   try … with DurableCancelled → …  // cancellation & failures are catchable
//   while / for                      // bounded loops (see Eternal for unbounded)
//   return / return!                 // return! is stack-safe (guarded recursion)

type WorkflowBuilder() =
    member _.Bind (m: Workflow<'a>, f: 'a -> Workflow<'b>) : Workflow<'b> = Wiring.bindW m f
    member _.MergeSources (a: Workflow<'a>, b: Workflow<'b>) : Workflow<'a * 'b> = Wiring.mergeW a b   // and! = fan-out
    member _.Return (x: 'a) : Workflow<'a> = Wiring.retW x
    member _.ReturnFrom (m: Workflow<'a>) : Workflow<'a> = m
    member _.TryWith (m: Workflow<'a>, handler: exn -> Workflow<'a>) : Workflow<'a> = Wiring.tryWithW (fun () -> m) handler
    // Ceremony: `try/with` desugars against the *delayed* body, and this
    // builder's Delay is guarded (returns the thunk) — so the compiler needs
    // a thunk-shaped overload. Same operation, mechanical requirement.
    member _.TryWith (m: unit -> Workflow<'a>, handler: exn -> Workflow<'a>) : Workflow<'a> = Wiring.tryWithW m handler
    member _.While (guard: unit -> bool, body: unit -> Workflow<unit>) : Workflow<unit> = Wiring.whileW guard body
    member _.Zero () : Workflow<unit> = Wiring.retW ()
    member _.Delay (f: unit -> Workflow<'a>) : unit -> Workflow<'a> = f                    // program-as-data
    member _.Run (f: unit -> Workflow<'a>) : Workflow<'a> = f ()

[<AutoOpen>]
module Builder =
    let workflow = WorkflowBuilder()

/// A defined workflow: name + input/output types + the program.
type WorkflowDef<'input, 'output> =
    internal
        { WfName: string
          WfEncIn: 'input -> string
          WfDecIn: string -> 'input
          WfEncOut: 'output -> string
          WfDecOut: string -> 'output
          WfFactory: ('input -> Workflow<'output>) option
          RegisterInto: RegBag -> RegBag }

    /// Start an instance. The id is the idempotency key: same id → same run.
    member def.Start (client: Client) (input: 'input) (id: Id) : Async<Run<'output>> = Wiring.startRun def client input id
    /// Call as a durable child from a parent workflow; the parent waits for
    /// the child's result (journaled; survives both processes dying).
    member _.CallChild (input: 'input) : Workflow<'output> = notYet
    /// Fire-and-forget child.
    member _.SpawnChild (input: 'input) : Workflow<ChildHandle<'output>> = notYet

and Id = Id of string
and ChildHandle<'o> = member _.Await () : Workflow<'o> = notYet

/// What an eternal (unbounded-loop) workflow returns each generation.
/// `ContinueAsNew` rolls the journal: state carries forward, history resets.
type Eternal<'state> =
    | Stop
    | ContinueAsNew of 'state

[<RequireQualifiedAccess>]
module Workflow =
    /// Define a workflow: two-segment name + the program as a function.
    let inline define (name: string) (program: 'input -> Workflow<'output>) : WorkflowDef<'input, 'output> =
        DerivedInternal.mkWorkflow name program typeof<'input> typeof<'output>
    /// Declare without implementing (contract sharing; see Step.declare).
    let inline declare<'input, 'output> (name: string) : WorkflowDef<'input, 'output> =
        DerivedInternal.declareWorkflow name typeof<'input> typeof<'output>
    /// An unbounded loop as generations: the program runs one generation and
    /// returns `ContinueAsNew state` to roll over with a fresh journal.
    let defineEternal (name: string) (generation: 'state -> Workflow<Eternal<'state>>) : WorkflowDef<'state, unit> = notYet
    /// Deterministic local computation (pure; NOT for effects — those are steps).
    let local (name: string) (compute: unit -> 'a) : Workflow<'a> = notYet
    /// Current time, captured deterministically (same value on replay).
    /// (Module-level *value*: constructed from the pre-initialized Internal
    /// node so module initialization order cannot observe an unset binding.)
    let currentTime : Workflow<Timestamp> = Workflow Node.currentTimeNode
    /// Durable sleep until an instant.
    let sleepUntil (t: Timestamp) : Workflow<unit> = Wiring.sleepUntilW t
    /// Durable sleep for a span.
    let sleep (d: Duration) : Workflow<unit> = Wiring.sleepW d.Ms
    /// Fan-out: run all, wait for all, results in order.
    let all (workflows: Workflow<'a> list) : Workflow<'a list> = Wiring.allW workflows
    /// Tagged race: first to finish wins; you match on YOUR union.
    ///   let! winner = Workflow.select [
    ///     Approved  ^| signal.Await ()
    ///     Deadline  ^| Workflow.sleep (Duration.hours 48) ]
    let select (branches: SelectBranch<'tag> list) : Workflow<'tag> = Wiring.selectW branches

type SelectBranch<'tag> = internal SelectBranch of WaitSpec<'tag>

[<AutoOpen>]
module SelectSyntax =
    /// Tag a branch for `Workflow.select`: `Approved ^| signal.Await ()`.
    let (^|) (tag: 'a -> 'tag) (branch: Workflow<'a>) : SelectBranch<'tag> = Wiring.branch tag branch

// ── 5. Services — stateless durable request-response ─────────────────────
//
// The "start here" construct: no key, no state, unlimited concurrency. Each
// call is its own durable execution — journaled steps, retries, exactly-once
// effects — and returns a value. (Semantically: a workflow with an
// auto-generated instance id; named because the semantics deserve a name.)

type ServiceDef<'input, 'output> =
    /// Request-response. Durable: if the caller dies awaiting, the execution
    /// continues regardless; reattach by idempotency key to collect it.
    member _.Call (client: Client) (input: 'input) : Async<'output> = notYet
    /// Same call, deduplicated: same key ⇒ same execution, one result.
    member _.CallIdempotent (client: Client) (idempotencyKey: string) (input: 'input) : Async<'output> = notYet

[<RequireQualifiedAccess>]
module Service =
    let define (name: string) (handler: 'input -> Workflow<'output>) : ServiceDef<'input, 'output> = notYet
    let declare<'input, 'output> (name: string) : ServiceDef<'input, 'output> = notYet

// ── 6. Entities (virtual objects) — durable keyed state ──────────────────
//
// Key-addressable, uniquely-identified stateful instances. The address IS
// the stream identity (entity name + key ⇒ one journal, one writer), so
// uniqueness is structural, not registry-based.
//
// CONCURRENCY CONTRACT (the virtual-object constraint):
//   • EXCLUSIVE: at most one `Decide` runs per key at any moment, across
//     all hosts — enforced below the API by durable inbox admission
//     (serialized + deduplicated) and epoch fencing at commit (a deposed
//     writer computes but cannot commit, even mid-split-brain).
//   • SHARED: state reads run concurrently, never block the writer, and
//     say explicitly what consistency they accept (the grade).
//
// `Decide` is pure — full read access to state, the key in scope, a reply
// for the caller, events to append — with reply + events committed
// atomically under the key's fence. State = fold of the entity's own
// journal: recovery is refold, so read-modify-write is replay-safe BY
// CONSTRUCTION. Multi-step effectful logic under a key belongs in a
// workflow the entity starts — compose, don't fuse.

// HOW THE CONSTRAINT IS ENFORCED (the lowering — every promise above maps
// to kernel machinery that exists on main and is already proven):
//
//   entity.Call / Send          → append to the key's durable inbox stream
//                                 (kernel Mailbox: FIFO, provenance-deduped —
//                                 duplicate sends fold once; proven by the
//                                 turn/lifecycle idempotency proofs)
//   "at most one Decide per key"→ the key's Processor drive: exactly one
//                                 fenced holder folds the journal, runs
//                                 Decide, and commits — cross-host, via
//                                 Authority epoch fencing (proven:
//                                 store.object-live-fencing lineage;
//                                 session.lifecycle-deposed-producer — a
//                                 deposed writer computes but CANNOT commit)
//   reply + events atomically   → one fenced append (kernel Decision commit);
//                                 the reply is journaled with the events, so
//                                 a crash between "decided" and "answered"
//                                 cannot double-apply on retry
//   entity.State grade          → StateReads over the same journal (proven:
//                                 state.stateview-strong-read; lag-as-data)
//
// None of that machinery is new. What IS new — and only real once the red
// corpus pins it — are these laws driven through THIS surface:
//   • two hosts race Call on one key → serialized; both effects exactly once
//   • zombie writer's reply and events fenced out after takeover
//   • shared State read during an exclusive write: never blocks, never torn

/// The key addressing one entity instance (Restate: `ctx.key`).
type Key = Key of string

type Decider<'command, 'event, 'state, 'reply> =
    { Initial: 'state
      Evolve: 'state -> 'event -> 'state
      Decide: Key -> 'command -> 'state -> 'reply * 'event list }

type EntityDef<'command, 'event, 'state, 'reply> =
    internal
        { EntName: string
          EntEncCmd: 'command -> string
          EntDecReply: string -> 'reply
          RegisterInto: RegBag -> RegBag }

    /// EXCLUSIVE handler, request-response: serialized per key; the reply is
    /// computed with exclusive state access and returned to the caller.
    member entity.Call (client: Client) (key: string) (command: 'command) : Async<'reply> = Wiring.entityCall entity client key command
    /// EXCLUSIVE handler, fire-and-forget (the reply is discarded). Durable,
    /// deduplicated; any process may send — no locks, no ownership needed.
    member _.Send (client: Client) (key: string) (command: 'command) : Async<unit> = notYet
    /// Send after a delay (durable: fires even if every process restarts).
    member _.SendAfter (client: Client) (key: string) (delay: Duration) (command: 'command) : Async<unit> = notYet
    /// SHARED handler: concurrent read of a key's state; never blocks the
    /// writer; the consistency grade is explicit.
    member _.State (client: Client) (key: string) (grade: ReadGrade) : Async<'state> = notYet

[<RequireQualifiedAccess>]
module Entity =
    let inline define (name: string) (decider: Decider<'command, 'event, 'state, 'reply>) : EntityDef<'command, 'event, 'state, 'reply> =
        DerivedInternal.mkEntity name decider typeof<'command> typeof<'event> typeof<'reply>

// ── 7. Worker — hosting ───────────────────────────────────────────────────
//
// Registration is data: a list of definitions. `Worker.run` claims work for
// this namespace and RUNS the host loop until stopped (it does not return
// between ticks). Liveness across workers: any live worker per namespace
// keeps everything moving; parked work costs nothing.

type Registration = internal Registration of (RegBag -> RegBag)

type Worker =
    internal { StopFn: unit -> Async<unit> }
    member worker.Stop () : Async<unit> = worker.StopFn ()

[<AutoOpen>]
module Registrations =
    /// Register any definition — one word: `reg reserve`, `reg checkout`,
    /// `reg counter`. (Ships as overloads/SRTP, not obj.)
    let reg (definition: obj) : Registration = Wiring.regOf definition

[<RequireQualifiedAccess>]
module Worker =
    /// Bind a declared step/workflow to its implementation in THIS worker.
    let implement (declaration: WorkflowDef<'i, 'o>) (program: 'i -> Workflow<'o>) : WorkflowDef<'i, 'o> =
        Wiring.mkWorkflowDef
            declaration.WfName
            (Some program)
            { EncodeIn = declaration.WfEncIn
              DecodeIn = declaration.WfDecIn
              EncodeOut = declaration.WfEncOut
              DecodeOut = declaration.WfDecOut }
    let implementStep (declaration: Step<'i, 'o>) (run: 'i -> Async<'o>) : Step<'i, 'o> =
        Wiring.mkStepWith
            declaration.StepName
            run
            { EncodeIn = declaration.EncIn
              DecodeIn = declaration.DecIn
              EncodeOut = declaration.EncOut
              DecodeOut = declaration.DecOut }
    /// Run a namespace worker hosting these definitions. Returns the running
    /// worker (loop already started); `worker.Stop()` for graceful shutdown.
    let run (basin: S2.Basin) (ns: string) (definitions: Registration list) : Async<Worker> = Wiring.runWorker basin ns definitions

// ── 8. Client — start, signal, cancel, observe ────────────────────────────

type Client =
    internal { ClientBasin: S2.Basin }

    /// Everything below hangs off a connected client.
    member client.Logs (address: string list) : DurableLog =
        { LogBasin = client.ClientBasin
          LogStream = String.concat "/" address }
    member client.Read (projection: Projection<'state>) (grade: ReadGrade) : Async<View<'state>> = Wiring.readProjection client projection grade

/// A handle to a running (or finished) workflow instance. Serializable:
/// reconstruct with `Run.attach client id` from any process.
and Run<'output> =
    internal
        { RunKey: string
          RunBasin: S2.Basin
          RunSource: string
          mutable RunSeq: int
          RunDecOut: string -> 'output }

    /// The result. Waits durably; follows ContinueAsNew generation chains.
    member run.Result : Async<Result<'output, RunFailure>> = Wiring.runResult run
    member run.Status : Async<RunStatus> = Wiring.runStatus run
    member run.Signal (signal: Signal<'p>) (payload: 'p) : Async<unit> = Wiring.runSignal run signal.SignalName (signal.SigEnc payload)
    /// Durable cancel: delivered at the workflow's next bind boundary as a
    /// catchable failure (compensate, then return), else → Cancelled terminal.
    member run.Cancel () : Async<unit> = Wiring.runSignal run Node.cancelSignalName "null"

and RunStatus = Running | Completed | Cancelled | Failed of StepError
and RunFailure = FailedRun of StepError | CancelledRun

[<RequireQualifiedAccess>]
module Client =
    let connect (basin: S2.Basin) : Client = { ClientBasin = basin }
    /// Reattach to an instance by id from anywhere.
    let inline attach<'output> (client: Client) (id: Id) : Run<'output> =
        DerivedInternal.attach client id typeof<'output>

// ── 9. Logs & Projections — stream-native reads ───────────────────────────

/// A sealed durable log: byte-faithful attach — recorded prefix, then live
/// tail, then the terminal. One loop, no polling.
/// (T1 ceremony: gained INTERNAL backing fields — basin + derived stream
/// name; members unchanged.)
type DurableLog =
    internal
        { LogBasin: S2.Basin
          LogStream: string }

    member log.Attach () : AsyncSeq<LogEvent> =
        let pull = SLog.attach log.LogBasin log.LogStream

        AsyncSeq(fun () ->
            async {
                let! next = pull ()

                return
                    match next with
                    | Some(true, reason) -> Some(Terminal reason)
                    | Some(false, data) -> Some(Chunk data)
                    | None -> None
            })

    /// Append one record. The ack is the version to hand `Through` for
    /// read-your-writes. (Ratified amendment: was `Async<unit>`, which
    /// contradicted the read-grade doctrine — no way to hold an append ack.)
    member log.Append (data: string) : Async<Version> =
        async {
            let! version = SLog.append log.LogBasin log.LogStream data
            return Version version
        }

    /// Seal the log: every attach — current and future — ends with
    /// `Terminal reason` after the recorded prefix and live tail; further
    /// appends are refused. (Ratified amendment: the terminal was promised
    /// but nothing could seal.)
    member log.Seal (reason: string) : Async<unit> = SLog.seal log.LogBasin log.LogStream reason

and LogEvent = Chunk of data: string | Terminal of reason: string
// async sequence; a JS-async-iterable-compatible pull. (T1 ceremony: the
// `unit` placeholder gained the backing pull thunk — a body cannot exist
// without state; the public members are unchanged.)
and AsyncSeq<'t> = internal AsyncSeq of pull: (unit -> Async<'t option>)

[<RequireQualifiedAccess>]
module AsyncSeq =
    /// Consume a sequence to its end (for a log: prefix → tail → terminal).
    let iter (f: 't -> unit) (source: AsyncSeq<'t>) : Async<unit> =
        let (AsyncSeq pull) = source

        async {
            let mutable going = true

            while going do
                let! next = pull ()

                match next with
                | Some item -> f item
                | None -> going <- false
        }

    /// Stop consuming when `f` returns Some; yields that value.
    let pick (f: 't -> 'r option) (source: AsyncSeq<'t>) : Async<'r> =
        let (AsyncSeq pull) = source

        async {
            let mutable picked = None
            let mutable going = true

            while going do
                let! next = pull ()

                match next with
                | Some item ->
                    match f item with
                    | Some value ->
                        picked <- Some value
                        going <- false
                    | None -> ()
                | None -> going <- false

            match picked with
            | Some value -> return value
            | None -> return failwith "AsyncSeq.pick: the sequence ended without a match"
        }

/// A named fold over durable records — history views, indexes, dashboards.
type Projection<'state> =
    internal Projection of name: string * address: string list * initial: 'state * apply: ('state -> string -> 'state)

/// Read grades — you always know what staleness you're accepting:
///   Eventual  — local fold; may lag; the lag is data (View.Behind)
///   Latest    — linearizable at the checked tail
///   Through v — read-your-writes for a writer holding an append ack
and ReadGrade = Eventual | Latest | Through of Version
and Version = Version of float
and View<'state> = { State: 'state; AsOf: Version; Behind: float }

[<RequireQualifiedAccess>]
module Projection =
    let define (name: string) (source: string list) (initial: 'state) (apply: 'state -> string -> 'state) : Projection<'state> =
        Projection (name, source, initial, apply)

/// A serializable predicate (CEL text) — persisted with the wait, evaluated
/// on relevant state change, never a closure. Registration-time validated.
type Cel = Cel of string

[<RequireQualifiedAccess>]
module Wait =
    /// Park a workflow until a projection's state satisfies the predicate
    /// (immediately, if it already does). Durable: pins no process; resumes
    /// via the wake path; replay serves the recorded resolution.
    /// (T1 ceremony: `inline` — Fable erases generics, so the derived
    /// state-codec type capture must happen at the call site, exactly as
    /// `define`/`declare`/`attach`.)
    let inline state (projection: Projection<'state>) (predicate: Cel) (timeout: Duration) : Workflow<Result<'state, Timeout>> =
        StreamsDerived.waitState projection predicate timeout typeof<'state>

// ── 10. Errors ─────────────────────────────────────────────────────────────
//
// Everything above returns Results or raises exactly one catchable durable
// exception inside `workflow { }`:

/// Raised at a bind boundary when this instance is cancelled. Catch it to
/// compensate (more journaled work is allowed), rethrow or return to finish.
exception DurableCancelled

/// Raised when a step exhausts retries / fails terminally and you didn't
/// handle the Result-shaped variant. Catchable; carries the StepError.
exception DurableStepFailed of StepError

// ── Wiring (internal) ──────────────────────────────────────────────────────
//
// Glue between the contract types above and the Internal machinery: this is
// where the L2 surface is lowered onto the L1 kernel. Internal helpers only;
// nothing here is surface.

module internal Wiring =
    open Firegrid.Store.Foundation.Durable

    module KD = Firegrid.Store.Foundation.Durable.Durable
    module KApp = Firegrid.Store.Foundation.Durable.App.DurableApp
    module KActivity = Firegrid.Store.Foundation.Durable.App.Activity
    module KWorkflow = Firegrid.Store.Foundation.Durable.App.Workflow

    /// The journaled terminal outcome of a run: what the `<instance>/out`
    /// stream carries and `Run.Result`/`Run.Status` decode.
    type WfOutcome =
        | WOk of string
        | WErr of StepError
        | WCancelled

    let private outcomeTy = typeof<WfOutcome>
    let encodeOutcome (outcome: WfOutcome) : string = Codec.encodeWith outcomeTy (box outcome)
    let decodeOutcome (text: string) : WfOutcome = Codec.decodeWith outcomeTy text |> unbox

    // ── program lowering ──────────────────────────────────────────────────

    let private cancelledExn () = DurableCancelled :> exn
    let lowerNode<'a> (node: WfNode<'a>) : Durable<'a> = Node.lower cancelledExn node
    let lowerW<'a> (Workflow node: Workflow<'a>) : Durable<'a> = lowerNode node

    let retW<'a> (value: 'a) : Workflow<'a> = Workflow(NProg(KD.result value))

    let bindW<'a, 'b> (m: Workflow<'a>) (f: 'a -> Workflow<'b>) : Workflow<'b> =
        Workflow(NProg(lowerW m |> KD.bind (fun value -> lowerW (f value))))

    let mergeW<'a, 'b> (a: Workflow<'a>) (b: Workflow<'b>) : Workflow<'a * 'b> =
        Workflow(NProg(Fanout.pair (lowerW a) (lowerW b)))

    let allW<'a> (workflows: Workflow<'a> list) : Workflow<'a list> =
        Workflow(NProg(Fanout.all (workflows |> List.map lowerW)))

    let tryWithW<'a> (body: unit -> Workflow<'a>) (handler: exn -> Workflow<'a>) : Workflow<'a> =
        Workflow(NProg(Programs.catch (fun () -> lowerW (body ())) (fun error -> lowerW (handler error))))

    let whileW (guard: unit -> bool) (body: unit -> Workflow<unit>) : Workflow<unit> =
        Workflow(NProg(Programs.whileLoop guard (fun () -> lowerW (body ()))))

    let sleepW (ms: float) : Workflow<unit> =
        Workflow(
            NWait
                { NeedsNow = true
                  Arity = 1
                  Tasks = fun now -> [ RaceEvent(EventKey.Timer(now + int64 ms)) ]
                  Project =
                    fun baseIndex result ->
                        match result with
                        | EventWon(index, EventKey.Timer _, _) when index = baseIndex -> KD.result ()
                        | other -> failwith ("sleep: unexpected race winner " + string other) }
        )

    let sleepUntilW (t: float) : Workflow<unit> =
        Workflow(
            NWait
                { NeedsNow = false
                  Arity = 1
                  Tasks = fun _ -> [ RaceEvent(EventKey.Timer(int64 t)) ]
                  Project =
                    fun baseIndex result ->
                        match result with
                        | EventWon(index, EventKey.Timer _, _) when index = baseIndex -> KD.result ()
                        | other -> failwith ("sleepUntil: unexpected race winner " + string other) }
        )

    let signalAwait<'p> (signal: Signal<'p>) : Workflow<'p> =
        Workflow(
            NWait
                { NeedsNow = false
                  Arity = 1
                  Tasks = fun _ -> [ RaceEvent(EventKey.Signal signal.SignalName) ]
                  Project =
                    fun baseIndex result ->
                        match result with
                        | EventWon(index, EventKey.Signal _, payload) when index = baseIndex ->
                            KD.result (signal.SigDec payload)
                        | other -> failwith ("signal await: unexpected race winner " + string other) }
        )

    let signalAwaitTimeout<'p> (signal: Signal<'p>) (timeout: Duration) : Workflow<Result<'p, Timeout>> =
        Workflow(
            NWait
                { NeedsNow = true
                  Arity = 2
                  Tasks =
                    fun now ->
                        [ RaceEvent(EventKey.Signal signal.SignalName)
                          RaceEvent(EventKey.Timer(now + int64 timeout.Ms)) ]
                  Project =
                    fun baseIndex result ->
                        match result with
                        | EventWon(index, EventKey.Signal _, payload) when index = baseIndex ->
                            KD.result (Ok(signal.SigDec payload))
                        | EventWon(index, EventKey.Timer _, _) when index = baseIndex + 1 ->
                            KD.result (Error Timeout)
                        | other -> failwith ("signal await: unexpected race winner " + string other) }
        )

    let branch<'a, 'tag> (tag: 'a -> 'tag) (w: Workflow<'a>) : SelectBranch<'tag> =
        match w with
        | Workflow(NWait spec) -> SelectBranch(WaitSpec.map tag spec)
        | Workflow(NProg _) ->
            failwith
                "Workflow.select: a branch must be a primitive wait (Signal.Await, Workflow.sleep, Workflow.sleepUntil)"

    let selectW<'tag> (branches: SelectBranch<'tag> list) : Workflow<'tag> =
        match branches with
        | [] -> failwith "Workflow.select: no branches"
        | _ ->
            Workflow(NWait(WaitSpec.combine (branches |> List.map (fun (SelectBranch spec) -> spec))))

    // ── steps ─────────────────────────────────────────────────────────────

    let codecsFor<'i, 'o> (inTy: System.Type) (outTy: System.Type) : Codecs<'i, 'o> =
        Codec.ensureWireSafe inTy
        Codec.ensureWireSafe outTy

        { EncodeIn = fun (value: 'i) -> Codec.encodeWith inTy (box value)
          DecodeIn = fun text -> Codec.decodeWith inTy text |> unbox<'i>
          EncodeOut = fun (value: 'o) -> Codec.encodeWith outTy (box value)
          DecodeOut = fun text -> Codec.decodeWith outTy text |> unbox<'o> }

    let mkStepWith<'i, 'o> (name: string) (run: 'i -> Async<'o>) (codecs: Codecs<'i, 'o>) : Step<'i, 'o> =
        let handler =
            StepWire.wrapHandler (fun payload ->
                async {
                    let! output = run (codecs.DecodeIn payload)
                    return codecs.EncodeOut output
                })

        { StepName = name
          EncIn = codecs.EncodeIn
          DecIn = codecs.DecodeIn
          EncOut = codecs.EncodeOut
          DecOut = codecs.DecodeOut
          RunFn = run
          RegisterInto =
            fun bag ->
                { bag with
                    App = KApp.addActivity (KActivity.define name handler) bag.App } }

    let mkStep<'i, 'o> (name: string) (run: 'i -> Async<'o>) (inTy: System.Type) (outTy: System.Type) : Step<'i, 'o> =
        mkStepWith name run (codecsFor inTy outTy)

    let declareStep<'i, 'o> (name: string) (inTy: System.Type) (outTy: System.Type) : Step<'i, 'o> =
        let declared (_: 'i) : Async<'o> =
            async {
                return
                    failwith (
                        "step '"
                        + name
                        + "' is declared here without a body; bind one with Worker.implementStep"
                    )
            }

        mkStepWith name declared (codecsFor inTy outTy)

    let private policyOf (retry: Retry) : StepPolicy =
        match retry with
        | NoRetry -> { A = 1; Ms = 0.0; F = 1.0 }
        | Fixed(attempts, every) -> { A = attempts; Ms = every.Ms; F = 1.0 }
        | Backoff(attempts, first, factor) -> { A = attempts; Ms = first.Ms; F = factor }

    let stepCall<'i, 'o> (step: Step<'i, 'o>) (retry: Retry) (input: 'i) : Workflow<'o> =
        let envelope = StepWire.encodeEnvelope { Pol = policyOf retry; P = step.EncIn input }
        let activity: Activity = { Name = step.StepName; Input = envelope }

        Workflow(
            NProg(
                Perform(
                    activity,
                    fun value ->
                        match StepWire.decodeOutcome value with
                        | SOk payload -> KD.result (step.DecOut payload)
                        | SFail message -> raise (DurableStepFailed(StepError.Failed message))
                        | STerm message -> raise (DurableStepFailed(StepError.Terminal message))
                )
            )
        )

    /// DELTA (documented in the promotion PR): the kernel's only call shape is
    /// Perform (call + await completion), so Send is journaled dispatch that
    /// still waits for handler completion before the next bind. True one-way
    /// delivery is the K1 kernel packet.
    let stepSend<'i, 'o> (step: Step<'i, 'o>) (input: 'i) : Workflow<unit> =
        let envelope = StepWire.encodeEnvelope { Pol = policyOf NoRetry; P = step.EncIn input }
        let activity: Activity = { Name = step.StepName; Input = envelope }
        Workflow(NProg(Perform(activity, fun _ -> KD.result ())))

    // ── workflow definitions ──────────────────────────────────────────────

    let mkWorkflowDef<'i, 'o> (name: string) (factory: ('i -> Workflow<'o>) option) (codecs: Codecs<'i, 'o>) : WorkflowDef<'i, 'o> =
        let register (bag: RegBag) : RegBag =
            match factory with
            | None ->
                failwith (
                    "workflow '"
                    + name
                    + "' is a declaration; bind an implementation with Worker.implement before registering"
                )
            | Some program ->
                let build (raw: string) : Durable<string> =
                    Programs.catch
                        (fun () ->
                            lowerW (program (codecs.DecodeIn raw))
                            |> KD.map (fun output -> encodeOutcome (WOk(codecs.EncodeOut output))))
                        (fun error ->
                            match error with
                            | DurableCancelled -> KD.result (encodeOutcome WCancelled)
                            | DurableStepFailed stepError -> KD.result (encodeOutcome (WErr stepError))
                            | other -> raise other)

                { bag with
                    App = KApp.addWorkflow (KWorkflow.defineWith name id id id id build) bag.App }

        { WfName = name
          WfEncIn = codecs.EncodeIn
          WfDecIn = codecs.DecodeIn
          WfEncOut = codecs.EncodeOut
          WfDecOut = codecs.DecodeOut
          WfFactory = factory
          RegisterInto = register }

    // ── runs (start, attach, observe) ─────────────────────────────────────

    let startRun<'i, 'o> (def: WorkflowDef<'i, 'o>) (client: Client) (input: 'i) (Id key: Id) : Async<Run<'o>> =
        async {
            let! result =
                DurableClient.startWith
                    client.ClientBasin
                    (InstanceId.create key)
                    (WorkflowName.create def.WfName)
                    (def.WfEncIn input)

            match result with
            | DurableClientStartStatus.Accepted _ -> ()
            | DurableClientStartStatus.Failed failure -> failwith ("workflow start failed: " + string failure)

            do! OutStream.ensure client.ClientBasin key

            return
                { RunKey = key
                  RunBasin = client.ClientBasin
                  RunSource = "run/" + Interop.entropy ()
                  RunSeq = 0
                  RunDecOut = def.WfDecOut }
        }

    let attachRun<'o> (client: Client) (Id key: Id) (outTy: System.Type) : Run<'o> =
        { RunKey = key
          RunBasin = client.ClientBasin
          RunSource = "run/" + Interop.entropy ()
          RunSeq = 0
          RunDecOut = fun text -> Codec.decodeWith outTy text |> unbox<'o> }

    let runResult<'o> (run: Run<'o>) : Async<Result<'o, RunFailure>> =
        async {
            let! body = OutStream.awaitOutcome run.RunBasin run.RunKey

            return
                match decodeOutcome body with
                | WOk payload -> Ok(run.RunDecOut payload)
                | WErr stepError -> Error(FailedRun stepError)
                | WCancelled -> Error CancelledRun
        }

    let runStatus<'o> (run: Run<'o>) : Async<RunStatus> =
        async {
            let! body = OutStream.readOutcome run.RunBasin run.RunKey

            return
                match body with
                | None -> Running
                | Some text ->
                    match decodeOutcome text with
                    | WOk _ -> Completed
                    | WErr stepError -> RunStatus.Failed stepError
                    | WCancelled -> Cancelled
        }

    let runSignal<'o> (run: Run<'o>) (name: string) (payload: string) : Async<unit> =
        async {
            let seq = run.RunSeq
            run.RunSeq <- run.RunSeq + 1

            let! result =
                DurableClient.raiseSignalFrom
                    run.RunBasin
                    (InstanceId.create run.RunKey)
                    run.RunSource
                    (int64 seq)
                    name
                    payload

            match result with
            | DurableClientSignalStatus.Accepted _ -> ()
            | DurableClientSignalStatus.Failed failure -> failwith ("signal failed: " + string failure)
        }

    // ── signals ───────────────────────────────────────────────────────────

    let mkSignal<'p> (name: string) (payloadTy: System.Type) : Signal<'p> =
        Codec.ensureWireSafe payloadTy

        { SignalName = name
          SigEnc = fun (value: 'p) -> Codec.encodeWith payloadTy (box value)
          SigDec = fun text -> Codec.decodeWith payloadTy text |> unbox<'p> }

    // ── entities ──────────────────────────────────────────────────────────

    let mkEntity<'c, 'e, 's, 'r>
        (name: string)
        (decider: Decider<'c, 'e, 's, 'r>)
        (cmdTy: System.Type)
        (evtTy: System.Type)
        (replyTy: System.Type)
        : EntityDef<'c, 'e, 's, 'r> =
        Codec.ensureWireSafe cmdTy
        Codec.ensureWireSafe evtTy
        Codec.ensureWireSafe replyTy

        let decCmd (text: string) : 'c = Codec.decodeWith cmdTy text |> unbox
        let encEvt (value: 'e) = Codec.encodeWith evtTy (box value)
        let decEvt (text: string) : 'e = Codec.decodeWith evtTy text |> unbox
        let encReply (value: 'r) = Codec.encodeWith replyTy (box value)

        let spec: EntityRuntimeSpec =
            { Name = name
              Initial = box decider.Initial
              Evolve = fun state body -> box (decider.Evolve (unbox<'s> state) (decEvt body))
              Decide =
                fun key cmdBody state ->
                    let reply, events = decider.Decide (Key key) (decCmd cmdBody) (unbox<'s> state)
                    encReply reply, events |> List.map encEvt }

        { EntName = name
          EntEncCmd = fun (value: 'c) -> Codec.encodeWith cmdTy (box value)
          EntDecReply = fun text -> Codec.decodeWith replyTy text |> unbox<'r>
          RegisterInto = fun bag -> { bag with Entities = bag.Entities @ [ spec ] } }

    let entityCall<'c, 'e, 's, 'r> (entity: EntityDef<'c, 'e, 's, 'r>) (client: Client) (key: string) (command: 'c) : Async<'r> =
        async {
            let basin = client.ClientBasin
            do! basin |> S2.ensureStream (EntityRun.journalName entity.EntName key)
            do! basin |> S2.ensureStream (EntityRun.inboxName entity.EntName key)
            let source = "ec/" + Interop.entropy ()
            do! EntityRun.submitCommand basin entity.EntName key source 0.0 (entity.EntEncCmd command)
            let! replyBody = EntityRun.awaitReply basin entity.EntName key source 0.0
            return entity.EntDecReply replyBody
        }

    // ── projections ───────────────────────────────────────────────────────

    let readProjection<'s> (client: Client) (projection: Projection<'s>) (grade: ReadGrade) : Async<View<'s>> =
        let (Projection(projName, address, initial, apply)) = projection

        match grade with
        | Latest ->
            async {
                let streamName = String.concat "/" address
                do! client.ClientBasin |> S2.ensureStream streamName
                let stream = client.ClientBasin |> S2.stream streamName
                let! tail = stream |> S2.checkTail

                if tail.SeqNum <= 0L then
                    return { State = initial; AsOf = Version 0.0; Behind = 0.0 }
                else
                    let! records = EntityRun.readAllRecords stream

                    let state =
                        records
                        |> List.fold (fun acc (record: S2.ReadRecord) -> apply acc record.Body) initial

                    return
                        { State = state
                          AsOf = Version(float tail.SeqNum)
                          Behind = 0.0 }
            }
        // Eventual / Through compose the kernel StateReads (resident fold +
        // check-tail barrier); Latest above keeps its stateless fold.
        | Eventual ->
            async {
                let streamName = String.concat "/" address
                let! state, asOf, behind = GradedReads.eventual client.ClientBasin projName streamName initial apply

                return
                    { State = state
                      AsOf = Version asOf
                      Behind = behind }
            }
        | Through(Version version) ->
            async {
                let streamName = String.concat "/" address
                let! state, asOf, behind = GradedReads.through client.ClientBasin projName streamName initial apply version

                return
                    { State = state
                      AsOf = Version asOf
                      Behind = behind }
            }

    // ── worker ────────────────────────────────────────────────────────────

    let regOf (definition: obj) : Registration =
        let register = Interop.dynGet definition "RegisterInto"

        if Interop.isNullish register then
            failwith "reg: not a registrable definition (expected a Step/Workflow/Entity definition value)"
        else
            Registration(unbox<RegBag -> RegBag> register)

    let runWorker (basin: S2.Basin) (_ns: string) (definitions: Registration list) : Async<Worker> =
        async {
            let bag =
                (RegBag.empty, definitions)
                ||> List.fold (fun acc (Registration register) -> register acc)

            // The CEL-wait watcher rides the worker: any process that hosts a
            // worker evaluates CEL waits, including worker-only processes.
            CelWatch.noteBasin basin
            let stop = WorkerLoop.start basin bag
            return { StopFn = stop }
        }

// ── Derived-codec capture plumbing ─────────────────────────────────────────
//
// PUBLIC only because F# requires everything an `inline` body touches to be
// accessible at the call site (Fable erases generics, so the contract's
// `define`/`declare`/`attach` capture `typeof<…>` inline and land here).
// NOT part of the ratified surface — never call directly.
[<System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
module DerivedInternal =
    let mkStep<'i, 'o> (name: string) (run: 'i -> Async<'o>) (inTy: System.Type) (outTy: System.Type) : Step<'i, 'o> =
        Wiring.mkStep name run inTy outTy

    let declareStep<'i, 'o> (name: string) (inTy: System.Type) (outTy: System.Type) : Step<'i, 'o> =
        Wiring.declareStep name inTy outTy

    let mkSignal<'p> (name: string) (payloadTy: System.Type) : Signal<'p> = Wiring.mkSignal name payloadTy

    let mkWorkflow<'i, 'o> (name: string) (program: 'i -> Workflow<'o>) (inTy: System.Type) (outTy: System.Type) : WorkflowDef<'i, 'o> =
        Wiring.mkWorkflowDef name (Some program) (Wiring.codecsFor inTy outTy)

    let declareWorkflow<'i, 'o> (name: string) (inTy: System.Type) (outTy: System.Type) : WorkflowDef<'i, 'o> =
        Wiring.mkWorkflowDef name None (Wiring.codecsFor inTy outTy)

    let mkEntity<'c, 'e, 's, 'r>
        (name: string)
        (decider: Decider<'c, 'e, 's, 'r>)
        (cmdTy: System.Type)
        (evtTy: System.Type)
        (replyTy: System.Type)
        : EntityDef<'c, 'e, 's, 'r> =
        Wiring.mkEntity name decider cmdTy evtTy replyTy

    let attach<'o> (client: Client) (id: Id) (outTy: System.Type) : Run<'o> = Wiring.attachRun client id outTy

// ── Streams-packet inline-capture plumbing ─────────────────────────────────
//
// Same ceremony as `DerivedInternal`, for the streams section: PUBLIC only
// because `Wait.state` is `inline` (Fable erases generics; the state-codec
// type capture happens at the call site and lands here). NOT part of the
// ratified surface — never call directly.
[<System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
module StreamsDerived =
    open Firegrid.Store.Foundation.Durable

    /// Lower `Wait.state` onto the kernel: journal the registration as a
    /// durable fact (a kernel `Log` op — idempotent under replay), then park
    /// on a signal⊕timeout race. The wake path (`CelWatch`) folds the
    /// projection's source, evaluates the SERIALIZABLE predicate on relevant
    /// change, and resumes the race through the kernel signal mechanism; the
    /// journaled resolution serves every replay.
    let waitState<'state>
        (projection: Projection<'state>)
        (predicate: Cel)
        (timeout: Duration)
        (stateTy: System.Type)
        : Workflow<Result<'state, Timeout>> =
        let (Projection(projName, address, initial, apply)) = projection
        let (Cel predText) = predicate
        let streamName = String.concat "/" address

        // Registration-time validation: an invalid predicate fails HERE,
        // never inside a parked wait.
        let evaluate = CelPredicate.compile predText
        let signalName = CelWatch.signalName projName predText

        Codec.ensureWireSafe stateTy

        CelWatch.registerSpec
            { Sig = signalName
              Stream = streamName
              Initial = box initial
              Apply = fun state body -> box (apply (unbox<'state> state) body)
              Pred = evaluate
              Enc = fun state -> Codec.encodeWith stateTy state }

        let decodeState (payload: string) : 'state =
            Codec.decodeWith stateTy payload |> unbox<'state>

        let registration: Workflow<unit> =
            Workflow(NProg(Log(CelWatch.registrationMessage signalName streamName predText, Return)))

        let wait: Workflow<Result<'state, Timeout>> =
            Workflow(
                NWait
                    { NeedsNow = true
                      Arity = 2
                      Tasks =
                        fun now ->
                            [ RaceEvent(EventKey.Signal signalName)
                              RaceEvent(EventKey.Timer(now + int64 timeout.Ms)) ]
                      Project =
                        fun baseIndex result ->
                            match result with
                            | EventWon(index, EventKey.Signal _, payload) when index = baseIndex ->
                                Durable.result (Ok(decodeState payload))
                            | EventWon(index, EventKey.Timer _, _) when index = baseIndex + 1 ->
                                Durable.result (Error Timeout)
                            | other -> failwith ("Wait.state: unexpected race winner " + string other) }
            )

        Wiring.bindW registration (fun () -> wait)
