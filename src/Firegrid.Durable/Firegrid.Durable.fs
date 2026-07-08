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
/// ═══════════════════════════════════════════════════════════════════════
namespace Firegrid.Durable

open Firegrid.Log

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
type WireNameAttribute(name: string) =
    inherit System.Attribute()

/// A span of time for timeouts, timers, and delays.
type Duration =
    static member seconds (n: float) : Duration = notYet
    static member minutes (n: float) : Duration = notYet
    static member hours   (n: float) : Duration = notYet
    static member days    (n: float) : Duration = notYet

/// Wall-clock instant (ms since epoch). `float` by doctrine (no int64).
type Timestamp = float

// ── 2. Steps — durable calls to the outside world ─────────────────────────

/// Why a step failed, as a value. `Terminal` failures stop retrying.
type StepError =
    | Failed of message: string          // retries exhausted
    | Terminal of message: string        // the step itself said "don't retry"

/// Retry policy for a step. Applied by the platform, journaled with the step.
type Retry =
    | NoRetry
    | Fixed of attempts: int * every: Duration
    | Backoff of attempts: int * first: Duration * factor: float

/// A durable operation: defined ONCE as a value, called from workflows.
/// The name is two-segment ("service/handler"); it lands in journals and is
/// the step's permanent identity.
type Step<'input, 'output> =
    /// Call the step from a workflow. Journaled: executed at most once per
    /// workflow position; replay serves the recorded result.
    member _.Call (input: 'input) : Workflow<'output> = notYet
    /// Fire-and-forget from a workflow: journaled dispatch, the workflow does
    /// NOT wait for completion. (One-way delivery; failures surface in ops
    /// telemetry, not in the calling workflow.)
    member _.Send (input: 'input) : Workflow<unit> = notYet
    /// As Call, with a retry policy.
    member _.CallWith (retry: Retry) (input: 'input) : Workflow<'output> = notYet

and Workflow<'a> = internal Workflow of unit // the durable-program type; built only by `workflow { }` and the members on Step/Signal/defs

[<RequireQualifiedAccess>]
module Step =
    /// Define a step: name + an ordinary async function. Serialization for
    /// 'input/'output is derived from the types.
    let define (name: string) (run: 'input -> Async<'output>) : Step<'input, 'output> = notYet
    /// Define with pinned codecs, for types whose wire format must survive
    /// refactoring (see payload doctrine).
    let defineWith (codecs: Codecs<'input, 'output>) (name: string) (run: 'input -> Async<'output>) : Step<'input, 'output> = notYet
    /// Declare a step you don't implement here — share the contract with a
    /// process that only *calls* it. `Worker.implement` binds the body.
    let declare<'input, 'output> (name: string) : Step<'input, 'output> = notYet

and Codecs<'i, 'o> = { EncodeIn: 'i -> string; DecodeIn: string -> 'i; EncodeOut: 'o -> string; DecodeOut: string -> 'o }

// ── 3. Signals — durable external events ──────────────────────────────────

/// A named, typed event a workflow can wait for and any process can send.
/// Parked waits are journal state: they pin no process and survive restarts.
type Signal<'payload> =
    /// Wait for the signal, up to `timeout`. Returns `Error Timeout` — never
    /// throws — so timeout handling is ordinary `match!`.
    member _.Await (timeout: Duration) : Workflow<Result<'payload, Timeout>> = notYet
    /// Wait forever (human-in-the-loop without a deadline).
    member _.Await () : Workflow<'payload> = notYet

and Timeout = Timeout

[<RequireQualifiedAccess>]
module Signal =
    let define<'payload> (name: string) : Signal<'payload> = notYet

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
    member _.Bind (m: Workflow<'a>, f: 'a -> Workflow<'b>) : Workflow<'b> = notYet
    member _.MergeSources (a: Workflow<'a>, b: Workflow<'b>) : Workflow<'a * 'b> = notYet   // and! = fan-out
    member _.Return (x: 'a) : Workflow<'a> = notYet
    member _.ReturnFrom (m: Workflow<'a>) : Workflow<'a> = notYet
    member _.TryWith (m: Workflow<'a>, handler: exn -> Workflow<'a>) : Workflow<'a> = notYet
    member _.While (guard: unit -> bool, body: unit -> Workflow<unit>) : Workflow<unit> = notYet
    member _.Zero () : Workflow<unit> = notYet
    member _.Delay (f: unit -> Workflow<'a>) : unit -> Workflow<'a> = notYet               // program-as-data
    member _.Run (f: unit -> Workflow<'a>) : Workflow<'a> = notYet

[<AutoOpen>]
module Builder =
    let workflow = WorkflowBuilder()

/// A defined workflow: name + input/output types + the program.
type WorkflowDef<'input, 'output> =
    /// Start an instance. The id is the idempotency key: same id → same run.
    member _.Start (client: Client) (input: 'input) (id: Id) : Async<Run<'output>> = notYet
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
    let define (name: string) (program: 'input -> Workflow<'output>) : WorkflowDef<'input, 'output> = notYet
    /// Declare without implementing (contract sharing; see Step.declare).
    let declare<'input, 'output> (name: string) : WorkflowDef<'input, 'output> = notYet
    /// An unbounded loop as generations: the program runs one generation and
    /// returns `ContinueAsNew state` to roll over with a fresh journal.
    let defineEternal (name: string) (generation: 'state -> Workflow<Eternal<'state>>) : WorkflowDef<'state, unit> = notYet
    /// Deterministic local computation (pure; NOT for effects — those are steps).
    let local (name: string) (compute: unit -> 'a) : Workflow<'a> = notYet
    /// Current time, captured deterministically (same value on replay).
    let currentTime : Workflow<Timestamp> = notYet
    /// Durable sleep until an instant.
    let sleepUntil (t: Timestamp) : Workflow<unit> = notYet
    /// Durable sleep for a span.
    let sleep (d: Duration) : Workflow<unit> = notYet
    /// Fan-out: run all, wait for all, results in order.
    let all (workflows: Workflow<'a> list) : Workflow<'a list> = notYet
    /// Tagged race: first to finish wins; you match on YOUR union.
    ///   let! winner = Workflow.select [
    ///     Approved  ^| signal.Await ()
    ///     Deadline  ^| Workflow.sleep (Duration.hours 48) ]
    let select (branches: SelectBranch<'tag> list) : Workflow<'tag> = notYet

and SelectBranch<'tag> = internal SelectBranch of unit

[<AutoOpen>]
module SelectSyntax =
    /// Tag a branch for `Workflow.select`: `Approved ^| signal.Await ()`.
    let (^|) (tag: 'a -> 'tag) (branch: Workflow<'a>) : SelectBranch<'tag> = notYet

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
    /// EXCLUSIVE handler, request-response: serialized per key; the reply is
    /// computed with exclusive state access and returned to the caller.
    member _.Call (client: Client) (key: string) (command: 'command) : Async<'reply> = notYet
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
    let define (name: string) (decider: Decider<'command, 'event, 'state, 'reply>) : EntityDef<'command, 'event, 'state, 'reply> = notYet

// ── 7. Worker — hosting ───────────────────────────────────────────────────
//
// Registration is data: a list of definitions. `Worker.run` claims work for
// this namespace and RUNS the host loop until stopped (it does not return
// between ticks). Liveness across workers: any live worker per namespace
// keeps everything moving; parked work costs nothing.

type Registration = internal Registration of unit
type Worker = member _.Stop () : Async<unit> = notYet

[<AutoOpen>]
module Registrations =
    /// Register any definition — one word: `reg reserve`, `reg checkout`,
    /// `reg counter`. (Ships as overloads/SRTP, not obj.)
    let reg (definition: obj) : Registration = notYet

[<RequireQualifiedAccess>]
module Worker =
    /// Bind a declared step/workflow to its implementation in THIS worker.
    let implement (declaration: WorkflowDef<'i, 'o>) (program: 'i -> Workflow<'o>) : WorkflowDef<'i, 'o> = notYet
    let implementStep (declaration: Step<'i, 'o>) (run: 'i -> Async<'o>) : Step<'i, 'o> = notYet
    /// Run a namespace worker hosting these definitions. Returns the running
    /// worker (loop already started); `worker.Stop()` for graceful shutdown.
    let run (basin: S2.Basin) (ns: string) (definitions: Registration list) : Async<Worker> = notYet

// ── 8. Client — start, signal, cancel, observe ────────────────────────────

type Client =
    /// Everything below hangs off a connected client.
    member _.Logs (address: string list) : DurableLog = notYet
    member _.Read (projection: Projection<'state>) (grade: ReadGrade) : Async<View<'state>> = notYet

/// A handle to a running (or finished) workflow instance. Serializable:
/// reconstruct with `Run.attach client id` from any process.
and Run<'output> =
    /// The result. Waits durably; follows ContinueAsNew generation chains.
    member _.Result : Async<Result<'output, RunFailure>> = notYet
    member _.Status : Async<RunStatus> = notYet
    member _.Signal (signal: Signal<'p>) (payload: 'p) : Async<unit> = notYet
    /// Durable cancel: delivered at the workflow's next bind boundary as a
    /// catchable failure (compensate, then return), else → Cancelled terminal.
    member _.Cancel () : Async<unit> = notYet

and RunStatus = Running | Completed | Cancelled | Failed of StepError
and RunFailure = FailedRun of StepError | CancelledRun

[<RequireQualifiedAccess>]
module Client =
    let connect (basin: S2.Basin) : Client = notYet
    /// Reattach to an instance by id from anywhere.
    let attach<'output> (client: Client) (id: Id) : Run<'output> = notYet

// ── 9. Logs & Projections — stream-native reads ───────────────────────────

/// A sealed durable log: byte-faithful attach — recorded prefix, then live
/// tail, then the terminal. One loop, no polling.
and DurableLog =
    member _.Attach () : AsyncSeq<LogEvent> = notYet
    member _.Append (data: string) : Async<unit> = notYet

and LogEvent = Chunk of data: string | Terminal of reason: string
and AsyncSeq<'t> = internal AsyncSeq of unit   // async sequence; a JS async iterable under the future TS emission

[<RequireQualifiedAccess>]
module AsyncSeq =
    /// Consume a sequence to its end (for a log: prefix → tail → terminal).
    let iter (f: 't -> unit) (source: AsyncSeq<'t>) : Async<unit> = notYet
    /// Stop consuming when `f` returns Some; yields that value.
    let pick (f: 't -> 'r option) (source: AsyncSeq<'t>) : Async<'r> = notYet

/// A named fold over durable records — history views, indexes, dashboards.
and Projection<'state> = internal Projection of unit

/// Read grades — you always know what staleness you're accepting:
///   Eventual  — local fold; may lag; the lag is data (View.Behind)
///   Latest    — linearizable at the checked tail
///   Through v — read-your-writes for a writer holding an append ack
and ReadGrade = Eventual | Latest | Through of Version
and Version = Version of float
and View<'state> = { State: 'state; AsOf: Version; Behind: float }

[<RequireQualifiedAccess>]
module Projection =
    let define (name: string) (source: string list) (initial: 'state) (apply: 'state -> string -> 'state) : Projection<'state> = notYet

/// A serializable predicate (CEL text) — persisted with the wait, evaluated
/// on relevant state change, never a closure. Registration-time validated.
and Cel = Cel of string

[<RequireQualifiedAccess>]
module Wait =
    /// Park a workflow until a projection's state satisfies the predicate
    /// (immediately, if it already does). Durable: pins no process; resumes
    /// via the wake path; replay serves the recorded resolution.
    let state (projection: Projection<'state>) (predicate: Cel) (timeout: Duration) : Workflow<Result<'state, Timeout>> = notYet

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
