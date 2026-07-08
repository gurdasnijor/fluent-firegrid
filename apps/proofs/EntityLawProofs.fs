/// ═══════════════════════════════════════════════════════════════════════
/// T1 corpus — entity (virtual object) laws (Packet 0.2 re-expression, all
/// three RED: their implementation is G2's packet).
///
/// The virtual-object constraint, driven through the public surface: at
/// most one Decide per key at any moment across ALL hosts (durable inbox
/// admission + epoch fencing), replies computed under exclusive state
/// access, shared reads that never block the writer.
///
/// HISTORY-CHECKING UPGRADE (pre-authorized law-body strengthening, human
/// directive in the Phase 0 statement): each command carries a unique
/// operation id; the counter's fold carries a constant-space hash chain
/// over applied operations (s2-verification's hash-chain state machine as
/// template); verification asserts the observed chains form exactly ONE
/// linear history containing every accepted call exactly once. The corpus's
/// reply-set equality (post-state totals = {1..N}) stays as a secondary
/// check. The laws stay RED.
/// ═══════════════════════════════════════════════════════════════════════
namespace Firegrid.Foundation.Proofs

open Firegrid.Durable

module CounterEntity =
    type Cmd = Add of n: int * op: string
    type Evt = Added of n: int * op: string
    type CounterState = { Total: int; Chain: string }

    /// FNV-1a 32-bit over the previous chain and the applied op id —
    /// constant space, deterministic, and the SAME code runs in the decider
    /// (system side) and the verifier (evidence side).
    let chainStep (chain: string) (op: string) : string =
        let text = chain + "|" + op
        let mutable hash = 0x811c9dc5

        for i in 0 .. text.Length - 1 do
            hash <- hash ^^^ int text.[i]
            hash <- hash * 16777619

        string (uint32 hash)

    /// The counter Decider replies with the POST-state: under serialized
    /// exactly-once processing, N racing `Add 1` calls must produce replies
    /// whose totals are exactly the set {1..N} and whose chains recompute as
    /// one linear history over the accepted op ids.
    let decider: Decider<Cmd, Evt, CounterState, CounterState> =
        { Initial = { Total = 0; Chain = "" }
          Evolve =
            fun state ->
                function
                | Added(n, op) ->
                    { Total = state.Total + n
                      Chain = chainStep state.Chain op }
          Decide =
            fun (Key _) command state ->
                match command with
                | Add(n, op) ->
                    let next =
                        { Total = state.Total + n
                          Chain = chainStep state.Chain op }

                    next, [ Added(n, op) ] }

    let define () = Entity.define "corpus/counter" decider

    /// Child host for the zombie law: hosts the counter until the runner
    /// pauses/resumes/kills this process.
    let childHost () : Async<int> =
        async {
            let! basin = CorpusSupport.childBasin ()
            let counter = define ()
            let! _worker = Worker.run basin (CorpusSupport.childNamespace ()) [ reg counter ]
            return! CorpusSupport.foreverChild ()
        }

/// Linear-history verification over Call replies (the history-checking
/// upgrade's evidence side).
module EntityHistory =
    /// One reply: the op id the caller attached and the post-state the
    /// decider returned.
    type Reply = { Op: string; Total: int; Chain: string }

    /// Violations of "the accepted calls form exactly one linear history":
    /// sorted by serialized position (post-state Total), positions must be
    /// exactly 1..N, every issued op id must appear exactly once, and every
    /// reply's chain must equal the chain recomputed by folding the op ids
    /// in serialized order from the initial state.
    let linearHistoryViolations (issued: string list) (replies: Reply list) : string list =
        let violations = ResizeArray<string>()
        let sorted = replies |> List.sortBy (fun reply -> reply.Total)
        let opsInOrder = sorted |> List.map (fun reply -> reply.Op)
        let issuedSet = Set.ofList issued
        let repliedSet = Set.ofList opsInOrder

        if List.length opsInOrder <> Set.count repliedSet then
            violations.Add "an operation id appears in more than one serialized position"

        if issuedSet <> repliedSet then
            violations.Add(
                sprintf
                    "replied ops differ from issued ops (missing %A, alien %A)"
                    (Set.difference issuedSet repliedSet |> Set.toList)
                    (Set.difference repliedSet issuedSet |> Set.toList)
            )

        let totals = sorted |> List.map (fun reply -> reply.Total)

        if totals <> [ 1 .. List.length replies ] then
            violations.Add(sprintf "serialized positions are not exactly 1..%d: %A" (List.length replies) totals)

        let mutable chain = ""

        for reply in sorted do
            chain <- CounterEntity.chainStep chain reply.Op

            if reply.Chain <> chain then
                violations.Add(sprintf "chain mismatch at position %d (op %s)" reply.Total reply.Op)

        List.ofSeq violations

    /// The chain the whole history should fold to.
    let recomputedFinalChain (replies: Reply list) : string =
        replies
        |> List.sortBy (fun reply -> reply.Total)
        |> List.fold (fun chain reply -> CounterEntity.chainStep chain reply.Op) ""

module EntityLawProofs =
    open CounterEntity

    let private totalsOf (replies: EntityHistory.Reply list) =
        replies |> List.map (fun reply -> reply.Total) |> Set.ofList

    // ── t1.entity-exclusive-serialization ─────────────────────────────────
    // Two hosts race `entity.Call` on ONE key: every call is serialized,
    // effects land exactly once, and the replies are mutually consistent —
    // now additionally required to form exactly one hash-chained linear
    // history.
    type ExclusiveObs =
        { Replies: EntityHistory.Reply list
          Issued: string list
          FinalTotal: int
          FinalChain: string }

    let private trackedCall
        (ctx: WorkloadContext)
        (counter: EntityDef<Cmd, Evt, CounterState, CounterState>)
        (client: Client)
        (key: string)
        (op: string)
        : Async<EntityHistory.Reply> =
        // Each concurrent call records a ProofOperation span keyed by its op
        // id — the operation history is trace evidence, not just in-memory
        // state (ADDITIVE: history-check).
        ProofOperation.run
            ctx
            "t1.entity.call"
            {| Key = key; Op = op |}
            { ProofOperationOptions.empty with
                OperationId = Some op
                Key = Some key }
            (async {
                let! reply = counter.Call client key (Add(1, op))

                return
                    { EntityHistory.Op = op
                      EntityHistory.Total = reply.Total
                      EntityHistory.Chain = reply.Chain }
            })

    let private exclusiveWorkload (ctx: WorkloadContext) : Async<ExclusiveObs> =
        ProofOperation.run
            ctx
            "t1.entity-exclusive-serialization"
            {| Calls = 40 |}
            { ProofOperationOptions.empty with
                Key = Some "entity-exclusive" }
            (async {
                let counter = define ()
                let! basin = CorpusSupport.workloadBasin ctx "t1-entity"
                let ns = "t1-entity"
                let! workerA = Worker.run basin ns [ reg counter ]
                let! workerB = Worker.run basin ns [ reg counter ]
                let clientA = Client.connect basin
                let clientB = Client.connect basin

                let opsA = [ for i in 1..20 -> "op-a-" + string i ]
                let opsB = [ for i in 1..20 -> "op-b-" + string i ]

                let calls =
                    (opsA |> List.map (trackedCall ctx counter clientA "race-key"))
                    @ (opsB |> List.map (trackedCall ctx counter clientB "race-key"))

                let! replies = Async.Parallel calls

                let! finalState = counter.State clientA "race-key" Latest

                do! workerA.Stop()
                do! workerB.Stop()

                return
                    { Replies = List.ofArray replies
                      Issued = opsA @ opsB
                      FinalTotal = finalState.Total
                      FinalChain = finalState.Chain }
            })

    let private exclusiveChecks (v: Verifiers<ExclusiveObs>) : Check<ExclusiveObs> list =
        [ // Corpus assertion, kept: reply-set equality (secondary check).
          LawCheck.equal
              "every Decide observed a distinct serialized state (no lost updates, no double-applies)"
              (fun o -> totalsOf o.Replies)
              (Set.ofList [ 1..40 ])
          LawCheck.equal "effects landed exactly once" (fun o -> o.FinalTotal) 40
          // ADDITIVE: history-check — exactly one linear history, every
          // accepted call exactly once, chains recompute.
          LawCheck.equal
              "the accepted calls form exactly one hash-chained linear history"
              (fun o -> EntityHistory.linearHistoryViolations o.Issued o.Replies)
              []
          LawCheck.holds
              "the final fold's chain equals the recomputed history chain"
              (fun o -> o.FinalChain = EntityHistory.recomputedFinalChain o.Replies)
              (fun o -> sprintf "final=%s recomputed=%s" o.FinalChain (EntityHistory.recomputedFinalChain o.Replies))
          // ADDITIVE: the 40 per-call operations are trace evidence.
          v.Trace.Operation
              "all 40 entity calls recorded ok"
              { TraceOperationMatch.named "t1.entity.call" with
                  Status = Some "ok"
                  Count = Some 40 }
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t1.entity-exclusive-serialization") ]

    let entityExclusiveSerialization =
        let lawProperty =
            property "t1.entity-exclusive-serialization" {
                s2Lite ""
                timeoutMs 180_000
                workload exclusiveWorkload
                verify exclusiveChecks
            }

        proof "t1.entity-exclusive-serialization" {
            describedAs
                "Entity Decide calls serialize exclusively across hosts: no lost or double applies, and (history-checking upgrade) the accepted calls form exactly one hash-chained linear history containing every call exactly once."

            property lawProperty
        }

    // ── t1.entity-zombie-fenced ───────────────────────────────────────────
    // A deposed writer cannot commit: a host paused mid-ownership (SIGSTOP —
    // a zombie that still believes it owns the key) is fenced out when a new
    // host takes over; when it resumes (SIGCONT), any in-flight commit it
    // attempts must NOT double-apply or corrupt the fold. The law holds for
    // every interleaving: totals stay exactly-once and replies consistent
    // across the takeover.
    type ZombieObs =
        { Replies: EntityHistory.Reply list
          Issued: string list
          FinalTotal: int
          FinalChain: string }

    let private zombieWorkload (ctx: WorkloadContext) : Async<ZombieObs> =
        ProofOperation.run
            ctx
            "t1.entity-zombie-fenced"
            {| Phases = 3 |}
            { ProofOperationOptions.empty with
                Key = Some "entity-zombie" }
            (async {
                let counter = define ()
                let! basin = CorpusSupport.workloadBasin ctx "t1-zombie"
                let ns = "t1-zombie"
                let client = Client.connect basin

                let phaseOps (phase: string) =
                    [ for i in 1..10 -> sprintf "op-%s-%d" phase i ]

                let runPhase (ops: string list) =
                    ops |> List.map (trackedCall ctx counter client "z-key") |> Async.Parallel

                // Phase 1 — the child host owns the key.
                let! phase1 = runPhase (phaseOps "p1")

                // Freeze the owner mid-life: it keeps its claim in memory
                // but cannot make progress — a zombie. (Fault-controller
                // SIGSTOP: report-level fault event + pause span.)
                do! ctx.Faults.PauseHost "zombie-host"

                // Phase 2 — a second host must take over and serve.
                let! workerB = Worker.run basin ns [ reg counter ]
                let! phase2 = runPhase (phaseOps "p2")

                // Phase 3 — the zombie thaws and tries to resume; its stale
                // fence must reject anything it still has in flight.
                do! ctx.Faults.ResumeHost "zombie-host"
                let! phase3 = runPhase (phaseOps "p3")

                let! finalState = counter.State client "z-key" Latest

                // The zombie is done: hard-kill through the fault controller
                // (the corpus's bare child.kill, now with report evidence).
                do! WorkloadContext.killHost "zombie-host" ctx
                do! workerB.Stop()

                return
                    { Replies = List.ofArray phase1 @ List.ofArray phase2 @ List.ofArray phase3
                      Issued = phaseOps "p1" @ phaseOps "p2" @ phaseOps "p3"
                      FinalTotal = finalState.Total
                      FinalChain = finalState.Chain }
            })

    let private zombieChecks (v: Verifiers<ZombieObs>) : Check<ZombieObs> list =
        [ // Corpus assertions, kept:
          LawCheck.equal
              "replies stay consistent across the takeover (every total distinct)"
              (fun o -> totalsOf o.Replies)
              (Set.ofList [ 1..30 ])
          LawCheck.equal "the deposed writer committed nothing twice" (fun o -> o.FinalTotal) 30
          // ADDITIVE: history-check across the takeover.
          LawCheck.equal
              "the accepted calls form exactly one hash-chained linear history across the takeover"
              (fun o -> EntityHistory.linearHistoryViolations o.Issued o.Replies)
              []
          // ADDITIVE: the zombie lifecycle is report + trace evidence.
          v.Fault.HostPauseReported "zombie-host"
          v.Fault.HostResumeReported "zombie-host"
          v.Fault.HostKillReported "zombie-host"
          v.Host.Started "zombie-host"
          v.Fault.HostPaused "zombie-host"
          v.Fault.HostResumed "zombie-host"
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t1.entity-zombie-fenced") ]

    let entityZombieFenced =
        let lawProperty =
            property "t1.entity-zombie-fenced" {
                s2Lite ""
                processHost (CorpusSupport.childHostSpec "zombie-host" "zombie-host" "t1-zombie" "t1-zombie")
                timeoutMs 240_000
                workload zombieWorkload
                verify zombieChecks
            }

        proof "t1.entity-zombie-fenced" {
            describedAs
                "A paused (SIGSTOP) deposed owner is fenced on resume (SIGCONT): it never double-applies, totals stay exactly-once across the takeover, and the accepted calls form one linear history."

            property lawProperty
        }

    // ── t1.entity-shared-read-nonblocking ─────────────────────────────────
    // SHARED reads run concurrently with the exclusive writer and never
    // block behind it: Eventual reads complete WHILE a write burst is in
    // flight, every observed state is a valid fold prefix, and a Latest read
    // after the burst is exact.
    type SharedReadObs =
        { ReadsDuringBurst: int
          ObservedTotals: int list
          FinalTotal: int }

    let private sharedReadWorkload (ctx: WorkloadContext) : Async<SharedReadObs> =
        ProofOperation.run
            ctx
            "t1.entity-shared-read-nonblocking"
            {| Burst = 30 |}
            { ProofOperationOptions.empty with
                Key = Some "entity-shared" }
            (async {
                let counter = define ()
                let! basin = CorpusSupport.workloadBasin ctx "t1-shared"
                let! worker = Worker.run basin "t1-shared" [ reg counter ]
                let client = Client.connect basin

                let mutable burstDone = false
                let mutable readsDuringBurst = 0
                let observed = ResizeArray<int>()

                let burst =
                    async {
                        for i in 1..30 do
                            let! _ = counter.Call client "s-key" (Add(1, "op-s-" + string i))
                            ()

                        burstDone <- true
                    }

                let reader =
                    async {
                        while not burstDone do
                            let! view = counter.State client "s-key" Eventual
                            observed.Add view.Total

                            if not burstDone then
                                readsDuringBurst <- readsDuringBurst + 1

                            do! CorpusNode.sleep 50
                    }

                do! Async.Parallel [ burst; reader ] |> Async.Ignore

                let! finalState = counter.State client "s-key" Latest
                do! worker.Stop()

                return
                    { ReadsDuringBurst = readsDuringBurst
                      ObservedTotals = List.ofSeq observed
                      FinalTotal = finalState.Total }
            })

    let private sharedReadChecks (v: Verifiers<SharedReadObs>) : Check<SharedReadObs> list =
        [ LawCheck.holds
              "shared reads completed WHILE the writer was busy (never blocked behind it)"
              (fun o -> o.ReadsDuringBurst > 0)
              (fun o -> sprintf "reads during burst: %d" o.ReadsDuringBurst)
          LawCheck.holds
              "every shared read is a valid fold prefix"
              (fun o -> o.ObservedTotals |> List.forall (fun total -> total >= 0 && total <= 30))
              (fun o -> sprintf "observed totals: %A" o.ObservedTotals)
          LawCheck.equal "Latest read after the burst is exact" (fun o -> o.FinalTotal) 30
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t1.entity-shared-read-nonblocking") ]

    let entitySharedReadNonblocking =
        let lawProperty =
            property "t1.entity-shared-read-nonblocking" {
                s2Lite ""
                timeoutMs 180_000
                workload sharedReadWorkload
                verify sharedReadChecks
            }

        proof "t1.entity-shared-read-nonblocking" {
            describedAs
                "Shared reads run concurrent with the exclusive writer: Eventual reads complete during a write burst, every observed state is a valid fold prefix, and a Latest read after the burst is exact."

            property lawProperty
        }
