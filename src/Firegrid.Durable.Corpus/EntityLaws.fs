/// ═══════════════════════════════════════════════════════════════════════
/// T1 red corpus — entity (virtual object) laws.
///
/// The virtual-object constraint, driven through the public surface: at most
/// one Decide per key at any moment across ALL hosts (durable inbox
/// admission + epoch fencing), replies computed under exclusive state
/// access, shared reads that never block the writer.
///
/// The counter Decider replies with the POST-state: under serialized
/// exactly-once processing, N racing `Add 1` calls must produce replies
/// whose totals are exactly the set {1..N} (every Decide observed a distinct
/// serialized state — no lost updates, no double-applies), and the final
/// fold must equal N.
/// ═══════════════════════════════════════════════════════════════════════
namespace Firegrid.Durable.Corpus

open Firegrid.Durable

module CounterEntity =
    type Cmd = Add of n: int
    type Evt = Added of n: int
    type CounterState = { Total: int }

    let decider: Decider<Cmd, Evt, CounterState, CounterState> =
        { Initial = { Total = 0 }
          Evolve =
            fun state ->
                function
                | Added n -> { Total = state.Total + n }
          Decide =
            fun (Key _) command state ->
                match command with
                | Add n -> { Total = state.Total + n }, [ Added n ] }

    let define () = Entity.define "corpus/counter" decider

    /// Child host for the zombie law: hosts the counter until the parent
    /// stops/kills this process.
    let childHost () : Async<int> =
        async {
            let basin = Harness.childBasin ()
            let counter = define ()
            let! _worker = Worker.run basin (Node.env "T1C_NS") [ reg counter ]
            return! Harness.foreverChild ()
        }

module EntityLaws =
    open CounterEntity

    let private totalsOf (replies: CounterState list) =
        replies |> List.map (fun reply -> reply.Total) |> Set.ofList

    // ── t1.entity-exclusive-serialization ─────────────────────────────────
    // Two hosts race `entity.Call` on ONE key: every call is serialized,
    // effects land exactly once, and the replies are mutually consistent.
    let entityExclusiveSerialization: Law =
        { Id = "t1.entity-exclusive-serialization"
          TimeoutMs = 180_000
          Run =
            fun () ->
                async {
                    let counter = define ()

                    do!
                        Harness.withEnv "entity-exclusive" (fun env ->
                            async {
                                let ns = "t1-entity"
                                let! workerA = Worker.run env.Basin ns [ reg counter ]
                                let! workerB = Worker.run env.Basin ns [ reg counter ]
                                let clientA = Client.connect env.Basin
                                let clientB = Client.connect env.Basin

                                let calls =
                                    [ for _ in 1..20 -> counter.Call clientA "race-key" (Add 1) ]
                                    @ [ for _ in 1..20 -> counter.Call clientB "race-key" (Add 1) ]

                                let! replies = Async.Parallel calls

                                Expect.equal
                                    "every Decide observed a distinct serialized state (no lost updates, no double-applies)"
                                    (Set.ofList [ 1..40 ])
                                    (totalsOf (List.ofArray replies))

                                let! finalState = counter.State clientA "race-key" Latest
                                Expect.equal "effects landed exactly once" { Total = 40 } finalState

                                do! workerA.Stop ()
                                do! workerB.Stop ()
                            })
                } }

    // ── t1.entity-zombie-fenced ───────────────────────────────────────────
    // A deposed writer cannot commit: a host paused mid-ownership (SIGSTOP —
    // a zombie that still believes it owns the key) is fenced out when a new
    // host takes over; when it resumes (SIGCONT), any in-flight commit it
    // attempts must NOT double-apply or corrupt the fold. The law holds for
    // every interleaving: totals stay exactly-once and replies consistent
    // across the takeover.
    let entityZombieFenced: Law =
        { Id = "t1.entity-zombie-fenced"
          TimeoutMs = 240_000
          Run =
            fun () ->
                async {
                    let counter = define ()
                    let scratch = Harness.scratchFor "entity-zombie"

                    do!
                        Harness.withEnv "entity-zombie" (fun env ->
                            async {
                                let ns = "t1-zombie"
                                let zombie = Harness.spawnChildHost env "zombie-host" ns scratch
                                let client = Client.connect env.Basin

                                // Phase 1 — the child host owns the key.
                                let! phase1 = Async.Parallel [ for _ in 1..10 -> counter.Call client "z-key" (Add 1) ]

                                // Freeze the owner mid-life: it keeps its claim in
                                // memory but cannot make progress — a zombie.
                                zombie.kill "SIGSTOP" |> ignore

                                // Phase 2 — a second host must take over and serve.
                                let! workerB = Worker.run env.Basin ns [ reg counter ]
                                let! phase2 = Async.Parallel [ for _ in 1..10 -> counter.Call client "z-key" (Add 1) ]

                                // Phase 3 — the zombie thaws and tries to resume; its
                                // stale fence must reject anything it still has in flight.
                                zombie.kill "SIGCONT" |> ignore
                                let! phase3 = Async.Parallel [ for _ in 1..10 -> counter.Call client "z-key" (Add 1) ]

                                let replies = List.ofArray phase1 @ List.ofArray phase2 @ List.ofArray phase3

                                Expect.equal
                                    "replies stay consistent across the takeover (every total distinct)"
                                    (Set.ofList [ 1..30 ])
                                    (totalsOf replies)

                                let! finalState = counter.State client "z-key" Latest
                                Expect.equal "the deposed writer committed nothing twice" { Total = 30 } finalState

                                zombie.kill "SIGKILL" |> ignore
                                do! workerB.Stop ()
                            })
                } }

    // ── t1.entity-shared-read-nonblocking ─────────────────────────────────
    // SHARED reads run concurrently with the exclusive writer and never
    // block behind it: Eventual reads complete WHILE a write burst is in
    // flight, every observed state is a valid fold prefix, and a Latest read
    // after the burst is exact.
    let entitySharedReadNonblocking: Law =
        { Id = "t1.entity-shared-read-nonblocking"
          TimeoutMs = 180_000
          Run =
            fun () ->
                async {
                    let counter = define ()

                    do!
                        Harness.withEnv "entity-shared" (fun env ->
                            async {
                                let! worker = Worker.run env.Basin "t1-shared" [ reg counter ]
                                let client = Client.connect env.Basin

                                let mutable burstDone = false
                                let mutable readsDuringBurst = 0
                                let observed = ResizeArray<int>()

                                let burst =
                                    async {
                                        for _ in 1..30 do
                                            let! _ = counter.Call client "s-key" (Add 1)
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

                                            do! Node.sleep 50
                                    }

                                do! Async.Parallel [ burst; reader ] |> Async.Ignore

                                Expect.isTrue
                                    "shared reads completed WHILE the writer was busy (never blocked behind it)"
                                    (readsDuringBurst > 0)

                                for total in List.ofSeq observed do
                                    Expect.isTrue
                                        (sprintf "every shared read is a valid fold prefix (saw %d)" total)
                                        (total >= 0 && total <= 30)

                                let! finalState = counter.State client "s-key" Latest
                                Expect.equal "Latest read after the burst is exact" { Total = 30 } finalState
                                do! worker.Stop ()
                            })
                } }
