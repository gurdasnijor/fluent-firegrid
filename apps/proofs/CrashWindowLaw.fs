namespace Firegrid.Foundation.Proofs

/// Template 2 — `CrashWindowLaw` (Packet 0.3b,
/// docs/handoffs/phase0-3b-foundation-consolidation-brief.md).
///
/// One parameterized at-most-once-under-crash law over the dangerous window:
/// an effect COMMITTED (journaled) but not yet DISPATCHED when the host dies.
/// Instantiations provide a `CrashWindowSurface` record factoring the shared
/// choreography:
///
///   seed (commit the effect, stop inside the window)
///   -> crash (abandon the seeded host's fence)
///   -> recover (fresh host/fence resumes from the journal)
///   -> observe
///
/// The template asserts the four uniform law checks:
///
///   1. the window is real — at the crash the effect was committed but its
///      dispatch had not happened;
///   2. nothing lost — the committed effect lands after recovery;
///   3. nothing duplicated — the effect is exactly-once-effective;
///   4. redundant recovery is idempotent;
///
/// plus trace-op evidence. Surface-specific invariants ride along as named
/// facts: consolidation deletes RESTATEMENTS, never ASSERTIONS — every check
/// of a retired proof maps to a core check here or keeps its original name as
/// a fact check. White-box crash-window seeding is legitimate here (the
/// black-box corpus laws cannot force a kill between commit and dispatch) and
/// lives declared in this harness tree, never in `src/`.
module CrashWindowLaw =
    type CrashWindowObservation =
        { /// The seeded state proved the commit landed without its dispatch.
          WindowEstablished: bool
          /// The committed effect landed after recovery (nothing lost).
          NothingLost: bool
          /// The effect landed exactly once (nothing duplicated).
          NothingDuplicated: bool
          /// Extra redundant recoveries changed nothing.
          RedundantRecoveryIdempotent: bool
          /// Surface-specific invariants, checked by name (`FactNames`).
          Facts: (string * bool) list }

    type CrashWindowSurface<'seeded, 'recovered> =
        { /// Instantiation name, e.g. "continue-as-new".
          Instance: string
          /// ProofOperation name recorded as trace evidence.
          OperationName: string
          /// Declared fact names — each becomes a named check.
          FactNames: string list
          Seed: WorkloadContext -> Async<'seeded>
          Crash: WorkloadContext -> 'seeded -> Async<unit>
          Recover: WorkloadContext -> 'seeded -> Async<'recovered>
          Observe: WorkloadContext -> 'seeded -> 'recovered -> Async<CrashWindowObservation> }

    type CrashWindowEvidence =
        { Instance: string
          WindowEstablished: bool
          NothingLost: bool
          NothingDuplicated: bool
          RedundantRecoveryIdempotent: bool
          Facts: (string * bool) list }

    let propertyName (instance: string) = "foundation.crash-window." + instance

    [<Literal>]
    let completionSpan = "proof.foundation.crash-window.completed"

    let workload
        (surface: CrashWindowSurface<'seeded, 'recovered>)
        (ctx: WorkloadContext)
        : Async<CrashWindowEvidence> =
        ProofOperation.run
            ctx
            surface.OperationName
            surface.Instance
            { ProofOperationOptions.empty with
                Key = Some surface.OperationName }
            (async {
                let! seeded = surface.Seed ctx
                do! surface.Crash ctx seeded
                let! recovered = surface.Recover ctx seeded
                let! observation = surface.Observe ctx seeded recovered

                let evidence =
                    { Instance = surface.Instance
                      WindowEstablished = observation.WindowEstablished
                      NothingLost = observation.NothingLost
                      NothingDuplicated = observation.NothingDuplicated
                      RedundantRecoveryIdempotent = observation.RedundantRecoveryIdempotent
                      Facts = observation.Facts }

                do!
                    ctx.EmitSpan
                        completionSpan
                        [ "proof.property", propertyName surface.Instance
                          "crash_window.instance", surface.Instance
                          "crash_window.window_established", string evidence.WindowEstablished
                          "crash_window.nothing_lost", string evidence.NothingLost
                          "crash_window.nothing_duplicated", string evidence.NothingDuplicated
                          "crash_window.redundant_recovery_idempotent",
                          string evidence.RedundantRecoveryIdempotent ]

                return evidence
            })

    let private factCheck (name: string) : Check<CrashWindowEvidence> =
        Expect.workload name (fun evidence ->
            evidence.Facts |> List.exists (fun (fact, holds) -> fact = name && holds))

    /// The four uniform law checks — also the verifier set for negative
    /// controls (a known-bad variant must fail one of these).
    let coreChecks () : Check<CrashWindowEvidence> list =
        [ Expect.workload
              "crash-window law: the window is real — committed but not yet dispatched at the crash"
              (fun e -> e.WindowEstablished)
          Expect.workload "crash-window law: nothing lost — the committed effect lands after recovery" (fun e ->
              e.NothingLost)
          Expect.workload "crash-window law: nothing duplicated — the effect is exactly-once-effective" (fun e ->
              e.NothingDuplicated)
          Expect.workload "crash-window law: redundant recovery is idempotent" (fun e ->
              e.RedundantRecoveryIdempotent) ]

    let checks (surface: CrashWindowSurface<'seeded, 'recovered>) : Check<CrashWindowEvidence> list =
        coreChecks ()
        @ (surface.FactNames |> List.map factCheck)
        @ [ TraceExpect.spanExists
                (surface.Instance + " completion span emitted")
                completionSpan
                [ "proof.property", propertyName surface.Instance
                  "crash_window.instance", surface.Instance ]
            TraceProof.operation
                (surface.Instance + " operation recorded")
                { TraceOperationMatch.named surface.OperationName with
                    Status = Some "ok"
                    OutputContains = [ "WindowEstablished"; "NothingLost"; "NothingDuplicated" ]
                    Count = Some 1 }
            |> TraceProof.asCheck ]

    let makePropertyWith
        (negativeControls: NegativeControlSpec<CrashWindowEvidence> list)
        (requiresNegativeControl: bool)
        (surface: CrashWindowSurface<'seeded, 'recovered>)
        : RunnableProperty =
        Property.make
            (propertyName surface.Instance)
            [ S2Lite "" ]
            (workload surface)
            (checks surface)
            negativeControls
            requiresNegativeControl

    let makeProperty (surface: CrashWindowSurface<'seeded, 'recovered>) : RunnableProperty =
        makePropertyWith [] false surface
