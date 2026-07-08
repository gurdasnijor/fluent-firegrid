namespace Firegrid.Foundation.Proofs

/// Template 1 — `FencingLaw` (Packet 0.3b,
/// docs/handoffs/phase0-3b-foundation-consolidation-brief.md).
///
/// One parameterized single-writer/fencing law, instantiated per surface by a
/// `FencingSurface` record factoring the shared choreography:
///
///   setup -> ownerAct -> supersede -> staleAttempt -> observe
///
/// The template asserts the four uniform law checks over the evidence every
/// instantiation produces:
///
///   1. exactly one winner commits;
///   2. the stale/losing attempt fails typed (Deposed / Raced / Regressed /
///      rejected — the surface declares its expected fence label);
///   3. the stale attempt commits nothing observable;
///   4. the post-state is consistent with the single winner;
///
/// plus trace-op evidence (a completion span and the recorded operation).
/// Surface-specific invariants ride along as named facts: consolidation
/// deletes RESTATEMENTS, never ASSERTIONS — every check of a retired proof
/// maps to a core check here or keeps its original name as a fact check.
///
/// Two phase notes the instantiations rely on:
/// - For CAS-election surfaces (checkpoint-commit) and mid-drive takeover
///   surfaces (wake-claim) the supersession is atomic with the owner's act —
///   `Supersede` is then a documented no-op and the election happens inside
///   `OwnerAct`.
/// - A negative-control variant is just another `FencingSurface` whose
///   workload feeds the same core checks (`coreChecks`), e.g. one that skips
///   the supersede so the stale attempt is allowed to commit.
module FencingLaw =
    /// How the stale/losing attempt concluded.
    type StaleOutcome =
        /// Failed typed — the fence label observed (e.g. "Deposed", "Regressed").
        | Fenced of label: string
        /// The stale attempt landed — a fencing violation (negative controls).
        | CommittedAnyway of detail: string
        /// The attempt neither committed nor failed typed.
        | Indeterminate of detail: string

    /// What the instantiation observed after the full choreography.
    type FencingObservation =
        { /// How many competing attempts committed — the law requires exactly 1.
          WinnerCommits: int
          /// Whether any stale effect is visible in the durable post-state.
          StaleEffectVisible: bool
          /// Whether the durable post-state matches the single winner's writes.
          PostStateConsistent: bool
          /// Surface-specific invariants, checked by name (`FactNames`).
          Facts: (string * bool) list }

    type FencingSurface<'world, 'owner, 'super> =
        { /// Instantiation name, e.g. "checkpoint-commit".
          Instance: string
          /// ProofOperation name recorded as trace evidence.
          OperationName: string
          /// The typed fence label the stale attempt must fail with.
          ExpectedFence: string
          /// Declared fact names — each becomes a named check.
          FactNames: string list
          Setup: WorkloadContext -> Async<'world>
          OwnerAct: WorkloadContext -> 'world -> Async<'owner>
          Supersede: WorkloadContext -> 'world -> 'owner -> Async<'super>
          StaleAttempt: WorkloadContext -> 'world -> 'owner -> 'super -> Async<StaleOutcome>
          Observe: WorkloadContext -> 'world -> 'owner -> 'super -> StaleOutcome -> Async<FencingObservation> }

    type FencingEvidence =
        { Instance: string
          ExpectedFence: string
          WinnerCommits: int
          StaleFenced: bool
          StaleLabel: string
          StaleEffectVisible: bool
          PostStateConsistent: bool
          Facts: (string * bool) list }

    let propertyName (instance: string) = "foundation.fencing." + instance

    [<Literal>]
    let completionSpan = "proof.foundation.fencing.completed"

    let workload (surface: FencingSurface<'world, 'owner, 'super>) (ctx: WorkloadContext) : Async<FencingEvidence> =
        ProofOperation.run
            ctx
            surface.OperationName
            surface.Instance
            { ProofOperationOptions.empty with
                Key = Some surface.OperationName }
            (async {
                let! world = surface.Setup ctx
                let! owner = surface.OwnerAct ctx world
                let! super = surface.Supersede ctx world owner
                let! stale = surface.StaleAttempt ctx world owner super
                let! observation = surface.Observe ctx world owner super stale

                let staleFenced, staleLabel =
                    match stale with
                    | Fenced label -> true, label
                    | CommittedAnyway detail -> false, "committed:" + detail
                    | Indeterminate detail -> false, "indeterminate:" + detail

                let evidence =
                    { Instance = surface.Instance
                      ExpectedFence = surface.ExpectedFence
                      WinnerCommits = observation.WinnerCommits
                      StaleFenced = staleFenced
                      StaleLabel = staleLabel
                      StaleEffectVisible = observation.StaleEffectVisible
                      PostStateConsistent = observation.PostStateConsistent
                      Facts = observation.Facts }

                do!
                    ctx.EmitSpan
                        completionSpan
                        [ "proof.property", propertyName surface.Instance
                          "fencing.instance", surface.Instance
                          "fencing.winner_commits", string evidence.WinnerCommits
                          "fencing.stale_fenced", string evidence.StaleFenced
                          "fencing.stale_label", evidence.StaleLabel
                          "fencing.stale_effect_visible", string evidence.StaleEffectVisible
                          "fencing.post_state_consistent", string evidence.PostStateConsistent ]

                return evidence
            })

    let private factCheck (name: string) : Check<FencingEvidence> =
        Expect.workload name (fun evidence ->
            evidence.Facts |> List.exists (fun (fact, holds) -> fact = name && holds))

    /// The four uniform law checks — also the verifier set for negative
    /// controls (a known-bad variant must fail one of these).
    let coreChecks () : Check<FencingEvidence> list =
        [ Expect.workload "fencing law: exactly one winner commits" (fun e -> e.WinnerCommits = 1)
          Expect.workload "fencing law: the stale attempt fails typed with the expected fence" (fun e ->
              e.StaleFenced && e.StaleLabel = e.ExpectedFence)
          Expect.workload "fencing law: the stale attempt commits nothing observable" (fun e ->
              not e.StaleEffectVisible)
          Expect.workload "fencing law: the post-state is consistent with the single winner" (fun e ->
              e.PostStateConsistent) ]

    let checks (surface: FencingSurface<'world, 'owner, 'super>) : Check<FencingEvidence> list =
        coreChecks ()
        @ (surface.FactNames |> List.map factCheck)
        @ [ TraceExpect.spanExists
                (surface.Instance + " completion span emitted")
                completionSpan
                [ "proof.property", propertyName surface.Instance
                  "fencing.instance", surface.Instance ]
            TraceProof.operation
                (surface.Instance + " operation recorded")
                { TraceOperationMatch.named surface.OperationName with
                    Status = Some "ok"
                    OutputContains = [ "WinnerCommits"; "StaleFenced"; "PostStateConsistent" ]
                    Count = Some 1 }
            |> TraceProof.asCheck ]

    let makePropertyWith
        (negativeControls: NegativeControlSpec<FencingEvidence> list)
        (requiresNegativeControl: bool)
        (surface: FencingSurface<'world, 'owner, 'super>)
        : RunnableProperty =
        Property.make
            (propertyName surface.Instance)
            [ S2Lite "" ]
            (workload surface)
            (checks surface)
            negativeControls
            requiresNegativeControl

    let makeProperty (surface: FencingSurface<'world, 'owner, 'super>) : RunnableProperty =
        makePropertyWith [] false surface
