namespace Firegrid.Foundation.Proofs

/// Template 3 — `RebuildEquivalenceLaw` (Packet 0.3b,
/// docs/handoffs/phase0-3b-foundation-consolidation-brief.md).
///
/// One parameterized fold/rebuild-equivalence law. Instantiations provide a
/// `RebuildSurface` record factoring the shared choreography:
///
///   writerOps (populate the source, including any interleaved checkpoint
///              commits the writer's policy performs)
///   -> policy (exercise the checkpoint/trim policy's guard invariants;
///              a no-op for surfaces without one)
///   -> rebuildVsReference (cold rebuilds — including never-checkpointed and
///              across-restart variants — compared against the reference
///              fold-from-zero)
///   -> poison (optional: the decode/apply poison variant)
///
/// The template asserts the uniform law checks, added per the variants the
/// surface declares:
///
///   1. always: rebuild equals the reference fold-from-zero;
///   2. `HasRestartVariant`: rebuild across a restart equals the reference;
///   3. `HasTrimPolicy`: trim never crosses a committed checkpoint and
///      floor-rebuild is equivalent;
///   4. `Poison` present: decode/apply poison fails closed permanently;
///
/// plus trace-op evidence. Surface-specific invariants ride along as named
/// facts: consolidation deletes RESTATEMENTS, never ASSERTIONS — every check
/// of a retired proof maps to a core check here or keeps its original name as
/// a fact check.
module RebuildLaw =
    type RebuildComparison =
        { /// Every cold rebuild equalled its reference fold-from-zero.
          RebuildEqualsReference: bool
          /// The across-restart rebuild equalled the reference (when declared).
          AcrossRestartEquivalent: bool
          /// Surface-specific invariants, checked by name (`FactNames`).
          Facts: (string * bool) list }

    type PolicyReport =
        { /// The checkpoint/trim policy's guard invariants held (when declared).
          PolicySafe: bool
          /// Surface-specific invariants, checked by name (`FactNames`).
          Facts: (string * bool) list }

    type PoisonReport =
        { /// The poison variant failed closed — permanently, never swallowed.
          FailsClosed: bool
          /// Surface-specific invariants, checked by name (`FactNames`).
          Facts: (string * bool) list }

    type RebuildSurface<'world, 'policy> =
        { /// Instantiation name, e.g. "checkpoint-trim".
          Instance: string
          /// ProofOperation name recorded as trace evidence.
          OperationName: string
          /// Declared fact names — each becomes a named check.
          FactNames: string list
          /// Whether the surface proves the across-restart rebuild variant.
          HasRestartVariant: bool
          /// Whether the surface proves a checkpoint/trim policy.
          HasTrimPolicy: bool
          WriterOps: WorkloadContext -> Async<'world>
          Policy: WorkloadContext -> 'world -> Async<'policy * PolicyReport>
          RebuildVsReference: WorkloadContext -> 'world -> 'policy -> Async<RebuildComparison>
          Poison: (WorkloadContext -> 'world -> 'policy -> Async<PoisonReport>) option }

    type RebuildEvidence =
        { Instance: string
          RebuildEqualsReference: bool
          AcrossRestartEquivalent: bool
          PolicySafe: bool
          HasPoisonVariant: bool
          PoisonFailsClosed: bool
          Facts: (string * bool) list }

    let propertyName (instance: string) = "foundation.rebuild-equivalence." + instance

    [<Literal>]
    let completionSpan = "proof.foundation.rebuild-equivalence.completed"

    let workload (surface: RebuildSurface<'world, 'policy>) (ctx: WorkloadContext) : Async<RebuildEvidence> =
        ProofOperation.run
            ctx
            surface.OperationName
            surface.Instance
            { ProofOperationOptions.empty with
                Key = Some surface.OperationName }
            (async {
                let! world = surface.WriterOps ctx
                let! policy, policyReport = surface.Policy ctx world
                let! comparison = surface.RebuildVsReference ctx world policy

                let! poisonReport =
                    match surface.Poison with
                    | Some poison ->
                        async {
                            let! report = poison ctx world policy
                            return Some report
                        }
                    | None -> async { return None }

                let evidence =
                    { Instance = surface.Instance
                      RebuildEqualsReference = comparison.RebuildEqualsReference
                      AcrossRestartEquivalent = comparison.AcrossRestartEquivalent
                      PolicySafe = policyReport.PolicySafe
                      HasPoisonVariant = Option.isSome surface.Poison
                      PoisonFailsClosed =
                        match poisonReport with
                        | Some report -> report.FailsClosed
                        | None -> true
                      Facts =
                        comparison.Facts
                        @ policyReport.Facts
                        @ (match poisonReport with
                           | Some report -> report.Facts
                           | None -> []) }

                do!
                    ctx.EmitSpan
                        completionSpan
                        [ "proof.property", propertyName surface.Instance
                          "rebuild.instance", surface.Instance
                          "rebuild.equals_reference", string evidence.RebuildEqualsReference
                          "rebuild.across_restart_equivalent", string evidence.AcrossRestartEquivalent
                          "rebuild.policy_safe", string evidence.PolicySafe
                          "rebuild.poison_fails_closed", string evidence.PoisonFailsClosed ]

                return evidence
            })

    let private factCheck (name: string) : Check<RebuildEvidence> =
        Expect.workload name (fun evidence ->
            evidence.Facts |> List.exists (fun (fact, holds) -> fact = name && holds))

    /// The uniform law checks for a given variant set — also the verifier set
    /// for negative controls (a known-bad variant must fail one of these).
    let coreChecks (hasRestartVariant: bool) (hasTrimPolicy: bool) (hasPoisonVariant: bool) : Check<RebuildEvidence> list =
        [ yield Expect.workload "rebuild law: rebuild equals the reference fold-from-zero" (fun e ->
              e.RebuildEqualsReference)
          if hasRestartVariant then
              yield
                  Expect.workload "rebuild law: rebuild across a restart equals the reference fold" (fun e ->
                      e.AcrossRestartEquivalent)
          if hasTrimPolicy then
              yield
                  Expect.workload
                      "rebuild law: trim never crosses a committed checkpoint and floor-rebuild is equivalent"
                      (fun e -> e.PolicySafe)
          if hasPoisonVariant then
              yield
                  Expect.workload "rebuild law: decode/apply poison fails closed permanently" (fun e ->
                      e.PoisonFailsClosed) ]

    let checks (surface: RebuildSurface<'world, 'policy>) : Check<RebuildEvidence> list =
        coreChecks surface.HasRestartVariant surface.HasTrimPolicy (Option.isSome surface.Poison)
        @ (surface.FactNames |> List.map factCheck)
        @ [ TraceExpect.spanExists
                (surface.Instance + " completion span emitted")
                completionSpan
                [ "proof.property", propertyName surface.Instance
                  "rebuild.instance", surface.Instance ]
            TraceProof.operation
                (surface.Instance + " operation recorded")
                { TraceOperationMatch.named surface.OperationName with
                    Status = Some "ok"
                    OutputContains = [ "RebuildEqualsReference"; "PoisonFailsClosed" ]
                    Count = Some 1 }
            |> TraceProof.asCheck ]

    let makePropertyWith
        (negativeControls: NegativeControlSpec<RebuildEvidence> list)
        (requiresNegativeControl: bool)
        (surface: RebuildSurface<'world, 'policy>)
        : RunnableProperty =
        Property.make
            (propertyName surface.Instance)
            [ S2Lite "" ]
            (workload surface)
            (checks surface)
            negativeControls
            requiresNegativeControl

    let makeProperty (surface: RebuildSurface<'world, 'policy>) : RunnableProperty =
        makePropertyWith [] false surface
