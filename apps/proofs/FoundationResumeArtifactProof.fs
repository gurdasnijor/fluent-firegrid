namespace Firegrid.Foundation.Proofs

open Firegrid.Log
open Firegrid.Foundation
open Firegrid.Store

/// B4 — the MS-C5 fenced native-resume-artifact-store proof obligation, driven
/// entirely through the public `ResumeArtifactStore` surface (a domain binding of
/// B1's `Authority` (I5) in the FencedOwner regime; no second authority). No deep
/// imports, no proof-only branches in production code.
///
/// - `session.resume-artifact-fenced` — the native resume artifact (e.g. a Claude
///   session id) is written under the session fence; a deposed writer's `store`
///   fails `Deposed` (it computes but cannot commit), closing agent-ui's
///   last-writer-wins session-store race. The proof pins the **claim-then-read**
///   interleaving: a resuming holder that `openWriter`s (rotating the epoch,
///   deposing any stale writer) *before* its re-hydration `read` cannot then
///   observe a stale writer's late `store` — that store is fenced `Deposed`, so
///   the "stale-store-after-new-holder-read-without-claim" fork is impossible. The
///   contrast is proven positively: an authority-free `read` alone does NOT rotate
///   the epoch (the old writer stays live and can still `store`), so it is the
///   claim-first order — not the read — that fences the stale writer out.
///
/// Extends the `store.object-live-fencing` live-deposed-owner technique (two S2
/// clients over one `s2Lite`, the stale writer kept live to model a
/// still-computing owner) to the resume register.
module FoundationResumeArtifactProof =

    // ---- Result record ----------------------------------------------------

    type ResumeArtifactProofResult =
        { WriterOpened: bool
          FirstStoreVisibleToObserver: bool
          BareReadDoesNotFence: bool
          ClaimThenReadRehydrates: bool
          StaleStoreFencedDeposed: bool
          RehydratedStateNotForked: bool
          LastStoreUnderFenceWins: bool
          BareTakeoverDoesNotShadow: bool
          PayloadRoundTripsD2FieldForField: bool }

    // ---- Helpers over the public surface ----------------------------------

    let private openOk
        (basin: S2.Basin)
        (session: Turn.SessionId)
        (holder: Authority.HolderId)
        : Async<ResumeArtifactStore.Writer> =
        async {
            match! ResumeArtifactStore.openWriter basin session holder with
            | Ok writer -> return writer
            | Error _ -> return failwith "resume-artifact: openWriter failed unexpectedly"
        }

    let private readOk (basin: S2.Basin) (session: Turn.SessionId) : Async<ResumeArtifact option> =
        async {
            match! ResumeArtifactStore.read basin session with
            | Ok artifact -> return artifact
            | Error _ -> return failwith "resume-artifact: read failed unexpectedly"
        }

    // ---- session.resume-artifact-fenced -----------------------------------

    let private resumeArtifactWorkload ctx =
        ProofOperation.run
            ctx
            "session.resume-artifact.fenced"
            "session-resume-artifact-fenced"
            { ProofOperationOptions.empty with
                Key = Some "session-resume-artifact-fenced" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let endpoint =
                    match s2.Endpoint with
                    | Some value -> value
                    | None -> failwith "resume-artifact requires an s2 endpoint (declare s2Lite)"

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "resume-artifact-" + suffix

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName

                // Resuming host: a fresh S2 connection to the same durable store,
                // modelling a separate process that takes over the session. The
                // original writer host-a is kept live across `basin` to model a
                // still-computing (stale) owner — the object-live-fencing lineage.
                let resumeClient =
                    S2.connectWith
                        { S2.ConnectOptions.create "s2-lite-resume-artifact-resume" with
                            AccountEndpoint = Some endpoint
                            BasinEndpoint = Some endpoint }

                let resumeBasin = resumeClient |> S2.basin basinName

                let session = Turn.SessionId("sess-" + suffix)
                let turn1 = Turn.TurnId "turn-1"

                let holderA = Authority.HolderId "host-a"
                let holderB = Authority.HolderId "host-b"
                let holderC = Authority.HolderId "host-c"

                let artifactA1 =
                    { Harness = "claude-agent-sdk"
                      Version = 1L
                      NativeType = Some "resume"
                      Payload = "claude-session-A1"
                      Turn = Some turn1 }

                let artifactA2 = { artifactA1 with Payload = "claude-session-A2" }

                let artifactStale =
                    { artifactA1 with Payload = "claude-session-STALE-FORK" }

                let artifactB =
                    { Harness = "claude-agent-sdk"
                      Version = 2L
                      NativeType = Some "resume"
                      Payload = "claude-session-B1"
                      Turn = Some turn1 }

                // Host A becomes the live writer and stores its resume artifact.
                let! writerA = openOk basin session holderA
                let! storeA1 = ResumeArtifactStore.store writerA artifactA1

                // Observer/UI reads authority-free (no claim). This must NOT fence A.
                let! obs1 = readOk basin session

                // A bare read did not rotate the epoch: host A is still live and can
                // store again. This is the "read-without-claim" branch — a read alone
                // is not a fence.
                let! storeA2 = ResumeArtifactStore.store writerA artifactA2
                let! obs2 = readOk basin session

                // Resuming host B follows CLAIM-THEN-READ: `openWriter` FIRST — which
                // rotates the register epoch and deposes A …
                let! writerB = openOk resumeBasin session holderB
                // … THEN its re-hydration read (re-hydrates A's last valid artifact).
                let! bRead = readOk resumeBasin session

                // The interleaving the law pins: A is a still-live stale process; its
                // LATE store lands AFTER B's claim-then-read. It is fenced `Deposed`,
                // so it cannot fork the state B re-hydrated.
                let! staleStore = ResumeArtifactStore.store writerA artifactStale
                let! afterStale = readOk resumeBasin session

                // last-store-under-fence-wins: B (the sole live holder) replaces the
                // artifact.
                let! storeB = ResumeArtifactStore.store writerB artifactB
                let! bFinal = readOk resumeBasin session

                // A bare takeover (claim, no subsequent store) does not shadow the
                // current artifact — the fence rotation is an S2 command record that
                // `read` skips, so the latest *artifact* still wins.
                let! _writerC = openOk resumeBasin session holderC
                let! cRead = readOk resumeBasin session

                let payloadRoundTrips =
                    match bFinal with
                    | Some a ->
                        a.Harness = artifactB.Harness
                        && a.Version = artifactB.Version
                        && a.NativeType = artifactB.NativeType
                        && a.Payload = artifactB.Payload
                        && a.Turn = artifactB.Turn
                    | None -> false

                let result =
                    { WriterOpened = (storeA1 = Ok())
                      FirstStoreVisibleToObserver = (obs1 = Some artifactA1)
                      BareReadDoesNotFence = (storeA2 = Ok() && obs2 = Some artifactA2)
                      ClaimThenReadRehydrates = (bRead = Some artifactA2)
                      StaleStoreFencedDeposed = (staleStore = Error ResumeArtifactStore.StoreError.Deposed)
                      RehydratedStateNotForked = (afterStale = Some artifactA2)
                      LastStoreUnderFenceWins = (storeB = Ok() && bFinal = Some artifactB)
                      BareTakeoverDoesNotShadow = (cRead = Some artifactB)
                      PayloadRoundTripsD2FieldForField = payloadRoundTrips }

                do!
                    ctx.EmitSpan
                        "proof.session.resume-artifact-fenced.completed"
                        [ "proof.property", "session.resume-artifact-fenced"
                          "resume.stale_store_deposed", string result.StaleStoreFencedDeposed
                          "resume.rehydrated_not_forked", string result.RehydratedStateNotForked
                          "resume.bare_read_not_a_fence", string result.BareReadDoesNotFence ]

                return result
            })

    let private resumeArtifactProperty =
        property "session.resume-artifact-fenced" {
            s2Lite ""
            workload resumeArtifactWorkload

            verify (fun v ->
                [ v.Expect.Workload "host-a opens the fenced writer and stores its resume artifact" (fun result ->
                      result.WriterOpened)
                  v.Expect.Workload "an authority-free observer read returns the stored artifact" (fun result ->
                      result.FirstStoreVisibleToObserver)
                  v.Expect.Workload "a bare read does not fence the writer — host-a stays live and can store again" (fun result ->
                      result.BareReadDoesNotFence)
                  v.Expect.Workload "the resuming holder claims-then-reads, re-hydrating the last valid artifact" (fun result ->
                      result.ClaimThenReadRehydrates)
                  v.Expect.Workload "the stale writer's late store is fenced Deposed after the takeover" (fun result ->
                      result.StaleStoreFencedDeposed)
                  v.Expect.Workload "the stale store cannot fork what the resuming holder re-hydrated" (fun result ->
                      result.RehydratedStateNotForked)
                  v.Expect.Workload "last-store-under-fence-wins: the live holder replaces the artifact" (fun result ->
                      result.LastStoreUnderFenceWins)
                  v.Expect.Workload "a bare takeover (claim, no store) does not shadow the current artifact" (fun result ->
                      result.BareTakeoverDoesNotShadow)
                  v.Expect.Workload "the artifact round-trips D2's harness/version/payload field-for-field" (fun result ->
                      result.PayloadRoundTripsD2FieldForField)
                  v.Trace.SpanExists
                      "resume-artifact completion span emitted"
                      "proof.session.resume-artifact-fenced.completed"
                      [ "proof.property", "session.resume-artifact-fenced" ]
                  v.Trace.Operation
                      "resume-artifact operation recorded"
                      ({ TraceOperationMatch.named "session.resume-artifact.fenced" with
                          Status = Some "ok"
                          OutputContains = [ "StaleStoreFencedDeposed"; "RehydratedStateNotForked" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "session.resume-artifact" {
            describedAs
                "MS-C5 fenced native-resume-artifact store over the public ResumeArtifactStore surface: the harness resume artifact is written under a per-session Authority fence (last-store-under-fence-wins), a deposed writer's store fails Deposed, and the claim-then-read convention makes the stale-store-after-new-holder-read fork impossible."

            property resumeArtifactProperty
        }
