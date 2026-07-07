namespace Firegrid.Store

open Firegrid.Log
open Firegrid.Foundation
open Firegrid.Foundation.SubjectHistory

/// MS-C5 — the fenced native-resume-artifact store (WP B4), a **domain binding of
/// B1's `Authority` (I5) in the FencedOwner regime** over one per-session
/// register, per
/// `docs/canon/architecture/fluent/authority-and-actors.md`. It is the sibling of
/// `SessionLifecycle` (the lifecycle-authority slice of MS-C5); the two are
/// independent and both consume I5 only.
///
/// The store is a **per-session fenced last-write-wins cell** of the harness-native
/// resume artifact (the "Native resume artifact" of the SDD Vocabulary: harness-owned
/// resume state, e.g. a Claude session id). Only the live session writer stores or
/// replaces it; a different identity deposes the prior writer by epoch increment, so
/// a deposed writer's `store` fails `Deposed` — exactly the DurableLog `Deposed` /
/// `store.object-live-fencing` law applied to the resume register. This is the
/// executable close of agent-ui's last-writer-wins session-store race: a stale
/// process can no longer clobber the live writer's artifact.
///
/// It invents **no second authority**: `openWriter` = `Authority.claim`,
/// `store` = `Authority.commit` (last-store-under-fence-wins), `read` is an
/// authority-free tailing reader. Sans-IO: subject derivation, the artifact JSON
/// codec, and latest-record selection are the pure core; `openWriter`/`store`/`read`
/// are the `Async` shells over the injected `S2.Basin`. EffSharp-free, Fable-safe.
///
/// The register's subject is session-derived (`sessions/{sessionId}/resume`, never
/// random) and its epoch rotates on takeover, so the fence is genuinely *per
/// session*. Coordinating the register claim with B3's session-log claim is the
/// shared MS-C5 lifecycle convention (stated verbatim on both sides): *"One session
/// holder identity claims both the session log and the resume register; a takeover
/// `start` claims the resume register before any re-hydration read, so a stale
/// writer's late `store` is fenced out before it can fork harness state."* That
/// **claim-then-read** convention is what the `session.resume-artifact-fenced` proof
/// enforces (interleaving included).
type ResumeArtifact =
    { /// Emitting harness id — pairs D2 `NativeResumeArtifact.harness` / `firegrid/native.harness`.
      Harness: string
      /// Harness-owned artifact version — pairs D2 `NativeResumeArtifact.version` (round-trip fidelity).
      Version: int64
      /// Additive: pairs `firegrid/native.nativeType` (adapter-side enrichment).
      NativeType: string option
      /// Opaque resume blob (D2 `payload: unknown`, serialized; e.g. a Claude session id); never parsed.
      Payload: string
      /// Additive provenance: the turn whose completion produced it, if known.
      Turn: Turn.TurnId option }

module ResumeArtifactStore =

    // ---- Derived subject (never random) -----------------------------------

    let private subjectName (SubjectId value) = value

    /// Derived (never random) subject for a session's resume register:
    /// `sessions/{sessionId}/resume`. Consumers pass `SessionId`, not names — the
    /// name is deterministic by design and never leaks.
    let subject (Turn.SessionId session) : SubjectId =
        SubjectId(String.concat "/" [ "sessions"; session; "resume" ])

    // ---- Artifact JSON codec (private; `JsJson`, as `Turn` uses) ----------
    // int64 is string-encoded and options are null-encoded for Fable safety, the
    // same convention `Turn` / `SessionLifecycle` already emit. The `Payload` is
    // stored verbatim — the codec never parses it (opaque, D2-owned).

    let private codec: SubjectHistory.Codec<ResumeArtifact> =
        { Encode =
            fun artifact ->
                let turnValue = artifact.Turn |> Option.map (fun (Turn.TurnId t) -> t)

                JsJson.stringify
                    {| harness = artifact.Harness
                       version = string artifact.Version
                       nativeType = defaultArg artifact.NativeType null
                       payload = artifact.Payload
                       turn = defaultArg turnValue null |}
          Decode =
            fun body ->
                try
                    let parsed = JsJson.parse<obj> body

                    Ok
                        { Harness = JsJson.stringProp "harness" parsed
                          Version = System.Int64.Parse(JsJson.stringProp "version" parsed)
                          NativeType = JsJson.optionalStringProp "nativeType" parsed
                          Payload = JsJson.stringProp "payload" parsed
                          Turn = JsJson.optionalStringProp "turn" parsed |> Option.map Turn.TurnId }
                with error ->
                    Error error.Message }

    // ---- Shell errors + the fenced writer handle --------------------------

    /// A fenced writer over one session's resume register — wraps an
    /// `Authority.Holder` (FencedOwner, I5). Only the live holder stores; a
    /// different identity deposes it (epoch increment). Abstract.
    type Writer = private { Holder: Authority.Holder<ResumeArtifact> }

    [<RequireQualifiedAccess>]
    type OpenError =
        /// Pass-through of `Authority.claim` (incl. `Sealed`); no narrowing —
        /// lane symmetry with B3's `StartError.Claim`.
        | Claim of Authority.ClaimError
        | Failed of S2Errors.S2Failure

    [<RequireQualifiedAccess>]
    type StoreError =
        /// A newer epoch took the session's write authority.
        | Deposed
        | Failed of S2Errors.S2Failure

    [<RequireQualifiedAccess>]
    type ReadError = Failed of S2Errors.S2Failure

    // ---- openWriter / store (I/O shells over the injected Basin) ----------

    /// Claim the fenced writer for a session under `holderId` (`Authority.claim`
    /// bound to the resume subject): the same identity re-attaches idempotently;
    /// a different identity rotates the fence to `epoch + 1` and deposes the prior
    /// writer. Claim is the sole deposal mechanism. `ClaimError` passes through
    /// under `OpenError.Claim` (incl. `Sealed`) — no narrowing.
    let openWriter
        (basin: S2.Basin)
        (session: Turn.SessionId)
        (holderId: Authority.HolderId)
        : Async<Result<Writer, OpenError>> =
        async {
            try
                match! Authority.claim basin codec (subject session) holderId with
                | Ok holder -> return Ok { Holder = holder }
                | Error claimError -> return Error(OpenError.Claim claimError)
            with error ->
                return Error(OpenError.Failed(S2Errors.classify error))
        }

    /// Store (replace) the current artifact under the writer's fence
    /// (`Authority.commit`). Last-store-under-fence-wins. A deposed writer fails
    /// `Deposed` — it may compute but cannot commit. The resume register is never
    /// sealed (a fenced last-write-wins cell, no terminal), so an `Authority`
    /// `Sealed` here is an unexpected substrate state, surfaced as `Failed`.
    let store (writer: Writer) (artifact: ResumeArtifact) : Async<Result<unit, StoreError>> =
        async {
            match! Authority.commit writer.Holder [ artifact ] with
            | Ok _ -> return Ok()
            | Error(Authority.CommitError.Deposed _) -> return Error StoreError.Deposed
            | Error Authority.CommitError.Sealed ->
                return Error(StoreError.Failed(S2Errors.Other "resume register unexpectedly sealed"))
            | Error(Authority.CommitError.Failed failure) -> return Error(StoreError.Failed failure)
        }

    // ---- read (authority-free tailing reader) -----------------------------

    /// Drain every artifact record on the resume subject, skipping the interleaved
    /// `Authority` fence rotations (S2 *command* records) via
    /// `IgnoreCommandRecords` — so a bare takeover (claim with no subsequent
    /// `store`) never shadows the current artifact. Paginated and bounded
    /// (`WaitSecs = 0` drains to the current tail rather than tailing forever). An
    /// empty / not-yet-created stream reads as no records.
    let private readArtifacts (basin: S2.Basin) (subj: SubjectId) : Async<ResumeArtifact list> =
        async {
            let name = subjectName subj
            do! S2.ensureStream name basin
            let stream = basin |> S2.stream name

            let rec loop (from: int64) (acc: ResumeArtifact list) =
                async {
                    let! batch =
                        async {
                            try
                                return!
                                    stream
                                    |> S2.readWith
                                        { S2.ReadOptions.empty with
                                            Start = Some(S2.FromSeqNum from)
                                            WaitSecs = Some 0
                                            IgnoreCommandRecords = true }
                            with error ->
                                match S2Errors.classify error with
                                | S2Errors.RangeNotSatisfiable _ -> return []
                                | _ -> return raise error
                        }

                    match batch with
                    | [] -> return acc
                    | records ->
                        let decoded =
                            records
                            |> List.map (fun (record: S2.ReadRecord) ->
                                match codec.Decode record.Body with
                                | Ok value -> value
                                | Error message -> failwithf "resume-artifact decode failed at seq %d: %s" record.SeqNum message)

                        let last = (List.last records).SeqNum
                        return! loop (last + 1L) (acc @ decoded)
                }

            return! loop 0L []
        }

    /// Read the current artifact — authority-free (readers need no claim; matches
    /// DurableLog `attach` — per the canon, readers never need authority).
    /// Returns the artifact of the latest successful `store`, or `None` when the
    /// session has never stored one. *Re-hydration* reads, however, must be
    /// claim-first (`openWriter` before `read`) — the claim-then-read law the
    /// `session.resume-artifact-fenced` proof enforces; this authority-free read is
    /// for observers/UI.
    let read (basin: S2.Basin) (session: Turn.SessionId) : Async<Result<ResumeArtifact option, ReadError>> =
        async {
            try
                let! artifacts = readArtifacts basin (subject session)
                return Ok(List.tryLast artifacts)
            with error ->
                return Error(ReadError.Failed(S2Errors.classify error))
        }
