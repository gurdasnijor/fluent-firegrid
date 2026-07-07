namespace Firegrid.Foundation

open Firegrid.Log
open Firegrid.Foundation.SubjectHistory

/// I5 — the write-authority protocol, per
/// `docs/canon/architecture/fluent/authority-and-actors.md`. Claim, epoch
/// deposal, and seal over a subject in the canon's three regimes: Open (CAS
/// admission), FencedOwner (epoch), Sealed.
///
/// This module is generic and domain-free. It composes the proven S2 fence
/// primitives (`Record.fence`, `AppendOptions.fencingToken`, `tryAppendWith`,
/// `FencingTokenMismatch`) that the P3 `Foundation/Durable` port also uses — it
/// does not duplicate them; it adds the epoch/holder semantics on top.
///
/// Fence token ↔ epoch: the substrate fence token is the epoch's string
/// encoding (`"{epoch}/{holderId}"`), so `claim` rotates the fence to
/// `epoch + 1` and a stale holder's fenced append surfaces
/// `S2Errors.FencingTokenMismatch expected`, from which the deposing epoch is
/// read back directly. Deposal is epoch increment, never revocation.
///
/// EffSharp-free: `Async` + `Result` + DU errors + `Codec` records only.
module Authority =

    /// Monotonic authority generation; a claim mints the next epoch.
    type Epoch = Epoch of int64

    /// Substrate fence token (the epoch's encoding). Opaque — never seen by
    /// consumers.
    type Fence = private Fence of string

    /// Identity of a would-be holder (worker/process). The same identity
    /// re-attaches; a different identity takes over. (encore-ds `insertOrGet`
    /// shape.)
    type HolderId = HolderId of string

    /// A live claim, bound to one epoch's fence — carries its Basin + Codec, so
    /// commit/seal take no extra args. Only the holder commits or seals.
    type Holder<'record> =
        private
            { Basin: S2.Basin
              Codec: Codec<'record>
              Subject: SubjectId
              HolderEpoch: Epoch
              HolderFence: Fence }

    let epoch (holder: Holder<'record>) : Epoch = holder.HolderEpoch

    [<RequireQualifiedAccess>]
    type ClaimError =
        /// Subject already terminal.
        | Sealed
        | Failed of S2Errors.S2Failure

    [<RequireQualifiedAccess>]
    type CommitError =
        /// A newer epoch rotated the fence.
        | Deposed of by: Epoch
        /// Subject sealed; no holder may append.
        | Sealed
        | Failed of S2Errors.S2Failure

    [<RequireQualifiedAccess>]
    type AdmitError<'record> =
        /// Open-CAS: another writer took the slot.
        | Lost of AppendConflict<'record>
        | Failed of S2Errors.S2Failure

    // ---- Fence-token codec (private) -------------------------------------

    /// Sentinel fence token installed on seal: no live holder ever presents it,
    /// so every post-seal fenced append is rejected with this as `expected`.
    [<Literal>]
    let private sealedToken = "firegrid-authority/sealed"

    /// Impossible fence token used to non-destructively harvest the current
    /// fence: a fenced append conditioned on it always mismatches (our encoder
    /// never produces it), revealing the real token as `expected` without
    /// writing.
    [<Literal>]
    let private probeToken = "firegrid-authority/probe"

    /// The state a fence token encodes.
    type private FenceState =
        | Unclaimed
        | LiveHeld of Epoch * HolderId
        | LogSealed

    let private encodeFence (Epoch e) (HolderId h) : string = sprintf "%d/%s" e h

    /// Decode a fence token back to its state. Live tokens are `"{epoch}/{holderId}"`
    /// — epoch is the digits before the first `/`, so a holderId containing `/`
    /// round-trips unambiguously.
    let private classifyToken (token: string) : FenceState =
        if token = sealedToken then
            LogSealed
        else
            let idx = token.IndexOf('/')

            if idx <= 0 then
                Unclaimed
            else
                let epochPart = token.Substring(0, idx)
                let holderPart = token.Substring(idx + 1)

                try
                    LiveHeld(Epoch(System.Int64.Parse epochPart), HolderId holderPart)
                with _ ->
                    Unclaimed

    let private subjectName (SubjectId value) = value

    /// Non-destructively read the subject's current fence via a conditional
    /// append that is designed to mismatch. Only called on a non-empty
    /// FencedOwner subject, which always carries a fence.
    let private harvest (basin: S2.Basin) (subject: SubjectId) : Async<Result<FenceState, S2Errors.S2Failure>> =
        async {
            let stream = basin |> S2.stream (subjectName subject)
            let opts = S2.AppendOptions.none |> S2.AppendOptions.fencingToken probeToken
            let! result = stream |> S2.tryAppendWith opts [ S2.Record.fence probeToken ]

            match result with
            | Error(S2Errors.FencingTokenMismatch expected) -> return Ok(classifyToken expected)
            // The probe wrote (subject had no fence — wrong regime). Surface it
            // rather than silently proceeding.
            | Ok _ -> return Ok Unclaimed
            | Error other -> return Error other
        }

    /// Retry bound for the (rare) case of several fresh holders racing takeover.
    [<Literal>]
    let private maxAttempts = 32

    // ---- FencedOwner regime ----------------------------------------------

    /// Claim the subject under `holderId`: returns the existing holder if it
    /// already holds the current epoch (idempotent), else rotates the fence to
    /// `epoch + 1` and takes over. `claim` is the sole deposal mechanism; fails
    /// `Sealed` if the subject is already terminal.
    let claim
        (basin: S2.Basin)
        (codec: Codec<'r>)
        (subject: SubjectId)
        (holderId: HolderId)
        : Async<Result<Holder<'r>, ClaimError>> =
        async {
            do! S2.ensureStream (subjectName subject) basin
            let stream = basin |> S2.stream (subjectName subject)

            let mkHolder (e: Epoch) (token: string) : Holder<'r> =
                { Basin = basin
                  Codec = codec
                  Subject = subject
                  HolderEpoch = e
                  HolderFence = Fence token }

            // Take over from a discovered current fence, or reflect an existing
            // hold idempotently.
            let rec takeover attemptsLeft (state: FenceState) =
                async {
                    match state with
                    | LogSealed -> return Error ClaimError.Sealed
                    | LiveHeld(e, h) when h = holderId ->
                        // Same identity already holds the current epoch: idempotent.
                        return Ok(mkHolder e (encodeFence e holderId))
                    | LiveHeld(Epoch e, h) ->
                        if attemptsLeft <= 0 then
                            return Error(ClaimError.Failed(S2Errors.Other "claim: takeover contention exceeded"))
                        else
                            let newEpoch = Epoch(e + 1L)
                            let newToken = encodeFence newEpoch holderId
                            let opts =
                                S2.AppendOptions.none
                                |> S2.AppendOptions.fencingToken (encodeFence (Epoch e) h)

                            let! rotated = stream |> S2.tryAppendWith opts [ S2.Record.fence newToken ]

                            match rotated with
                            | Ok _ -> return Ok(mkHolder newEpoch newToken)
                            | Error(S2Errors.FencingTokenMismatch expected) ->
                                // Another claimer raced us; re-decide from the new fence.
                                return! takeover (attemptsLeft - 1) (classifyToken expected)
                            | Error other -> return Error(ClaimError.Failed other)
                    | Unclaimed ->
                        return
                            Error(ClaimError.Failed(S2Errors.Other "claim: subject carries no authority fence (wrong regime?)"))
                }

            let! tail = SubjectHistory.tail basin subject

            if SubjectHistory.versionNumber tail = 0L then
                // Fresh subject: seed epoch 1 with an open-CAS append on the empty tail.
                let token1 = encodeFence (Epoch 1L) holderId
                let opts = S2.AppendOptions.none |> S2.AppendOptions.matchSeqNum 0L
                let! seeded = stream |> S2.tryAppendWith opts [ S2.Record.fence token1 ]

                match seeded with
                | Ok _ -> return Ok(mkHolder (Epoch 1L) token1)
                | Error(S2Errors.SeqNumMismatch _) ->
                    // Lost the genesis race; discover and take over / re-attach.
                    let! discovered = harvest basin subject

                    match discovered with
                    | Ok state -> return! takeover maxAttempts state
                    | Error failure -> return Error(ClaimError.Failed failure)
                | Error other -> return Error(ClaimError.Failed other)
            else
                let! discovered = harvest basin subject

                match discovered with
                | Ok state -> return! takeover maxAttempts state
                | Error failure -> return Error(ClaimError.Failed failure)
        }

    let private mapFenceMismatch (expected: string) : CommitError =
        match classifyToken expected with
        | LogSealed -> CommitError.Sealed
        | LiveHeld(by, _) -> CommitError.Deposed by
        | Unclaimed -> CommitError.Failed(S2Errors.Other "unexpected fence state after mismatch")

    /// Fenced append of domain records under the holder's epoch.
    let commit (holder: Holder<'r>) (records: 'r list) : Async<Result<Version, CommitError>> =
        async {
            if List.isEmpty records then
                let! tail = SubjectHistory.tail holder.Basin holder.Subject
                return Ok tail
            else
                let stream = holder.Basin |> S2.stream (subjectName holder.Subject)
                let (Fence token) = holder.HolderFence
                let opts = S2.AppendOptions.none |> S2.AppendOptions.fencingToken token
                let encoded = records |> List.map (holder.Codec.Encode >> S2.Record.text)
                let! result = stream |> S2.tryAppendWith opts encoded

                match result with
                | Ok ack -> return Ok(Version ack.End.SeqNum)
                | Error(S2Errors.FencingTokenMismatch expected) -> return Error(mapFenceMismatch expected)
                | Error other -> return Error(CommitError.Failed other)
        }

    /// Append the terminal record and extinguish authority (first-terminal-wins).
    /// The terminal and the sealing fence rotation land in one atomic fenced
    /// batch, so a deposed holder seals nothing.
    let seal (holder: Holder<'r>) (terminal: 'r) : Async<Result<unit, CommitError>> =
        async {
            let stream = holder.Basin |> S2.stream (subjectName holder.Subject)
            let (Fence token) = holder.HolderFence
            let opts = S2.AppendOptions.none |> S2.AppendOptions.fencingToken token

            let records =
                [ S2.Record.text (holder.Codec.Encode terminal)
                  S2.Record.fence sealedToken ]

            let! result = stream |> S2.tryAppendWith opts records

            match result with
            | Ok _ -> return Ok()
            | Error(S2Errors.FencingTokenMismatch expected) -> return Error(mapFenceMismatch expected)
            | Error other -> return Error(CommitError.Failed other)
        }

    // ---- Open regime ------------------------------------------------------

    /// Leaderless single-winner CAS (checkpoint election, dedupe claims,
    /// admission records). Thin naming over `SubjectHistory.appendExpected`.
    let admit
        (basin: S2.Basin)
        (codec: Codec<'r>)
        (subject: SubjectId)
        (expected: Version)
        (records: 'r list)
        : Async<Result<Version, AdmitError<'r>>> =
        async {
            do! S2.ensureStream (subjectName subject) basin
            let! result = SubjectHistory.appendExpected basin codec subject expected records

            match result with
            | Ok version -> return Ok version
            | Error(AppendFailure.Conflict conflict) -> return Error(AdmitError.Lost conflict)
            | Error(AppendFailure.Failed failure) -> return Error(AdmitError.Failed failure)
        }
