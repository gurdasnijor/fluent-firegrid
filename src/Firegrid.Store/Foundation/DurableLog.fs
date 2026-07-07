namespace Firegrid.Foundation

open Firegrid.Log
open Firegrid.Foundation.SubjectHistory

/// I1 — the generic sealed, single-writer, schema-coded durable log:
/// `SubjectHistory` + `Authority` + seal. A domain binding (e.g. `Turn`) is an
/// address scheme plus chunk/terminal codecs over this API — never a new
/// method. Domain methods on the binding are a G1 violation.
///
/// Chunk and terminal records share one authoritative stream through a private
/// tag envelope, so the whole log is one fenced subject: `create` = claim,
/// `append`/`seal` = fenced commit, `attach`/`next`/`terminal` = an
/// authority-free tailing reader.
///
/// EffSharp-free: `Async` + `Result` + DU errors + `Codec` records +
/// pull-cursor reads.
module DurableLog =

    /// Derived (never random) address → subject/stream name.
    type Address = { Segments: string list }

    /// Streamed-body and terminal codecs for one log.
    type Codec<'chunk, 'terminal> =
        { Chunk: SubjectHistory.Codec<'chunk>
          Terminal: SubjectHistory.Codec<'terminal> }

    [<RequireQualifiedAccess>]
    type CreateError =
        | Sealed
        | Failed of S2Errors.S2Failure

    [<RequireQualifiedAccess>]
    type AppendError =
        | Deposed
        | Sealed
        | Failed of S2Errors.S2Failure

    [<RequireQualifiedAccess>]
    type AttachError =
        | NotFound
        | Failed of S2Errors.S2Failure

    // ---- Chunk|terminal envelope over one Authority codec -----------------

    type private Envelope<'chunk, 'terminal> =
        | Streamed of 'chunk
        | Terminated of 'terminal

    let private envelopeCodec (codec: Codec<'c, 't>) : SubjectHistory.Codec<Envelope<'c, 't>> =
        { Encode =
            fun envelope ->
                match envelope with
                | Streamed chunk -> "C" + codec.Chunk.Encode chunk
                | Terminated terminal -> "T" + codec.Terminal.Encode terminal
          Decode =
            fun body ->
                if body.Length = 0 then
                    Error "empty durable-log record"
                else
                    let rest = body.Substring(1)

                    match body.[0] with
                    | 'C' -> codec.Chunk.Decode rest |> Result.map Streamed
                    | 'T' -> codec.Terminal.Decode rest |> Result.map Terminated
                    | tag -> Error(sprintf "unknown durable-log tag '%c'" tag) }

    let private subjectOf (address: Address) : SubjectId =
        SubjectId(String.concat "/" address.Segments)

    let private subjectName (SubjectId value) = value

    /// A producer wraps an `Authority.Holder` over the chunk|terminal codec.
    type Producer<'chunk, 'terminal> =
        private { Holder: Authority.Holder<Envelope<'chunk, 'terminal>> }

    /// An attachment is a reader (no authority needed): a re-openable tailing
    /// cursor plus the observed terminal.
    type Attachment<'chunk, 'terminal> =
        private
            { Basin: S2.Basin
              Subject: SubjectId
              Codec: SubjectHistory.Codec<Envelope<'chunk, 'terminal>>
              mutable Cursor: S2.ReadCursor
              mutable Position: int64
              mutable Terminal: 'terminal option
              mutable Ended: bool }

    // ---- Producer ---------------------------------------------------------

    /// `create` = `Authority.claim` under `holderId`, bound to the address: the
    /// same identity re-attaches to the live log; a different identity takes it
    /// over under a new epoch (never forks a second stream). AlreadyLive
    /// rejection is lifecycle policy (MS-C5), not a mechanism here.
    let create
        (basin: S2.Basin)
        (codec: Codec<'c, 't>)
        (address: Address)
        (holderId: Authority.HolderId)
        : Async<Result<Producer<'c, 't>, CreateError>> =
        async {
            let! claimed = Authority.claim basin (envelopeCodec codec) (subjectOf address) holderId

            match claimed with
            | Ok holder -> return Ok { Holder = holder }
            | Error Authority.ClaimError.Sealed -> return Error CreateError.Sealed
            | Error(Authority.ClaimError.Failed failure) -> return Error(CreateError.Failed failure)
        }

    let private mapCommitError (error: Authority.CommitError) : AppendError =
        match error with
        | Authority.CommitError.Deposed _ -> AppendError.Deposed
        | Authority.CommitError.Sealed -> AppendError.Sealed
        | Authority.CommitError.Failed failure -> AppendError.Failed failure

    let append (producer: Producer<'c, 't>) (chunk: 'c) : Async<Result<unit, AppendError>> =
        async {
            let! committed = Authority.commit producer.Holder [ Streamed chunk ]

            match committed with
            | Ok _ -> return Ok()
            | Error error -> return Error(mapCommitError error)
        }

    let seal (producer: Producer<'c, 't>) (terminal: 't) : Async<Result<unit, AppendError>> =
        async {
            let! sealed' = Authority.seal producer.Holder (Terminated terminal)

            match sealed' with
            | Ok() -> return Ok()
            | Error error -> return Error(mapCommitError error)
        }

    // ---- Reader -----------------------------------------------------------

    /// Poll window for the tailing cursor: `next` re-opens with this wait until a
    /// chunk or the seal arrives (the `openCursorWithWait` idiom). Command
    /// (fence) records are filtered out — only envelope records are read.
    [<Literal>]
    let private waitSecs = 1

    let private openCursor (basin: S2.Basin) (subject: SubjectId) (from: int64) : Async<S2.ReadCursor> =
        basin
        |> S2.stream (subjectName subject)
        |> S2.readCursor
            { S2.ReadOptions.empty with
                Start = Some(S2.FromSeqNum from)
                WaitSecs = Some waitSecs
                IgnoreCommandRecords = true }

    let attach
        (basin: S2.Basin)
        (codec: Codec<'c, 't>)
        (address: Address)
        : Async<Result<Attachment<'c, 't>, AttachError>> =
        async {
            let subject = subjectOf address

            try
                do! S2.ensureStream (subjectName subject) basin
                let! cursor = openCursor basin subject 0L

                return
                    Ok
                        { Basin = basin
                          Subject = subject
                          Codec = envelopeCodec codec
                          Cursor = cursor
                          Position = 0L
                          Terminal = None
                          Ended = false }
            with error ->
                return Error(AttachError.Failed(S2Errors.classify error))
        }

    /// Pull-cursor (canon stream idiom): blocks-with-wait until the next chunk or
    /// the seal arrives, yielding `Ok (Some c)` per chunk and `Ok None` at the
    /// terminal.
    let rec next (attachment: Attachment<'c, 't>) : Async<Result<'c option, AttachError>> =
        async {
            if attachment.Ended then
                return Ok None
            else
                try
                    let! record = S2.tryNext attachment.Cursor

                    match record with
                    | Some record ->
                        attachment.Position <- record.SeqNum + 1L

                        match attachment.Codec.Decode record.Body with
                        | Ok(Streamed chunk) -> return Ok(Some chunk)
                        | Ok(Terminated terminal) ->
                            attachment.Terminal <- Some terminal
                            attachment.Ended <- true
                            return Ok None
                        | Error message -> return Error(AttachError.Failed(S2Errors.Other message))
                    | None ->
                        // Wait window elapsed with no new record: re-open from the
                        // current position and keep waiting.
                        do! S2.closeReadCursor attachment.Cursor
                        let! cursor = openCursor attachment.Basin attachment.Subject attachment.Position
                        attachment.Cursor <- cursor
                        return! next attachment
                with error ->
                    return Error(AttachError.Failed(S2Errors.classify error))
        }

    /// Resolves once the log is sealed: the terminal record's body. Drains any
    /// remaining chunks first (blocks-with-wait until the seal arrives).
    let rec terminal (attachment: Attachment<'c, 't>) : Async<Result<'t, AttachError>> =
        async {
            match attachment.Terminal with
            | Some value -> return Ok value
            | None ->
                let! advanced = next attachment

                match advanced with
                | Ok(Some _) -> return! terminal attachment
                | Ok None ->
                    match attachment.Terminal with
                    | Some value -> return Ok value
                    | None -> return Error(AttachError.Failed(S2Errors.Other "durable log ended without a terminal"))
                | Error error -> return Error error
        }

    let close (attachment: Attachment<'c, 't>) : Async<unit> =
        S2.closeReadCursor attachment.Cursor
