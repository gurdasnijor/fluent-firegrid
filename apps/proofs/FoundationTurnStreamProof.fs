namespace Firegrid.Foundation.Proofs

open Firegrid.Log
open Firegrid.Foundation
open Firegrid.Store

/// B2 — the MS-C2 turn-stream proof obligations, driven entirely through the
/// public `DurableLog` / `Turn` surface (I1) shipped by B1. No deep imports, no
/// proof-only branches in production code.
///
/// - `session.turn-attach` — a reader attaching mid-flight observes a
///   byte-identical prefix to a reader attached from the start, then the same
///   live tail and terminal.
///
///   SUNSET (P0.3b): `session.turn-attach` duplicates the red corpus law
///   `t1.log-attach-byte-faithful` one layer down (docs/proofs-inventory.md,
///   family 7). Retire this proof when that law greens — we do not delete
///   coverage for a law that is not yet green.
///
/// The `session.turn-crash-terminal` and `session.turn-idempotent-create`
/// obligations moved to the `foundation.fencing` invariant family
/// (FoundationFencingProof.fs) in Packet 0.3b.
module FoundationTurnStreamProof =

    type AttachProofResult =
        { StartReaderObservedFullPrefix: bool
          MidReaderObservedFullPrefix: bool
          DecodedPrefixesIdentical: bool
          EncodedPrefixesByteIdentical: bool
          BothObservedSameTerminal: bool
          TerminalIsCompleted: bool }

    // ---- Helpers over the public surface ---------------------------------

    let private createOk
        (basin: S2.Basin)
        (address: DurableLog.Address)
        (holder: Authority.HolderId)
        : Async<DurableLog.Producer<TurnChunk, TurnTerminal>> =
        async {
            match! DurableLog.create basin Turn.codec address holder with
            | Ok producer -> return producer
            | Error _ -> return failwith "durable-log: create failed unexpectedly (single-writer workload)"
        }

    let private appendOk (producer: DurableLog.Producer<TurnChunk, TurnTerminal>) (chunk: TurnChunk) : Async<unit> =
        async {
            match! DurableLog.append producer chunk with
            | Ok() -> return ()
            | Error _ -> return failwith "durable-log: append by the live holder failed unexpectedly"
        }

    let private sealOk (producer: DurableLog.Producer<TurnChunk, TurnTerminal>) (terminal: TurnTerminal) : Async<unit> =
        async {
            match! DurableLog.seal producer terminal with
            | Ok() -> return ()
            | Error _ -> return failwith "durable-log: seal by the live holder failed unexpectedly"
        }

    let private attachOk
        (basin: S2.Basin)
        (address: DurableLog.Address)
        : Async<DurableLog.Attachment<TurnChunk, TurnTerminal>> =
        async {
            match! DurableLog.attach basin Turn.codec address with
            | Ok attachment -> return attachment
            | Error _ -> return failwith "durable-log: attach failed unexpectedly"
        }

    /// Replay-from-zero + live-tail + terminal. Blocks-with-wait on `next`; since
    /// each proof drains only after the log is sealed, it terminates at the
    /// terminal (a hung reader would never return and would fail the trial).
    let private drain (attachment: DurableLog.Attachment<TurnChunk, TurnTerminal>) : Async<TurnChunk list * TurnTerminal> =
        let chunks = ResizeArray<TurnChunk>()

        let rec loop () =
            async {
                match! DurableLog.next attachment with
                | Ok(Some chunk) ->
                    chunks.Add chunk
                    return! loop ()
                | Ok None ->
                    match! DurableLog.terminal attachment with
                    | Ok terminal -> return (List.ofSeq chunks, terminal)
                    | Error _ -> return failwith "durable-log: terminal read failed"
                | Error _ -> return failwith "durable-log: next read failed"
            }

        loop ()

    let private encodeChunks (chunks: TurnChunk list) : string list =
        chunks |> List.map Turn.codec.Chunk.Encode

    // ---- session.turn-attach ---------------------------------------------

    let private attachWorkload ctx =
        ProofOperation.run
            ctx
            "session.turn.attach"
            "session-turn-attach"
            { ProofOperationOptions.empty with
                Key = Some "session-turn-attach" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx
                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "turn-attach-" + suffix

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName

                let address = Turn.address (Turn.SessionId("sess-" + suffix)) (Turn.TurnId "turn-1")
                let! producer = createOk basin address (Authority.HolderId "producer-a")

                // A reader attached from the start, on the still-empty log.
                let! startReader = attachOk basin address

                // Mid-flight production.
                do! appendOk producer (TurnChunk.Text "chunk-0")
                do! appendOk producer (TurnChunk.Text "chunk-1")

                // A reader attached mid-flight (before the seal).
                let! midReader = attachOk basin address

                // The reserved claim-check `Blob` shape rides the same wire path.
                do! appendOk producer (TurnChunk.Text "chunk-2")
                do! appendOk producer (TurnChunk.Blob "attachment://turn-1/part-0")
                do! sealOk producer TurnTerminal.Completed

                // Both readers replay from zero, so both observe the full prefix,
                // the live tail appended after they attached, and the terminal.
                let! startChunks, startTerminal = drain startReader
                let! midChunks, midTerminal = drain midReader
                do! DurableLog.close startReader
                do! DurableLog.close midReader

                let produced =
                    [ TurnChunk.Text "chunk-0"
                      TurnChunk.Text "chunk-1"
                      TurnChunk.Text "chunk-2"
                      TurnChunk.Blob "attachment://turn-1/part-0" ]

                let result =
                    { StartReaderObservedFullPrefix = (startChunks = produced)
                      MidReaderObservedFullPrefix = (midChunks = produced)
                      DecodedPrefixesIdentical = (startChunks = midChunks)
                      EncodedPrefixesByteIdentical = (encodeChunks startChunks = encodeChunks midChunks)
                      BothObservedSameTerminal = (startTerminal = midTerminal)
                      TerminalIsCompleted = (startTerminal = TurnTerminal.Completed) }

                do!
                    ctx.EmitSpan
                        "proof.session.turn-attach.completed"
                        [ "proof.property", "session.turn-attach"
                          "turn.prefix_byte_identical", string result.EncodedPrefixesByteIdentical
                          "turn.same_terminal", string result.BothObservedSameTerminal ]

                return result
            })

    let private attachProperty =
        property "session.turn-attach" {
            s2Lite ""
            workload attachWorkload

            verify (fun v ->
                [ v.Expect.Workload "reader attached from the start observes the full produced prefix" (fun result ->
                      result.StartReaderObservedFullPrefix)
                  v.Expect.Workload "reader attached mid-flight observes the full produced prefix" (fun result ->
                      result.MidReaderObservedFullPrefix)
                  v.Expect.Workload "mid-flight and start readers decode identical chunk prefixes" (fun result ->
                      result.DecodedPrefixesIdentical)
                  v.Expect.Workload "the observed prefixes are byte-identical on the wire" (fun result ->
                      result.EncodedPrefixesByteIdentical)
                  v.Expect.Workload "both readers observe the same terminal" (fun result ->
                      result.BothObservedSameTerminal)
                  v.Expect.Workload "the terminal is the sealed Completed status" (fun result ->
                      result.TerminalIsCompleted)
                  v.Trace.SpanExists
                      "turn-attach completion span emitted"
                      "proof.session.turn-attach.completed"
                      [ "proof.property", "session.turn-attach" ]
                  v.Trace.Operation
                      "turn-attach operation recorded"
                      ({ TraceOperationMatch.named "session.turn.attach" with
                          Status = Some "ok"
                          OutputContains = [ "EncodedPrefixesByteIdentical"; "BothObservedSameTerminal" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "session.turn-streams" {
            describedAs
                "MS-C2 turn streams over the public DurableLog/Turn surface: byte-faithful attach (SUNSET: retire when t1.log-attach-byte-faithful greens)."

            property attachProperty
        }
