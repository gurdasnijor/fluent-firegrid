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
/// - `session.turn-crash-terminal` — a live deposed producer cannot append after
///   a recovery host takes over; recovery drives the turn to a durable terminal;
///   an attached reader observes the terminal rather than hanging. The
///   `store-object-live-fencing` two-host + real-kill technique, adapted to the
///   foundation runner (which has no `processHost`): two S2 clients over one
///   `s2Lite`, the "killed" producer kept live to model a still-computing owner.
/// - `session.turn-idempotent-create` — a same-identity retry re-attaches to the
///   one stream (never forks); a different identity takes over under a new epoch,
///   depposing the prior producers.
module FoundationTurnStreamProof =

    type AttachProofResult =
        { StartReaderObservedFullPrefix: bool
          MidReaderObservedFullPrefix: bool
          DecodedPrefixesIdentical: bool
          EncodedPrefixesByteIdentical: bool
          BothObservedSameTerminal: bool
          TerminalIsCompleted: bool }

    type CrashTerminalProofResult =
        { RecoveryHostTookOver: bool
          LiveDeposedProducerCannotCommit: bool
          RecoveryReachedDurableTerminal: bool
          ReaderObservedTerminalNotHang: bool
          StaleWriteNeverEnteredLog: bool
          ReaderSawPreCrashThenRecovery: bool }

    type IdempotentCreateProofResult =
        { SameIdentityRetryReattached: bool
          SameIdentityNotDeposed: bool
          DifferentIdentityTookOver: bool
          PriorProducersDeposedAfterTakeover: bool
          OneStreamNeverForked: bool
          StaleWritesRejected: bool
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

    // ---- session.turn-crash-terminal -------------------------------------

    let private crashTerminalWorkload ctx =
        ProofOperation.run
            ctx
            "session.turn.crash-terminal"
            "session-turn-crash-terminal"
            { ProofOperationOptions.empty with
                Key = Some "session-turn-crash-terminal" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let endpoint =
                    match s2.Endpoint with
                    | Some value -> value
                    | None -> failwith "crash-terminal requires an s2 endpoint (declare s2Lite)"

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "turn-crash-" + suffix

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName

                // Recovery host: a fresh S2 connection to the same durable store,
                // modelling a separate process that takes over after the crash.
                let recoveryClient =
                    S2.connectWith
                        { S2.ConnectOptions.create "s2-lite-turn-crash-recovery" with
                            AccountEndpoint = Some endpoint
                            BasinEndpoint = Some endpoint }

                let recoveryBasin = recoveryClient |> S2.basin basinName

                let address = Turn.address (Turn.SessionId("sess-" + suffix)) (Turn.TurnId "turn-1")

                // Producer A drives the turn mid-flight, without sealing.
                let! producerA = createOk basin address (Authority.HolderId "producer-a")
                do! appendOk producerA (TurnChunk.Text "a-chunk-0")
                do! appendOk producerA (TurnChunk.Text "a-chunk-1")

                // A reader attaches mid-turn, before the crash and recovery.
                let! reader = attachOk basin address

                // "kill -9": producer A is declared crashed. We keep its holder
                // live to model a still-computing owner that will try to commit.
                // Recovery process B claims the same turn (new epoch, takeover).
                let! recoveryResult = DurableLog.create recoveryBasin Turn.codec address (Authority.HolderId "recovery-b")

                let recoveryHostTookOver =
                    match recoveryResult with
                    | Ok _ -> true
                    | Error _ -> false

                let producerB =
                    match recoveryResult with
                    | Ok producer -> producer
                    | Error _ -> failwith "recovery host failed to take over the crashed turn"

                // The live deposed producer A computes but cannot commit.
                let! staleResult = DurableLog.append producerA (TurnChunk.Text "a-stale-post-crash")
                let liveDeposedProducerCannotCommit = (staleResult = Error DurableLog.AppendError.Deposed)

                // Recovery drives the interrupted turn to a durable terminal.
                do! appendOk producerB (TurnChunk.Text "b-recovery-note")
                do! sealOk producerB TurnTerminal.Cancelled

                // The attached reader observes the terminal rather than hanging.
                let! chunks, terminal = drain reader
                do! DurableLog.close reader

                let result =
                    { RecoveryHostTookOver = recoveryHostTookOver
                      LiveDeposedProducerCannotCommit = liveDeposedProducerCannotCommit
                      RecoveryReachedDurableTerminal = (terminal = TurnTerminal.Cancelled)
                      ReaderObservedTerminalNotHang = (terminal = TurnTerminal.Cancelled)
                      StaleWriteNeverEnteredLog =
                        not (chunks |> List.exists (fun chunk -> chunk = TurnChunk.Text "a-stale-post-crash"))
                      ReaderSawPreCrashThenRecovery =
                        (chunks =
                            [ TurnChunk.Text "a-chunk-0"
                              TurnChunk.Text "a-chunk-1"
                              TurnChunk.Text "b-recovery-note" ]) }

                do!
                    ctx.EmitSpan
                        "proof.session.turn-crash-terminal.completed"
                        [ "proof.property", "session.turn-crash-terminal"
                          "turn.live_deposed_cannot_commit", string result.LiveDeposedProducerCannotCommit
                          "turn.recovery_terminal", string result.RecoveryReachedDurableTerminal
                          "turn.stale_write_rejected", string result.StaleWriteNeverEnteredLog ]

                return result
            })

    let private crashTerminalProperty =
        property "session.turn-crash-terminal" {
            s2Lite ""
            workload crashTerminalWorkload

            verify (fun v ->
                [ v.Expect.Workload "a recovery host takes over the crashed turn under a new epoch" (fun result ->
                      result.RecoveryHostTookOver)
                  v.Expect.Workload "the live deposed producer cannot commit stale output after takeover" (fun result ->
                      result.LiveDeposedProducerCannotCommit)
                  v.Expect.Workload "recovery drives the turn to a durable terminal" (fun result ->
                      result.RecoveryReachedDurableTerminal)
                  v.Expect.Workload "the attached reader observes the terminal rather than hanging" (fun result ->
                      result.ReaderObservedTerminalNotHang)
                  v.Expect.Workload "the deposed producer's stale write never entered the durable log" (fun result ->
                      result.StaleWriteNeverEnteredLog)
                  v.Expect.Workload "the reader observes the pre-crash prefix then the recovery output" (fun result ->
                      result.ReaderSawPreCrashThenRecovery)
                  v.Trace.SpanExists
                      "turn-crash-terminal completion span emitted"
                      "proof.session.turn-crash-terminal.completed"
                      [ "proof.property", "session.turn-crash-terminal" ]
                  v.Trace.Operation
                      "turn-crash-terminal operation recorded"
                      ({ TraceOperationMatch.named "session.turn.crash-terminal" with
                          Status = Some "ok"
                          OutputContains = [ "LiveDeposedProducerCannotCommit"; "RecoveryReachedDurableTerminal" ]
                          Count = Some 1 }) ])
        }

    // ---- session.turn-idempotent-create ----------------------------------

    let private idempotentCreateWorkload ctx =
        ProofOperation.run
            ctx
            "session.turn.idempotent-create"
            "session-turn-idempotent-create"
            { ProofOperationOptions.empty with
                Key = Some "session-turn-idempotent-create" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx
                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "turn-create-" + suffix

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName

                let address = Turn.address (Turn.SessionId("sess-" + suffix)) (Turn.TurnId "turn-1")

                // First create under holder-a, and one committed chunk.
                let! producer1 = createOk basin address (Authority.HolderId "holder-a")
                do! appendOk producer1 (TurnChunk.Text "from-p1")

                // Same-identity retry: re-attaches to the live log (idempotent),
                // never a second stream.
                let! retryResult = DurableLog.create basin Turn.codec address (Authority.HolderId "holder-a")

                let sameIdentityRetryReattached =
                    match retryResult with
                    | Ok _ -> true
                    | Error _ -> false

                let producer2 =
                    match retryResult with
                    | Ok producer -> producer
                    | Error _ -> failwith "same-identity retry did not re-attach"

                do! appendOk producer2 (TurnChunk.Text "from-p2")

                // The retry did not depose the first producer (same epoch).
                let! producer1StillLive = DurableLog.append producer1 (TurnChunk.Text "from-p1-again")
                let sameIdentityNotDeposed = (producer1StillLive = Ok())

                // A different identity takes over under a new epoch.
                let! takeoverResult = DurableLog.create basin Turn.codec address (Authority.HolderId "holder-b")

                let differentIdentityTookOver =
                    match takeoverResult with
                    | Ok _ -> true
                    | Error _ -> false

                let producer3 =
                    match takeoverResult with
                    | Ok producer -> producer
                    | Error _ -> failwith "different identity failed to take over"

                // Both holder-a producers are now deposed.
                let! p1AfterTakeover = DurableLog.append producer1 (TurnChunk.Text "stale-1")
                let! p2AfterTakeover = DurableLog.append producer2 (TurnChunk.Text "stale-2")

                let priorProducersDeposedAfterTakeover =
                    p1AfterTakeover = Error DurableLog.AppendError.Deposed
                    && p2AfterTakeover = Error DurableLog.AppendError.Deposed

                do! appendOk producer3 (TurnChunk.Text "from-p3")
                do! sealOk producer3 TurnTerminal.Completed

                // One stream carries every committed chunk across both identities —
                // no fork — and rejects the deposed producers' stale writes.
                let! reader = attachOk basin address
                let! chunks, terminal = drain reader
                do! DurableLog.close reader

                let expectedLog =
                    [ TurnChunk.Text "from-p1"
                      TurnChunk.Text "from-p2"
                      TurnChunk.Text "from-p1-again"
                      TurnChunk.Text "from-p3" ]

                let result =
                    { SameIdentityRetryReattached = sameIdentityRetryReattached
                      SameIdentityNotDeposed = sameIdentityNotDeposed
                      DifferentIdentityTookOver = differentIdentityTookOver
                      PriorProducersDeposedAfterTakeover = priorProducersDeposedAfterTakeover
                      OneStreamNeverForked = (chunks = expectedLog)
                      StaleWritesRejected =
                        not (
                            chunks
                            |> List.exists (fun chunk ->
                                chunk = TurnChunk.Text "stale-1" || chunk = TurnChunk.Text "stale-2")
                        )
                      TerminalIsCompleted = (terminal = TurnTerminal.Completed) }

                do!
                    ctx.EmitSpan
                        "proof.session.turn-idempotent-create.completed"
                        [ "proof.property", "session.turn-idempotent-create"
                          "turn.retry_reattached", string result.SameIdentityRetryReattached
                          "turn.different_identity_took_over", string result.DifferentIdentityTookOver
                          "turn.one_stream", string result.OneStreamNeverForked ]

                return result
            })

    let private idempotentCreateProperty =
        property "session.turn-idempotent-create" {
            s2Lite ""
            workload idempotentCreateWorkload

            verify (fun v ->
                [ v.Expect.Workload "a same-identity retry re-attaches to the live log" (fun result ->
                      result.SameIdentityRetryReattached)
                  v.Expect.Workload "the same-identity retry does not depose the first producer" (fun result ->
                      result.SameIdentityNotDeposed)
                  v.Expect.Workload "a different identity takes over under a new epoch" (fun result ->
                      result.DifferentIdentityTookOver)
                  v.Expect.Workload "the prior producers are deposed after the takeover" (fun result ->
                      result.PriorProducersDeposedAfterTakeover)
                  v.Expect.Workload "one stream carries every committed chunk across both identities" (fun result ->
                      result.OneStreamNeverForked)
                  v.Expect.Workload "the deposed producers' stale writes are rejected" (fun result ->
                      result.StaleWritesRejected)
                  v.Expect.Workload "the takeover holder seals a Completed terminal" (fun result ->
                      result.TerminalIsCompleted)
                  v.Trace.SpanExists
                      "turn-idempotent-create completion span emitted"
                      "proof.session.turn-idempotent-create.completed"
                      [ "proof.property", "session.turn-idempotent-create" ]
                  v.Trace.Operation
                      "turn-idempotent-create operation recorded"
                      ({ TraceOperationMatch.named "session.turn.idempotent-create" with
                          Status = Some "ok"
                          OutputContains = [ "SameIdentityRetryReattached"; "OneStreamNeverForked" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "session.turn-streams" {
            describedAs
                "MS-C2 turn streams over the public DurableLog/Turn surface: byte-faithful attach, live-deposed crash recovery to a durable terminal, and idempotent create (re-attach vs. epoch takeover)."

            property attachProperty
            property crashTerminalProperty
            property idempotentCreateProperty
        }
