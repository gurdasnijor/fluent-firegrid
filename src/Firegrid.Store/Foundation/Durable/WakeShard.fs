namespace Firegrid.Store.Foundation.Durable

open Fable.Core
open Firegrid.Log
open Firegrid.Store
open Firegrid.Foundation

/// MS-C3 / cross-lane interface **I3** — the wake-record schema and the
/// deterministic shard naming, per
/// `docs/canon/architecture/fluent/authority-and-actors.md` (a wake shard is a
/// shard-granularity mailbox; the router is its actor). Consumed by B3
/// (lifecycle wakes) and future temporal features; changing the record schema
/// *or* the naming scheme is a G1 gate.
///
/// This is the **pure core** of the wake path: record schema, `codec`,
/// `shardOf`/`shardSubject`, and the open-append `post`. None of it touches
/// `Authority` — the FencedOwner shell lives in `WakeRouter`. It reuses the
/// kernel's one pointer vocabulary (`ActorAddress`, `WakeReason`) rather than
/// minting a parallel one.
///
/// EffSharp-free: `Async` + `Result` + DU errors + `Codec` records. The hash and
/// UTF-8 encoding are standard JS ops (no BigInt, no BCL hashing), so `shardOf`
/// is bit-identical wherever it runs.
[<RequireQualifiedAccess>]
module WakeShard =

    /// I3 — a shard identifier: `0 <= id < Count`. Shards partition the subject
    /// space so exactly one router tails each shard's stream.
    type ShardId = ShardId of int

    /// I3 — fixed deployment parameter: how many shard streams exist under a
    /// namespace. Resharding (changing `Count`) is a canon non-goal (no
    /// rebalancing protocol); a `Count` change is an operational migration, out
    /// of C1 scope.
    type ShardConfig = { Namespace: string; Count: int }

    /// I3 — the wake record: a POINTER, never a payload. `Subject` is the actor to
    /// drive; `Reason` is why. `Reason` reuses the kernel `WakeReason`
    /// (`MailboxReady | TimerFired | ChildTerminal`) so there is ONE wake
    /// vocabulary, not two. The triggering message/timer/child already lives on
    /// the subject's own mailbox/log — this record carries none of it.
    type WakeRecord = { Subject: ActorAddress; Reason: WakeReason }

    // ---- Record codec -----------------------------------------------------

    /// I3 — codec for the shard stream. Records are open-appended by any poster.
    /// The shard stream NEVER seals — it is an open-append mailbox forever, with
    /// no terminal record; do not import `DurableLog`'s seal semantics onto the
    /// wake path by analogy. `int64` timer timestamps are encoded as decimal
    /// strings (Fable-safe JSON has no native int64).
    let codec: SubjectHistory.Codec<WakeRecord> =
        { Encode =
            fun record ->
                let segments = record.Subject.Segments |> List.toArray

                match record.Reason with
                | WakeReason.MailboxReady -> JsJson.stringify {| subject = segments; kind = "mailbox" |}
                | WakeReason.TimerFired(TimerId timer, dueAt) ->
                    JsJson.stringify
                        {| subject = segments
                           kind = "timer"
                           timer = timer
                           dueAt = string dueAt |}
                | WakeReason.ChildTerminal(SubjectId child) ->
                    JsJson.stringify {| subject = segments; kind = "child"; child = child |}
          Decode =
            fun body ->
                try
                    let parsed = JsJson.parse<obj> body
                    let subject: ActorAddress =
                        { Segments = JsJson.prop<string[]> parsed "subject" |> Array.toList }

                    match JsJson.stringProp "kind" parsed with
                    | "mailbox" -> Ok { Subject = subject; Reason = WakeReason.MailboxReady }
                    | "timer" ->
                        let timer = TimerId(JsJson.stringProp "timer" parsed)
                        let dueAt = System.Int64.Parse(JsJson.stringProp "dueAt" parsed)
                        Ok { Subject = subject; Reason = WakeReason.TimerFired(timer, dueAt) }
                    | "child" ->
                        Ok
                            { Subject = subject
                              Reason = WakeReason.ChildTerminal(SubjectId(JsJson.stringProp "child" parsed)) }
                    | other -> Error(sprintf "unknown wake kind '%s'" other)
                with error ->
                    Error error.Message }

    // ---- Shard naming (deterministic, I3) ---------------------------------

    /// Exact 32-bit wrapping multiply (JS `Math.imul`); the FNV-1a step multiplies
    /// by the prime `16777619` (`0x01000193`).
    [<Emit("Math.imul($0, 16777619)")>]
    let private mulFnvPrime (_h: int) : int = jsNative

    /// Read a 32-bit value unsigned (`x >>> 0`) as a JS number in `[0, 2^32)`.
    [<Emit("$0 >>> 0")>]
    let private toUnsigned (_h: int) : float = jsNative

    /// UTF-8 bytes of a string as `0..255` ints (`TextEncoder`).
    [<Emit("Array.from(new TextEncoder().encode($0))")>]
    let private utf8Bytes (_s: string) : int[] = jsNative

    /// I3 — the canonical byte key of a subject: UTF-8 of `"seg0/seg1/..."`.
    let private addressKey (subject: ActorAddress) : string =
        String.concat "/" subject.Segments

    /// I3 — stable, deployment-independent mapping subject → shard, pinned in full
    /// so any later change is a visible G1 reshard, never a silent one:
    /// `key = UTF-8 of String.concat "/" Segments`; `hash = FNV-1a-32` (offset
    /// basis `0x811c9dc5`, prime `16777619`, wrapping mod 2^32); result read
    /// UNSIGNED, `mod Count`. Never `Object.GetHashCode`. Same subject → same
    /// shard for a fixed `Count`.
    let shardOf (config: ShardConfig) (subject: ActorAddress) : ShardId =
        let count = if config.Count < 1 then 1 else config.Count
        let mutable hash = -2128831035 // 0x811c9dc5 (FNV offset basis) as a 32-bit int

        for byte in utf8Bytes (addressKey subject) do
            hash <- mulFnvPrime (hash ^^^ byte)

        ShardId(int (toUnsigned hash % float count))

    /// I3 — derived (never random) shard-stream subject: `"{ns}/wake/{shardId}"`.
    let shardSubject (config: ShardConfig) (ShardId id) : SubjectHistory.SubjectId =
        SubjectHistory.SubjectId(sprintf "%s/wake/%d" config.Namespace id)

    /// Convenience: the shard stream a subject's wakes land on (`shardOf` then
    /// `shardSubject`).
    let subjectShard (config: ShardConfig) (subject: ActorAddress) : SubjectHistory.SubjectId =
        shardSubject config (shardOf config subject)

    // ---- Producer side ----------------------------------------------------

    /// Post a wake for `subject`: open-append the pointer to its shard stream (the
    /// router's mailbox). ANY process may post — the shard stream is an
    /// open-append mailbox; delivery to the target is the router's job. Dedupe is
    /// the router's cursor + idempotent drive, not the poster's concern; N posts
    /// for one subject cost at most N idempotent drives.
    let post
        (basin: S2.Basin)
        (config: ShardConfig)
        (subject: ActorAddress)
        (reason: WakeReason)
        : Async<Result<unit, S2Errors.S2Failure>> =
        async {
            let shard = subjectShard config subject
            let (SubjectHistory.SubjectId name) = shard

            try
                do! S2.ensureStream name basin
                let! _ = SubjectHistory.append basin codec shard [ { Subject = subject; Reason = reason } ]
                return Ok()
            with error ->
                return Error(S2Errors.classify error)
        }
