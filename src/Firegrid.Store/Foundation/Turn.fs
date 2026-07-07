namespace Firegrid.Store

open Firegrid.Foundation

/// One token-level output record on a turn stream — the same wire chunk the UI
/// consumes (MS-C2 decision: the turn stream carries the token sequence, so
/// attach is byte-faithful). `Blob` reserves the claim-check reference shape for
/// payloads too large for one S2 record; its resolution is deferred (MS-C2).
type TurnChunk =
    | Text of string
    | Blob of reference: string

/// How a turn ended. First-valid-terminal-wins is enforced by `DurableLog.seal`.
type TurnTerminal =
    | Completed
    | Failed of reason: string
    | Cancelled

/// Turn: a `DurableLog` binding — an address scheme plus chunk/terminal codecs,
/// and ZERO new methods. If a turn needs an operation `DurableLog` lacks, that is
/// gate G1/G6, not a `Turn` method. Producing and attaching go through
/// `DurableLog` with `Turn.codec` and `Turn.address`.
module Turn =
    type SessionId = SessionId of string
    type TurnId = TurnId of string

    /// `sessions/{sessionId}/turns/{turnId}` — deterministic, derived from ids
    /// the client already holds (matches agent-ui's `streamIdFor`).
    let address (SessionId session) (TurnId turn) : DurableLog.Address =
        { Segments = [ "sessions"; session; "turns"; turn ] }

    let private chunkCodec: SubjectHistory.Codec<TurnChunk> =
        { Encode =
            fun chunk ->
                match chunk with
                | Text text -> JsJson.stringify {| kind = "text"; text = text |}
                | Blob reference -> JsJson.stringify {| kind = "blob"; ref = reference |}
          Decode =
            fun body ->
                try
                    let parsed = JsJson.parse<obj> body

                    match JsJson.stringProp "kind" parsed with
                    | "text" -> Ok(Text(JsJson.stringProp "text" parsed))
                    | "blob" -> Ok(Blob(JsJson.stringProp "ref" parsed))
                    | other -> Error(sprintf "unknown turn chunk kind '%s'" other)
                with error ->
                    Error error.Message }

    let private terminalCodec: SubjectHistory.Codec<TurnTerminal> =
        { Encode =
            fun terminal ->
                match terminal with
                | Completed -> JsJson.stringify {| status = "completed" |}
                | Failed reason -> JsJson.stringify {| status = "failed"; reason = reason |}
                | Cancelled -> JsJson.stringify {| status = "cancelled" |}
          Decode =
            fun body ->
                try
                    let parsed = JsJson.parse<obj> body

                    match JsJson.stringProp "status" parsed with
                    | "completed" -> Ok Completed
                    | "failed" -> Ok(Failed(JsJson.stringProp "reason" parsed))
                    | "cancelled" -> Ok Cancelled
                    | other -> Error(sprintf "unknown turn terminal status '%s'" other)
                with error ->
                    Error error.Message }

    let codec: DurableLog.Codec<TurnChunk, TurnTerminal> =
        { Chunk = chunkCodec
          Terminal = terminalCodec }
