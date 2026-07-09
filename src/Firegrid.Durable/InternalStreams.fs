/// ═══════════════════════════════════════════════════════════════════════
/// Firegrid.Durable — stream-native internals (G3 green-making).
///
/// Machinery for section 9 of the contract (logs, graded projection reads,
/// CEL waits), composed over the L1 kernel:
///
///   • SLog        — the L2 durable log: header-tagged chunk/terminal records
///                   with byte-faithful BODIES (a projection over the same
///                   address folds exactly what was appended — the kernel
///                   DurableLog's body envelope would leak into folds), CAS
///                   appends (seal-checked), and the openCursorWithWait
///                   tailing idiom for attach.
///   • GradedReads — Eventual / Through composed from the kernel StateReads
///                   (resident StateView fold + check-tail barriers); lag is
///                   returned as data.
///   • CelPredicate— a minimal, honest CEL subset (comparisons, boolean ops,
///                   literals, `state.<Field>` paths) parsed at registration
///                   time, evaluated over the live fold state.
///   • CelWatch    — the parked-wait wake path: registrations are durable
///                   facts in the instance journal (kernel `LogEmitted`
///                   records); a per-basin watcher tails instance journals,
///                   folds each wait's source projection through StateReads,
///                   and resumes satisfied waits through the kernel signal
///                   mechanism (`DurableClient.raiseSignalFrom`, deduped by
///                   (source, seq) at the mailbox fold).
///
/// Contract-type-agnostic by design (same rule as Internal.fs): this file
/// compiles BEFORE the contract, so contract-typed glue lives in the
/// contract file's own section-9 bodies and `StreamsDerived`.
/// ═══════════════════════════════════════════════════════════════════════
namespace Firegrid.Durable.Internal

open Fable.Core
open Firegrid.Log
open Firegrid.Foundation
open Firegrid.Store.Foundation.Durable

// ── SLog: the L2 durable log ───────────────────────────────────────────────
//
// One S2 stream per address. Records carry the payload BYTES as the body and
// the record kind in a header: ("t","c") chunk · ("t","t") terminal. Appends
// are CAS (matchSeqNum) so an append can never interleave past a seal; the
// seal IS the terminal record. Attach is the kernel `openCursorWithWait`
// idiom: a long-poll pull cursor, re-opened from the current position when
// the wait window lapses — one loop, no consumer-side polling.

[<RequireQualifiedAccess>]
module internal SLog =
    let private ensured = System.Collections.Generic.HashSet<string>()

    let private ensureOnce (basin: S2.Basin) (name: string) : Async<unit> =
        async {
            let key = S2.basinName basin + "|" + name

            if not (ensured.Contains key) then
                do! basin |> S2.ensureStream name
                ensured.Add key |> ignore
        }

    let private isTerminal (record: S2.ReadRecord) =
        record.Headers |> List.exists (fun (k, v) -> k = "t" && v = "t")

    let private lastRecord (stream: S2.Stream) (tail: int64) : Async<S2.ReadRecord option> =
        async {
            if tail <= 0L then
                return None
            else
                let! records =
                    stream
                    |> S2.readWith
                        { S2.ReadOptions.empty with
                            Start = Some(S2.FromSeqNum(tail - 1L))
                            Count = Some 1
                            Clamp = true }

                return List.tryHead records
        }

    /// CAS append of one record: observe the tail, refuse if the last record
    /// is the terminal, append with matchSeqNum so a concurrent writer (or
    /// sealer) forces a re-check instead of an interleave.
    let private appendCas (basin: S2.Basin) (name: string) (record: S2.Record) : Async<S2.AppendAck> =
        async {
            do! ensureOnce basin name
            let stream = basin |> S2.stream name

            let rec attempt remaining =
                async {
                    let! tail = stream |> S2.checkTail
                    let! last = lastRecord stream tail.SeqNum

                    match last with
                    | Some sealRecord when isTerminal sealRecord ->
                        return failwith ("durable log is sealed: " + name)
                    | _ ->
                        let options = S2.AppendOptions.none |> S2.AppendOptions.matchSeqNum tail.SeqNum
                        let! appended = stream |> S2.tryAppendWith options [ record ]

                        match appended with
                        | Ok ack -> return ack
                        | Error(S2Errors.SeqNumMismatch _) when remaining > 0 -> return! attempt (remaining - 1)
                        | Error failure -> return failwith ("durable log append failed: " + string failure)
                }

            return! attempt 20
        }

    /// Append one chunk; the ack is the EXCLUSIVE version (applied-through
    /// convention): a read `Through` this version reflects this append.
    let append (basin: S2.Basin) (name: string) (data: string) : Async<float> =
        async {
            let! ack = appendCas basin name (S2.Record.textWith [ "t", "c" ] data)
            return float (ack.End.SeqNum + 1L)
        }

    /// Seal: append the terminal record. Every attach — current and future —
    /// ends with it; the CAS check refuses appends past it.
    let seal (basin: S2.Basin) (name: string) (reason: string) : Async<unit> =
        async {
            let! _ = appendCas basin name (S2.Record.textWith [ "t", "t" ] reason)
            return ()
        }

    /// A pull over the log: `Some(false, data)` per chunk, `Some(true,
    /// reason)` at the terminal (then `None` forever). Long-poll cursor,
    /// re-opened from the current position when the wait window lapses.
    let attach (basin: S2.Basin) (name: string) : unit -> Async<(bool * string) option> =
        let mutable cursor: S2.ReadCursor option = None
        let mutable position = 0L
        let mutable ended = false

        let openCursor () =
            async {
                let! opened =
                    basin
                    |> S2.stream name
                    |> S2.readCursor
                        { S2.ReadOptions.empty with
                            Start = Some(S2.FromSeqNum position)
                            WaitSecs = Some 1
                            IgnoreCommandRecords = true }

                cursor <- Some opened
                return opened
            }

        fun () ->
            async {
                if ended then
                    return None
                else
                    do! ensureOnce basin name

                    let rec pull () =
                        async {
                            let! live =
                                match cursor with
                                | Some existing -> async { return existing }
                                | None -> openCursor ()

                            let! record = S2.tryNext live

                            match record with
                            | Some data ->
                                position <- data.SeqNum + 1L

                                if isTerminal data then
                                    ended <- true
                                    do! S2.closeReadCursor live
                                    cursor <- None
                                    return Some(true, data.Body)
                                else
                                    return Some(false, data.Body)
                            | None ->
                                // Wait window lapsed: re-open from the current
                                // position and keep waiting (the kernel idiom).
                                do! S2.closeReadCursor live
                                cursor <- None
                                return! pull ()
                        }

                    return! pull ()
            }

// ── GradedReads: Eventual / Through over kernel StateReads ────────────────
//
// One resident reader (kernel StateView: a tailing fold pump) per
// (basin, projection, source). Eventual returns the local snapshot with the
// lag as data; Through waits until the fold has applied through the
// requested version (fast path local, else the kernel check-tail barrier).
// Latest stays on the contract file's existing stateless check-tail fold.

[<RequireQualifiedAccess>]
module internal GradedReads =
    let private stringCodec: SubjectHistory.Codec<string> =
        { Encode = id
          Decode = Ok }

    let private readers = System.Collections.Generic.Dictionary<string, obj>()

    let private readerKey (basin: S2.Basin) (projName: string) (streamName: string) =
        S2.basinName basin + "|" + projName + "|" + streamName

    let private ensureReader
        (basin: S2.Basin)
        (projName: string)
        (streamName: string)
        (initial: 'state)
        (apply: 'state -> string -> 'state)
        : Async<StateReads.Reader<string, 'state>> =
        async {
            let key = readerKey basin projName streamName

            match readers.TryGetValue key with
            | true, existing -> return unbox existing
            | _ ->
                do! basin |> S2.ensureStream streamName

                let! reader =
                    StateReads.start
                        basin
                        stringCodec
                        (SubjectHistory.SubjectId streamName)
                        (SubjectHistory.Seq 0L)
                        initial
                        (fun state (record: SubjectHistory.StoredRecord<string>) -> apply state record.Body)

                // An interleaved ensure may have won the race; keep the first.
                match readers.TryGetValue key with
                | true, existing ->
                    do! StateReads.stop reader
                    return unbox existing
                | _ ->
                    readers.[key] <- box reader
                    return reader
        }

    let private tailNumber (basin: S2.Basin) (streamName: string) : Async<int64> =
        async {
            let! tail = SubjectHistory.tail basin (SubjectHistory.SubjectId streamName)
            return SubjectHistory.versionNumber tail
        }

    /// Eventual: the local applied snapshot — a monotonic fold prefix — with
    /// `behind` = committed tail minus applied (staleness as data).
    let eventual
        (basin: S2.Basin)
        (projName: string)
        (streamName: string)
        (initial: 'state)
        (apply: 'state -> string -> 'state)
        : Async<'state * float * float> =
        async {
            let! reader = ensureReader basin projName streamName initial apply
            let! snapshot = StateReads.readEventual reader
            let applied = SubjectHistory.versionNumber snapshot.AppliedTail
            let! tail = tailNumber basin streamName
            return snapshot.State, float applied, float (max 0L (tail - applied))
        }

    /// Through v: read-your-writes — resolves once the fold has applied
    /// through `version` (an append ack or any handed-off AsOf).
    let through
        (basin: S2.Basin)
        (projName: string)
        (streamName: string)
        (initial: 'state)
        (apply: 'state -> string -> 'state)
        (version: float)
        : Async<'state * float * float> =
        async {
            let! reader = ensureReader basin projName streamName initial apply
            let! snapshot = StateReads.readThrough (SubjectHistory.Version(int64 version)) reader
            let applied = SubjectHistory.versionNumber snapshot.AppliedTail
            let! tail = tailNumber basin streamName
            return snapshot.State, float applied, float (max 0L (tail - applied))
        }

// ── CelPredicate: a minimal, honest CEL subset ─────────────────────────────
//
// Enough CEL to state the laws' predicates, no more: literals (numbers,
// strings, booleans, null), `state.<Field>` paths, comparisons
// (== != < <= > >=), boolean operators (&& || !), parentheses. Parsed once
// at registration time (an invalid predicate fails registration, never a
// running wait); evaluated over the live fold state via dynamic field
// access (Fable records are plain JS objects).

[<RequireQualifiedAccess>]
module internal CelPredicate =
    [<Emit("$0[$1]")>]
    let private jsGet (_target: obj) (_key: string) : obj = jsNative

    [<Emit("typeof $0")>]
    let private jsTypeof (_value: obj) : string = jsNative

    [<Emit("$0 === $1")>]
    let private jsStrictEq (_left: obj) (_right: obj) : bool = jsNative

    [<Emit("$0 == null")>]
    let private jsIsNull (_value: obj) : bool = jsNative

    type private Tok =
        | TId of string
        | TNum of float
        | TStr of string
        | TBool of bool
        | TNull
        | TOp of string
        | TDot
        | TLParen
        | TRParen

    type private Expr =
        | ELit of obj
        | EPath of string list
        | ENot of Expr
        | EAnd of Expr * Expr
        | EOr of Expr * Expr
        | ECmp of op: string * left: Expr * right: Expr

    let private tokenize (text: string) : Tok list =
        let tokens = ResizeArray<Tok>()
        let mutable index = 0
        let length = text.Length
        let isIdentStart c = System.Char.IsLetter c || c = '_'
        let isIdent c = System.Char.IsLetterOrDigit c || c = '_'

        while index < length do
            let c = text.[index]

            if c = ' ' || c = '\t' || c = '\r' || c = '\n' then
                index <- index + 1
            elif isIdentStart c then
                let start = index

                while index < length && isIdent text.[index] do
                    index <- index + 1

                match text.Substring(start, index - start) with
                | "true" -> tokens.Add(TBool true)
                | "false" -> tokens.Add(TBool false)
                | "null" -> tokens.Add TNull
                | word -> tokens.Add(TId word)
            elif System.Char.IsDigit c then
                let start = index

                while index < length && (System.Char.IsDigit text.[index] || text.[index] = '.') do
                    index <- index + 1

                tokens.Add(TNum(float (text.Substring(start, index - start))))
            elif c = '\'' || c = '"' then
                let start = index + 1
                index <- index + 1

                while index < length && text.[index] <> c do
                    index <- index + 1

                if index >= length then
                    failwith "CEL: unterminated string literal"

                tokens.Add(TStr(text.Substring(start, index - start)))
                index <- index + 1
            elif c = '.' then
                tokens.Add TDot
                index <- index + 1
            elif c = '(' then
                tokens.Add TLParen
                index <- index + 1
            elif c = ')' then
                tokens.Add TRParen
                index <- index + 1
            else
                let pair = if index + 1 < length then text.Substring(index, 2) else ""

                match pair with
                | "==" | "!=" | "<=" | ">=" | "&&" | "||" ->
                    tokens.Add(TOp pair)
                    index <- index + 2
                | _ ->
                    match c with
                    | '<' | '>' ->
                        tokens.Add(TOp(string c))
                        index <- index + 1
                    | '!' ->
                        tokens.Add(TOp "!")
                        index <- index + 1
                    | other -> failwith (sprintf "CEL: unexpected character '%c'" other)

        List.ofSeq tokens

    type private Parser = { Toks: Tok[]; mutable Pos: int }

    let private peek (parser: Parser) : Tok option =
        if parser.Pos < parser.Toks.Length then Some parser.Toks.[parser.Pos] else None

    let private advance (parser: Parser) = parser.Pos <- parser.Pos + 1

    let rec private parseOr parser =
        let mutable left = parseAnd parser
        let mutable go = true

        while go do
            match peek parser with
            | Some(TOp "||") ->
                advance parser
                left <- EOr(left, parseAnd parser)
            | _ -> go <- false

        left

    and private parseAnd parser =
        let mutable left = parseCmp parser
        let mutable go = true

        while go do
            match peek parser with
            | Some(TOp "&&") ->
                advance parser
                left <- EAnd(left, parseCmp parser)
            | _ -> go <- false

        left

    and private parseCmp parser =
        let left = parseUnary parser

        match peek parser with
        | Some(TOp(("==" | "!=" | "<" | "<=" | ">" | ">=") as op)) ->
            advance parser
            ECmp(op, left, parseUnary parser)
        | _ -> left

    and private parseUnary parser =
        match peek parser with
        | Some(TOp "!") ->
            advance parser
            ENot(parseUnary parser)
        | _ -> parsePrimary parser

    and private parsePrimary parser =
        match peek parser with
        | Some(TNum value) ->
            advance parser
            ELit(box value)
        | Some(TStr value) ->
            advance parser
            ELit(box value)
        | Some(TBool value) ->
            advance parser
            ELit(box value)
        | Some TNull ->
            advance parser
            ELit null
        | Some TLParen ->
            advance parser
            let inner = parseOr parser

            match peek parser with
            | Some TRParen ->
                advance parser
                inner
            | _ -> failwith "CEL: expected ')'"
        | Some(TId name) ->
            advance parser
            let segments = ResizeArray<string>()
            segments.Add name
            let mutable go = true

            while go do
                match peek parser with
                | Some TDot ->
                    advance parser

                    match peek parser with
                    | Some(TId segment) ->
                        advance parser
                        segments.Add segment
                    | _ -> failwith "CEL: expected a field name after '.'"
                | _ -> go <- false

            EPath(List.ofSeq segments)
        | other -> failwith (sprintf "CEL: unexpected token %A" other)

    let private asBool (value: obj) : bool =
        if jsTypeof value = "boolean" then
            unbox<bool> value
        else
            failwith "CEL: predicate must evaluate to a boolean"

    let rec private eval (root: obj) (expr: Expr) : obj =
        match expr with
        | ELit value -> value
        | EPath [] -> failwith "CEL: empty path"
        | EPath(head :: rest) ->
            if head <> "state" then
                failwith ("CEL: unknown identifier '" + head + "' (predicates range over `state`)")
            else
                let mutable current = root

                for segment in rest do
                    if jsIsNull current then
                        failwith ("CEL: field access on null at '" + segment + "'")

                    current <- jsGet current segment

                current
        | ENot inner -> box (not (asBool (eval root inner)))
        | EAnd(left, right) -> box (asBool (eval root left) && asBool (eval root right))
        | EOr(left, right) -> box (asBool (eval root left) || asBool (eval root right))
        | ECmp(op, leftExpr, rightExpr) ->
            let left = eval root leftExpr
            let right = eval root rightExpr

            match op with
            | "==" -> box (jsStrictEq left right)
            | "!=" -> box (not (jsStrictEq left right))
            | _ ->
                let ordered =
                    if jsTypeof left = "number" && jsTypeof right = "number" then
                        compare (unbox<float> left) (unbox<float> right)
                    elif jsTypeof left = "string" && jsTypeof right = "string" then
                        System.String.CompareOrdinal(unbox<string> left, unbox<string> right)
                    else
                        failwith ("CEL: relational operands must both be numbers or strings (" + op + ")")

                match op with
                | "<" -> box (ordered < 0)
                | "<=" -> box (ordered <= 0)
                | ">" -> box (ordered > 0)
                | ">=" -> box (ordered >= 0)
                | other -> failwith ("CEL: unknown comparison " + other)

    /// Parse `text` NOW (registration-time validation) and return the
    /// evaluator. Throws on an invalid predicate or a non-boolean result.
    let compile (text: string) : obj -> bool =
        let parser = { Toks = tokenize text |> List.toArray; Pos = 0 }
        let expr = parseOr parser

        if parser.Pos <> parser.Toks.Length then
            failwith ("CEL: unexpected trailing tokens in predicate: " + text)

        fun state -> asBool (eval state expr)

// ── CelWatch: the parked-wait wake path ────────────────────────────────────
//
// A `Wait.state` lowers to (1) a kernel `Log` op — the registration as a
// DURABLE FACT in the instance journal ("celwait|<signal>|<source>|<pred>"),
// idempotent under replay by op id — and (2) a signal⊕timer race. This
// module is the other half: an in-process watcher per observed basin tails
// instance journals for open registrations, folds each wait's source stream
// (kernel StateReads), evaluates the predicate on relevant change, and
// resumes satisfied waits via `DurableClient.raiseSignalFrom` — the kernel
// parked-wait signal mechanism. Delivery is exactly-once: the mailbox fold
// dedupes on (source, seq), and the journaled `SignalReceived` serves every
// replay thereafter (a later state change never rewrites history).
//
// Specs (the fold + predicate closures) are captured at program
// construction, which runs in the worker process on every drive — so the
// watcher always has the closures for any wait a live worker can park.
// The watcher rides the worker: `Wiring.runWorker` notes the basin at
// start (architect ruling, PR #121), so any process hosting a worker —
// including a worker-only process — evaluates CEL waits.

[<RequireQualifiedAccess>]
module internal CelWatch =
    [<Emit("Math.imul($0, 16777619)")>]
    let private mulFnvPrime (_hash: int) : int = jsNative

    [<Emit("($0 >>> 0).toString(36)")>]
    let private toUnsignedBase36 (_hash: int) : string = jsNative

    [<Emit("Array.from(new TextEncoder().encode($0))")>]
    let private utf8Bytes (_text: string) : int[] = jsNative

    /// FNV-1a-32 of the predicate text (the WakeShard idiom): a stable,
    /// deployment-independent discriminator for the signal name.
    let private fnv (text: string) : string =
        let mutable hash = -2128831035 // 0x811c9dc5

        for byte in utf8Bytes text do
            hash <- mulFnvPrime (hash ^^^ byte)

        toUnsignedBase36 hash

    /// Deterministic signal name for a (projection, predicate) wait: the same
    /// wait resolves to the same name on every replay.
    let signalName (projName: string) (predicate: string) : string =
        "celw/" + projName + "/" + fnv predicate

    [<Literal>]
    let private registrationPrefix = "celwait|"

    /// The registration fact journaled with the wait (kernel `LogEmitted`).
    /// Deterministic — identical on every replay, so the journal carries it
    /// exactly once per wait position.
    let registrationMessage (signal: string) (sourceStream: string) (predicate: string) : string =
        registrationPrefix + signal + "|" + sourceStream + "|" + predicate

    /// The watch closures for one (projection, predicate) wait, captured at
    /// program construction in the driving process.
    type Spec =
        { Sig: string
          Stream: string
          Initial: obj
          Apply: obj -> string -> obj
          Pred: obj -> bool
          Enc: obj -> string }

    let private specs = System.Collections.Generic.Dictionary<string, Spec>()

    let registerSpec (spec: Spec) : unit = specs.[spec.Sig] <- spec

    // ---- Per-basin watcher --------------------------------------------------

    /// One open registration parked in one instance journal. Op offsets are
    /// fixed by the wait's lowering: log op L, then CurrentTime (L+1), then
    /// the race — signal L+2, timer L+3, cancel L+4.
    type private Reg =
        { Op: int
          Sig: string
          mutable Closed: bool
          mutable LastSent: float }

    type private LogWatch =
        { mutable Cursor: int64
          Regs: ResizeArray<Reg> }

    let private watchedBasins = System.Collections.Generic.HashSet<string>()

    [<Emit("Date.now()")>]
    let private nowMs () : float = jsNative

    /// Incremental read of an instance journal from `from` to the tail
    /// observed at entry (paginated; command records filtered). Returns the
    /// records and the advanced cursor.
    let private readJournal (basin: S2.Basin) (streamName: string) (from: int64) : Async<S2.ReadRecord list * int64> =
        async {
            try
                let stream = basin |> S2.stream streamName
                let! tail = stream |> S2.checkTail

                if from >= tail.SeqNum then
                    return [], from
                else
                    let rec page (cursor: int64) acc =
                        async {
                            if cursor >= tail.SeqNum then
                                return List.rev acc
                            else
                                let! records =
                                    stream
                                    |> S2.readWith
                                        { S2.ReadOptions.empty with
                                            Start = Some(S2.FromSeqNum cursor)
                                            Clamp = true
                                            IgnoreCommandRecords = true }

                                match List.rev records with
                                | [] -> return List.rev acc // only command records remain
                                | (last: S2.ReadRecord) :: _ ->
                                    let acc = (acc, records) ||> List.fold (fun state record -> record :: state)
                                    return! page (last.SeqNum + 1L) acc
                        }

                    let! records = page from []
                    return records, tail.SeqNum
            with error ->
                match S2Errors.classify error with
                | S2Errors.RangeNotSatisfiable _ -> return [], from
                | _ -> return raise error
        }

    let private parseRegistration (message: string) : string option =
        if message.StartsWith registrationPrefix then
            match message.Split '|' with
            | parts when parts.Length >= 2 -> Some parts.[1]
            | _ -> None
        else
            None

    /// Fold journal records into the watch state: new registrations open;
    /// a SignalReceived / TimerFired inside a wait's op window closes it.
    let private applyJournal (watch: LogWatch) (records: S2.ReadRecord list) =
        for record in records do
            match StepRecordCodec.decode record.Body with
            | Ok(Incoming(HistoryEvent event)) ->
                match event with
                | LogEmitted(OpId op, message) ->
                    match parseRegistration message with
                    | Some signal ->
                        let known =
                            watch.Regs |> Seq.exists (fun reg -> reg.Op = op && reg.Sig = signal)

                        if not known then
                            watch.Regs.Add { Op = op; Sig = signal; Closed = false; LastSent = 0.0 }
                    | None -> ()
                | SignalReceived(OpId op, _, _) ->
                    for reg in watch.Regs do
                        if not reg.Closed && op >= reg.Op + 1 && op <= reg.Op + 4 then
                            reg.Closed <- true
                | TimerFired(OpId op) ->
                    for reg in watch.Regs do
                        if not reg.Closed && op = reg.Op + 3 then
                            reg.Closed <- true
                | _ -> ()
            | _ -> () // not a step record / undecodable: not an instance journal record we track

    /// One watcher pass: discover instance journals, advance each journal
    /// watch, then evaluate every open registration whose spec is known and
    /// wake the satisfied ones.
    let private pass (basin: S2.Basin) (watches: System.Collections.Generic.Dictionary<string, LogWatch>) =
        async {
            let! streams = basin |> S2.listStreamsWith ""

            let journals =
                streams
                |> List.filter (fun stream -> stream.DeletedAt.IsNone && stream.Name.EndsWith "/log")

            for journal in journals do
                let watch =
                    match watches.TryGetValue journal.Name with
                    | true, existing -> existing
                    | _ ->
                        let created = { Cursor = 0L; Regs = ResizeArray() }
                        watches.[journal.Name] <- created
                        created

                let! records, cursor = readJournal basin journal.Name watch.Cursor
                applyJournal watch records
                watch.Cursor <- cursor

            for entry in watches do
                let journalName = entry.Key
                let watch = entry.Value

                for reg in watch.Regs do
                    if not reg.Closed then
                        match specs.TryGetValue reg.Sig with
                        | true, spec ->
                            // Fold the wait's source through the kernel reader;
                            // evaluate on the freshest applied state.
                            let! state, _, _ = GradedReads.eventual basin ("celw|" + reg.Sig) spec.Stream spec.Initial spec.Apply

                            let satisfied =
                                try
                                    spec.Pred state
                                with error ->
                                    Interop.consoleError ("Firegrid.Durable cel-wait predicate failed: " + error.Message)
                                    false

                            let now = nowMs ()

                            // Re-send while open at a low cadence (a restart may
                            // have lost the first send); the mailbox (source,
                            // seq) fold makes duplicates a no-op.
                            if satisfied && (reg.LastSent = 0.0 || now - reg.LastSent > 2_000.0) then
                                reg.LastSent <- now
                                let instanceKey = journalName.Substring(0, journalName.Length - 4)

                                let! delivery =
                                    DurableClient.raiseSignalFrom
                                        basin
                                        (InstanceId.create instanceKey)
                                        ("celw/" + string reg.Op)
                                        0L
                                        reg.Sig
                                        (spec.Enc state)

                                match delivery with
                                | DurableClientSignalStatus.Accepted _ -> ()
                                | DurableClientSignalStatus.Failed failure ->
                                    Interop.consoleError ("Firegrid.Durable cel-wait wake failed: " + string failure)
                        | _ -> () // spec not constructed in this process: another host owns the drive
        }

    let private watcherLoop (basin: S2.Basin) =
        let watches = System.Collections.Generic.Dictionary<string, LogWatch>()

        let rec loop (consecutiveErrors: int) =
            async {
                let! outcome = Async.Catch(pass basin watches)

                match outcome with
                | Choice1Of2 () ->
                    do! Interop.sleepUnref 120
                    return! loop 0
                | Choice2Of2 error when consecutiveErrors >= 4 ->
                    // Persistent infrastructure failure (the basin's backend is
                    // gone): stop quietly — durable state is safe.
                    Interop.consoleError ("Firegrid.Durable cel-wait watcher stopped: " + error.Message)
                | Choice2Of2 _ ->
                    do! Interop.sleepUnref 200
                    return! loop (consecutiveErrors + 1)
            }

        Async.StartAsPromise(loop 0) |> ignore

    /// Observe a basin (from any surface call that holds one) and ensure its
    /// watcher is running. Idempotent per basin.
    let noteBasin (basin: S2.Basin) : unit =
        if watchedBasins.Add(S2.basinName basin) then
            watcherLoop basin
