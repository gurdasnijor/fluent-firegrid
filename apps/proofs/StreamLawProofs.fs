/// ═══════════════════════════════════════════════════════════════════════
/// T1 corpus — stream-native read laws (Packet 0.2 re-expression): sealed
/// logs, graded projection reads, CEL predicate waits, and the golden wire
/// fixtures. Law ids, semantics, and ratchet status FROZEN; only ADDITIVE
/// strength (trace checks, negative controls).
/// ═══════════════════════════════════════════════════════════════════════
namespace Firegrid.Foundation.Proofs

open Fable.Core
open Firegrid.Durable

module StreamLawProofs =

    // ── t1.golden-wire-fixtures ───────────────────────────────────────────
    // Serialization is DERIVED from your types, and the derived formats are
    // load-bearing: this law pins them against a committed golden fixture
    // (records → fields in declaration order; unions → case-NAME-tagged
    // arrays; renames are wire-breaking unless pinned with [<WireName>]).
    // Observation path: a projection over the entity's journal address
    // receives the raw records; each fixture payload must appear, in command
    // order, within those records. (Assumes entity journal address =
    // [ entityName; key ] — flagged for ratification.)
    type GoldenSnap = { Total: int; Tags: string list }

    type GoldenCmd =
        | Note of string
        | Tally of int * string
        | Snapshot
        | Rename of float

    type GoldenEvt =
        | Noted of note: string
        | Tallied of count: int * label: string
        | Snapshotted of snap: GoldenSnap
        | [<WireName("legacy-name")>] Renamed of value: float

    type GoldenState = { Applied: int }

    type GoldenObs =
        { MissingInOrder: string list
          FixtureLineCount: int }

    /// dist/Main.js -> ../fixtures/golden-wire.fixture.jsonl, so the law
    /// resolves the committed fixture from the compiled entry location, not
    /// the caller's cwd (the ratchet runner invokes suites from the repo
    /// root).
    let private fixturePath () =
        Reports.join [ CorpusNode.entryScript (); ".."; ".."; "fixtures"; "golden-wire.fixture.jsonl" ]

    let private goldenWorkload (tamperExpectation: bool) (ctx: WorkloadContext) : Async<GoldenObs> =
        ProofOperation.run
            ctx
            "t1.golden-wire-fixtures"
            {| Tampered = tamperExpectation |}
            { ProofOperationOptions.empty with
                Key = Some "golden-wire" }
            (async {
                let path = fixturePath ()

                if not (CorpusNode.exists path) then
                    failwith ("golden fixture missing: " + path)

                let fixtureLines =
                    CorpusNode.readFile path
                    |> fun text -> text.Split('\n')
                    |> Array.toList
                    |> List.filter (fun line -> line <> "" && not (line.StartsWith "#"))

                let expectedLines =
                    if tamperExpectation then
                        // NEGATIVE CONTROL ONLY: a knowingly-wrong fixture
                        // line — the wire-pinning detector must catch it.
                        fixtureLines @ [ "[\"Bogus\",\"never-on-the-wire\"]" ]
                    else
                        fixtureLines

                let golden =
                    Entity.define
                        "corpus/golden"
                        { Initial = { Applied = 0 }
                          Evolve = fun state _event -> { Applied = state.Applied + 1 }
                          Decide =
                            fun (Key _) command state ->
                                let events =
                                    match command with
                                    | Note text -> [ Noted text ]
                                    | Tally(n, label) -> [ Tallied(n, label) ]
                                    | Snapshot -> [ Snapshotted { Total = 3; Tags = [ "a"; "b" ] } ]
                                    | Rename value -> [ Renamed value ]

                                state.Applied + 1, events }

                let! basin = CorpusSupport.workloadBasin ctx "t1-golden"
                let! worker = Worker.run basin "t1-golden" [ reg golden ]
                let client = Client.connect basin

                let! _ = golden.Call client "gold-1" (Note "hello wire")
                let! _ = golden.Call client "gold-1" (Tally(2, "twice"))
                let! _ = golden.Call client "gold-1" Snapshot
                let! _ = golden.Call client "gold-1" (Rename 1.5)

                // Raw record capture: fold the journal's records as opaque
                // strings.
                let raw =
                    Projection.define "corpus/golden-raw" [ "corpus/golden"; "gold-1" ] ([]: string list) (fun acc record ->
                        acc @ [ record ])

                let! view = client.Read raw Latest
                let joined = String.concat "\n" view.State

                // Every derived payload appears, in command order.
                let missing = ResizeArray<string>()
                let mutable searchFrom = 0

                for line in expectedLines do
                    let index = joined.IndexOf(line, searchFrom)

                    if index >= 0 then
                        searchFrom <- index + line.Length
                    else
                        missing.Add line

                do! worker.Stop()

                return
                    { MissingInOrder = List.ofSeq missing
                      FixtureLineCount = List.length fixtureLines }
            })

    let private goldenChecks (v: Verifiers<GoldenObs>) : Check<GoldenObs> list =
        [ LawCheck.equal
              "derived wire format pinned: every fixture line appears, in command order"
              (fun o -> o.MissingInOrder)
              []
          // The committed fixture is load-bearing: it must actually pin the
          // four command payloads (guards against an emptied fixture file
          // silently weakening the law).
          LawCheck.equal "the committed fixture pins 4 payload lines" (fun o -> o.FixtureLineCount) 4
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t1.golden-wire-fixtures") ]

    // Negative control (wire family): a knowingly-wrong fixture line must
    // fail the wire-pinning detector.
    let private goldenTamperedControl =
        negativeControl<GoldenObs> "knowingly-wrong fixture line fails the wire-pinning check" {
            workload (goldenWorkload true)

            verify
                [ LawCheck.equal
                      "derived wire format pinned: every fixture line appears, in command order"
                      (fun o -> o.MissingInOrder)
                      [] ]

            expectFailure "derived wire format pinned"
        }

    let goldenWireFixtures =
        let lawProperty =
            property "t1.golden-wire-fixtures" {
                s2Lite ""
                timeoutMs 120_000
                workload (goldenWorkload false)
                verify goldenChecks
                negativeControl goldenTamperedControl
                requiresNegativeControl
            }

        proof "t1.golden-wire-fixtures" {
            describedAs
                "Derived wire encodings are pinned to the committed golden fixture: every fixture payload appears in the entity's journal records, in command order."

            property lawProperty
        }
