/// ═══════════════════════════════════════════════════════════════════════
/// Firegrid.Durable — example usages.
///
/// Companion to Firegrid.Durable.fs (the contract). Each module below is a
/// self-contained scenario, ordered from "hello" to a full agent system.
/// Review the API THROUGH these: if an example reads badly, the surface is
/// wrong — comment on the surface, not the example.
/// ═══════════════════════════════════════════════════════════════════════
namespace Firegrid.Durable.Examples

open Firegrid.Durable

// ══ 1. Hello, durable ═════════════════════════════════════════════════════
// One step, one workflow, a worker, a client. The whole lifecycle.
module Hello =

    let greet = Step.define "hello/greet" (fun (name: string) -> async {
        return sprintf "Hello, %s!" name })

    let hello = Workflow.define "hello/run" (fun (name: string) -> workflow {
        let! greeting = greet.Call name
        return greeting })

    let main basin = async {
        let! worker = Worker.run basin "demo" [ reg greet; reg hello ]
        let client = Client.connect basin

        let! run = hello.Start client "world" (Id "hello-1")
        let! result = run.Result          // Ok "Hello, world!"

        do! worker.Stop () }

// ══ 1b. Services — stateless durable request-response ═════════════════════
// The "start here" construct: no key, no state, unlimited concurrency —
// but every call is a durable execution (journaled steps, retries).
module Convert =

    let fetchRate = Step.define "fx/fetch-rate" (fun (pair: string) -> async { return 1.09 })

    let convert = Service.define "fx/convert" (fun (amount: float, pair: string) -> workflow {
        let! rate = fetchRate.CallWith (Backoff (3, Duration.seconds 1.0, 2.0)) pair
        return amount * rate })

    let demo basin = async {
        let client = Client.connect basin
        let! eur = convert.Call client (100.0, "USD/EUR")
        // Webhook-style dedupe: same key ⇒ same execution, one result.
        let! once = convert.CallIdempotent client "stripe-evt-819" (100.0, "USD/EUR")
        return eur, once }

// ══ 2. Checkout — fan-out, a human decision, a deadline ═══════════════════
module Checkout =

    type Order = { Id: string; Amount: float }
    type Receipt = Confirmed of reservation: string | Rejected of orderId: string
    type Decision = { Accepted: bool; By: string }

    let reserve  = Step.define "orders/reserve" (fun (o: Order) -> async { return "res-" + o.Id })
    let notify   = Step.define "orders/notify"  (fun (o: Order) -> async { return () })
    let approved = Signal.define<Decision> "orders/approved"

    let checkout = Workflow.define "orders/checkout" (fun (order: Order) -> workflow {
        // let! sequences; and! runs independently and joins (fan-out/fan-in)
        let! reservation = reserve.Call order
        and! ()          = notify.Call order

        // Waits time out as values — no exceptions, just match
        match! approved.Await (Duration.hours 48.0) with
        | Ok d when d.Accepted -> return Confirmed reservation
        | Ok _ | Error Timeout -> return Rejected order.Id })

    // Any other process approves it later — reattach by id:
    let approve basin decision = async {
        let client = Client.connect basin
        let run = Client.attach<Receipt> client (Id "order-42")
        do! run.Signal approved decision }

// ══ 3. Saga — compensation, and cancellation as a catchable value ═════════
module TripBooking =

    type Trip = { Car: string; Hotel: string; Flight: string }
    type Booking = Booked of confirmations: string list | RolledBack

    let reserveCar    = Step.define "travel/reserve-car"    (fun (c: string) -> async { return "car-ok" })
    let cancelCar     = Step.define "travel/cancel-car"     (fun (c: string) -> async { return () })
    let reserveHotel  = Step.define "travel/reserve-hotel"  (fun (h: string) -> async { return "hotel-ok" })
    let cancelHotel   = Step.define "travel/cancel-hotel"   (fun (h: string) -> async { return () })
    let reserveFlight = Step.define "travel/reserve-flight" (fun (f: string) -> async { return "flight-ok" })

    let bookTrip = Workflow.define "travel/book" (fun (trip: Trip) -> workflow {
        // Compensations accumulate as plain data; try/with works across
        // suspension points. DurableCancelled arrives here too — so a
        // cancelled booking rolls back, durably, even mid-crash.
        try
            let! car = reserveCar.CallWith (Backoff (5, Duration.seconds 1.0, 2.0)) trip.Car
            let! hotel = reserveHotel.Call trip.Hotel
            let! flight = reserveFlight.Call trip.Flight
            return Booked [ car; hotel; flight ]
        with
        | DurableStepFailed _ | DurableCancelled ->
            do! cancelHotel.Call trip.Hotel     // journaled: each compensation
            do! cancelCar.Call trip.Car         // runs exactly-once-effective
            return RolledBack })

    // Cancel from anywhere; the workflow above observes it at its next bind:
    let abort basin = async {
        let client = Client.connect basin
        do! (Client.attach<Booking> client (Id "trip-7")).Cancel () }

// ══ 4. Racing outcomes — tagged select ════════════════════════════════════
module Review =

    type Outcome = Approved of by: string | Expired | Withdrawn

    let decision  = Signal.define<string> "review/decision"
    let withdrawn = Signal.define<unit>   "review/withdrawn"

    let review = Workflow.define "review/run" (fun (docId: string) -> workflow {
        // First branch to finish wins; you match on YOUR union:
        let! outcome =
            Workflow.select [
                Approved            ^| decision.Await ()
                (fun () -> Expired) ^| Workflow.sleep (Duration.days 7.0)
                (fun () -> Withdrawn) ^| withdrawn.Await () ]
        return outcome })

// ══ 5. Time — reminders, delayed commands, schedules ══════════════════════
module Reminders =

    let sendEmail = Step.define "reminders/email" (fun (msg: string) -> async { return () })

    let remind = Workflow.define "reminders/one" (fun (at: Timestamp) -> workflow {
        do! Workflow.sleepUntil at            // durable: fires even if every
        do! sendEmail.Call "it's time"        // process restarted meanwhile
        return () })

// ══ 6. Eternal workflows — unbounded loops as generations ═════════════════
module Subscription =

    type SubState = { UserId: string; RenewedCount: int }

    let charge = Step.define "billing/charge" (fun (userId: string) -> async { return true })

    // Each generation: one renewal cycle. ContinueAsNew rolls the journal —
    // ten years of renewals never accumulate in one history.
    let subscription = Workflow.defineEternal "billing/subscription" (fun (state: SubState) -> workflow {
        do! Workflow.sleep (Duration.days 30.0)
        let! ok = charge.Call state.UserId
        if ok then
            return ContinueAsNew { state with RenewedCount = state.RenewedCount + 1 }
        else
            return Stop })

// ══ 7. Entities — durable keyed state, no locks anywhere ═════════════════
module Inventory =

    type Command = Reserve of qty: int | Restock of qty: int
    type Event   = Reserved of int | Restocked of int | ReservationRefused of int
    type Stock   = { OnHand: int }

    /// The caller gets an ANSWER, computed under exclusive state access:
    type Reply = Accepted of remaining: int | Refused of reason: string

    let decider : Decider<Command, Event, Stock, Reply> =
        { Initial = { OnHand = 0 }
          Evolve = fun stock -> function
            | Reserved n  -> { OnHand = stock.OnHand - n }
            | Restocked n -> { OnHand = stock.OnHand + n }
            | ReservationRefused _ -> stock
          // Exclusive handler: at most one Decide per key at a time, across
          // all hosts. Reply and events commit atomically under the fence.
          Decide = fun (Key sku) command stock ->
            match command with
            | Restock n -> Accepted (stock.OnHand + n), [ Restocked n ]
            | Reserve n when n <= stock.OnHand ->
                Accepted (stock.OnHand - n), [ Reserved n ]
            | Reserve n ->
                Refused (sprintf "%s: only %d on hand" sku stock.OnHand),
                [ ReservationRefused n ] }

    let inventory = Entity.define "shop/inventory" decider

    let demo basin = async {
        let client = Client.connect basin

        // EXCLUSIVE handlers — request-response or fire-and-forget; either
        // way serialized per key, deduplicated, durable, from any process:
        let! reply = inventory.Call client "sku-123" (Reserve 3)
        match reply with
        | Accepted remaining -> printfn "reserved; %d left" remaining
        | Refused reason     -> printfn "no: %s" reason
        do! inventory.Send client "sku-123" (Restock 10)
        do! inventory.SendAfter client "sku-123" (Duration.days 1.0) (Reserve 1)

        // SHARED handlers — concurrent reads that never block the writer,
        // with the staleness you're accepting made explicit:
        let! quick = inventory.State client "sku-123" Eventual   // fast, may lag
        let! exact = inventory.State client "sku-123" Latest     // linearizable
        return exact }

// ══ 8. Sharing contracts across processes ═════════════════════════════════
// The declaration is the contract. The caller's process never sees the body.
module Contracts =

    // shared library (both processes reference this):
    let scoreDecl = Workflow.declare<string, float> "ml/score"

    // worker process:
    let runWorker basin = async {
        let score = Worker.implement scoreDecl (fun text -> workflow {
            let! result = (Step.define "ml/infer" (fun (t: string) -> async { return 0.98 })).Call text
            return result })
        return! Worker.run basin "ml" [ reg score ] }

    // caller process (different binary, same declaration):
    let callIt basin = async {
        let client = Client.connect basin
        let! run = scoreDecl.Start client "hello" (Id "score-1")
        return! run.Result }

// ══ 9. The firegrid — a choreography-first agent system ══════════════════
// The model owns control flow; every decision point is one durable op.
module Agent =

    type ModelSays =
        | ToolUse of tool: string * args: string
        | NeedsApproval of what: string
        | FollowUpAt of at: Timestamp * selfPrompt: string
        | Delegate of subtasks: string list
        | Done of outcome: string

    type TurnInput = { SessionId: string; Prompt: string }

    let callModel = Step.define "agent/model" (fun (transcript: string list) -> async {
        return Done "…" })                                  // the harness call
    let runTool   = Step.define "agent/tool" (fun (tool: string, args: string) -> async {
        return "tool-output" })
    let approval  = Signal.define<bool> "agent/approval"

    let turnDecl = Workflow.declare<TurnInput, string> "agent/turn"

    let turn = Worker.implement turnDecl (fun input -> workflow {
        let rec drive (transcript: string list) = workflow {
            match! callModel.Call transcript with
            | Done outcome -> return outcome
            | ToolUse (tool, args) ->                        // execute(): journaled —
                let! output = runTool.Call (tool, args)      // never re-runs on replay
                return! drive (transcript @ [ output ])
            | NeedsApproval what ->                          // wait_for(): parks free,
                match! approval.Await (Duration.days 7.0) with   // pins no process
                | Ok true  -> return! drive (transcript @ [ "approved" ])
                | Ok false | Error Timeout -> return "declined"
            | FollowUpAt (at, selfPrompt) ->                 // wait_until(t, prompt)
                do! Workflow.sleepUntil at
                return! drive (transcript @ [ selfPrompt ])
            | Delegate subtasks ->                           // spawn_all(): durable
                let! results =                               // children, fan-in
                    Workflow.all [
                        for sub in subtasks ->
                            turnDecl.CallChild { input with Prompt = sub } ]
                return! drive (transcript @ results) }
        return! drive [ input.Prompt ] })

    // The session entity: which turn is live; cancel is just a command.
    type SessionCmd = StartTurn of turnId: string | CancelTurn of turnId: string
    type SessionEvt = TurnStarted of string | TurnCancelled of string
    type Session    = { LiveTurn: string option }

    let session =
        Entity.define "agent/session"
            { Initial = { LiveTurn = None }
              Evolve = fun s -> function
                | TurnStarted t   -> { LiveTurn = Some t }
                | TurnCancelled _ -> { LiveTurn = None }
              Decide = fun _key cmd s ->
                match cmd, s.LiveTurn with
                | StartTurn t, None    -> (), [ TurnStarted t ]
                | StartTurn _, Some _  -> (), []            // single live turn: policy
                | CancelTurn t, Some live when t = live -> (), [ TurnCancelled t ]
                | CancelTurn _, _      -> (), [] }          // idempotent

// ══ 10. Watching it run — logs and projections ════════════════════════════
module Observability =

    let watchTurn basin sessionId turnId = async {
        let client = Client.connect basin
        // Byte-faithful attach: recorded prefix → live tail → terminal,
        // one loop, no polling, from any process at any time.
        let log = client.Logs [ "agent"; sessionId; "turns"; turnId ]
        do! log.Attach () |> AsyncSeq.iter (fun ev ->
            match ev with
            | Chunk data       -> printfn "%s" data
            | Terminal reason  -> printfn "— turn ended: %s" reason) }

    // A dashboard view: fold facts into state, read at a chosen grade.
    let turnCounts =
        Projection.define "agent/turn-counts" [ "agent"; "events" ]
            (0, 0)                                          // (started, ended)
            (fun (s, e) fact ->
                if fact.Contains "TurnStarted" then (s + 1, e) else (s, e + 1))

    let dashboard basin = async {
        let client = Client.connect basin
        let! view = client.Read turnCounts Eventual
        // view.Behind says exactly how stale this is — staleness is data
        return view.State, view.Behind }
