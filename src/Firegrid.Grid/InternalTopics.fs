/// ═══════════════════════════════════════════════════════════════════════
/// Firegrid.Grid — topics, publish, and ingress (Phase C / C4 green-making).
///
/// Implementation behind `Grid.Publish` / `Session.Deliver` and the topic
/// half of `wait_for`, lowered ONLY onto the public Firegrid.Durable (L2)
/// surface exactly as the ratified annotations promise:
///
///   topic          → entity "grid/topic" (keyed by topic name): the
///                    subscriber roster and the published-event record are
///                    the topic's own journal — the topic log IS the
///                    coordination [→ Entity.define]
///   subscribe      → topic entity durable FIFO inbox admission
///                    [→ entity .Send]: the subscription is durably
///                    admitted BEFORE the park is journaled, and inbox
///                    FIFO order guarantees every subscription admitted
///                    before a publish is visible to that publish's decide
///   publish        → the Publish SERVICE ("grid/publish"): the ingress
///                    ack is the service execution's DURABLE ADMISSION —
///                    journaled start, no worker involved (the webhook law
///                    stops every worker first) [→ Service/Workflow start];
///                    execution (worker-hosted, later): topic entity lists
///                    the matching subscribers → Run.Signal each (fan-out)
///                    [→ Entity + run.Signal]
///   wake delivery  → a typed signal on the parked turn's run
///                    [→ Signal.define / run.Signal], the same mechanic as
///                    the approval gate (InternalApproval.fs)
///
/// Worker-pass discipline (why no step here ever AWAITS an entity reply):
/// the kernel activity adapter completes a due batch inside the workflow
/// tick, and the worker pass drives inbox keys sequentially — a step
/// handler that awaited an entity reply would block the very pass that
/// drives the entity. Every cross-key handoff is therefore a durable
/// append (entity Send / workflow Start / run Signal) that acks
/// immediately, and the publish program observes the entity's fold through
/// journaled client-side reads separated by durable sleeps (each sleep
/// yields the tick so the pass can drive the entity).
///
/// Fold-poll cadence (CI robustness — the self-feeding drive spin): a due
/// timer counts as worker progress, so the drive keeps ticking the SAME
/// workflow while its timers keep coming up due. On a loaded runner a
/// single tick's wall latency exceeds a short fixed sleep — the poll loop
/// then re-arms an already-due timer every tick and monopolizes the pass
/// for the drive's whole tick budget, starving the very entity drive the
/// poll is waiting on (observed on CI and reproduced under a CPU
/// throttle: ~100 polls/~100 s before the fold landed). The poll sleeps
/// therefore ESCALATE — short first (fast machines detect the fold in one
/// or two passes, unchanged), doubling to a 2 s cap (a loaded tick can no
/// longer outrun its own timer, so the loop parks Waiting and the pass
/// moves on to drive the entity) — and every fold-poll is BOUNDED by the
/// law-timeout headroom (~2 min of cumulative sleep), failing terminal
/// instead of polling forever, so an execution abandoned by a dead
/// workload can never outlive its trial as a zombie loop.
///
/// Nothing in this file reaches below the L2 contract.
/// ═══════════════════════════════════════════════════════════════════════
namespace Firegrid

open Fable.Core
open Firegrid.Durable

// ── Topic-entity wire types ────────────────────────────────────────────────

/// One parked subscription: which run to wake, through which typed signal,
/// under which match predicate ("" = match every event on the topic).
type internal TopicSub =
    { SubRun: string
      SubSignal: string
      SubMatch: string }

type internal TopicCommand =
    | TSubscribe of run: string * signal: string * matchText: string
    | TPublish of id: string * payload: string

type internal TopicEvent =
    | TEvSubscribed of run: string * signal: string * matchText: string
    | TEvPublished of id: string * matched: TopicSub list

type internal TopicPubRec =
    { PubRecId: string
      PubRecMatched: TopicSub list }

type internal TopicState =
    { TSubs: TopicSub list
      TPubs: TopicPubRec list }

type internal TopicReply = TopicAck

/// The wake event a topic delivers to a parked run (the typed signal's
/// payload): the published event, verbatim.
type internal TopicWake = { WakeTopic: string; WakePayload: string }

// ── Publish-service wire types ─────────────────────────────────────────────

/// Input of the "grid/publish" service. `PubId` is the execution's identity
/// (idempotency: a re-admitted publish folds once at the topic entity).
type internal PublishIn =
    { PubId: string
      PubTopic: string
      PubPayload: string }

type internal TopicPollIn = { PlTopic: string; PlId: string }
type internal TopicPollOut = { PlFound: bool; PlSubs: TopicSub list }

type internal DeliverIn =
    { DlTopic: string
      DlPayload: string
      DlSubs: TopicSub list }

type internal SubscribeIn =
    { SbTopic: string
      SbRun: string
      SbSignal: string
      SbMatch: string }

// ── The topic runtime ──────────────────────────────────────────────────────

module internal GridTopics =
    [<Emit("Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 10)")>]
    let private entropy () : string = jsNative

    // ── Fold-poll cadence (see the header note) ───────────────────────────

    /// The escalating poll-sleep ladder: 0.12 s doubling to a 2 s cap.
    /// Deterministic in the attempt ordinal, so replays walk the same tree.
    let pollDelaySeconds (attempt: int) : float =
        match attempt with
        | 0 -> 0.12
        | 1 -> 0.24
        | 2 -> 0.48
        | 3 -> 0.96
        | _ -> 2.0

    /// Attempt bound ≈ the law-timeout headroom: cumulative sleep across
    /// 64 rungs ≈ 122 s (the harness property budget is 120 s). A fold not
    /// observed within it is a terminal failure, never an eternal poll.
    let pollAttemptBudget: int = 64

    // ── Match evaluation (pure — runs inside the topic entity's Decide) ──
    //
    // T2 scope: the corpus's ratified match shape is `<field> == '<value>'`
    // (GridExamples §4: the model publishes payloads that carry their
    // sub-topic as a leading "<label>: " prefix — the corpus mirror of the
    // example's `{topic: "s2-perf", …}` field). An event matches when the
    // quoted value equals the payload's leading label or the topic name
    // itself; a malformed predicate matches nothing (the law observing the
    // missed wake fails loud). Full CEL evaluation is the T3 adapter
    // packet's schema work.

    let private payloadLabel (payload: string) : string =
        match payload.IndexOf ':' with
        | index when index > 0 -> payload.Substring(0, index).Trim()
        | _ -> ""

    let matchAccepts (matchText: string) (topic: string) (payload: string) : bool =
        if matchText = "" then
            true
        else
            match matchText.Split('\'') with
            | [| _; value; _ |] -> payloadLabel payload = value || topic = value
            | _ -> false

    // ── The topic entity: roster + publish record, FIFO-admitted ─────────
    //
    // Decide is pure. Publishing computes the matched subscribers AT FOLD
    // TIME — after every subscription that entered the FIFO inbox before
    // the publish — and journals them with the event; matched
    // subscriptions are consumed (wait_for is a one-shot park). Duplicate
    // subscribes and duplicate publish ids fold once.

    let private topicDecider: Decider<TopicCommand, TopicEvent, TopicState, TopicReply> =
        { Initial = { TSubs = []; TPubs = [] }
          Evolve =
            fun state event ->
                match event with
                | TEvSubscribed(run, signal, matchText) ->
                    { state with
                        TSubs =
                            state.TSubs
                            @ [ { SubRun = run
                                  SubSignal = signal
                                  SubMatch = matchText } ] }
                | TEvPublished(id, matched) ->
                    { TSubs =
                        state.TSubs
                        |> List.filter (fun sub ->
                            not (
                                matched
                                |> List.exists (fun m -> m.SubRun = sub.SubRun && m.SubSignal = sub.SubSignal)
                            ))
                      TPubs =
                        state.TPubs
                        @ [ { PubRecId = id
                              PubRecMatched = matched } ] }
          Decide =
            fun (Key topic) command state ->
                match command with
                | TSubscribe(run, signal, matchText) ->
                    if state.TSubs |> List.exists (fun s -> s.SubRun = run && s.SubSignal = signal) then
                        TopicAck, []
                    else
                        TopicAck, [ TEvSubscribed(run, signal, matchText) ]
                | TPublish(id, payload) ->
                    if state.TPubs |> List.exists (fun p -> p.PubRecId = id) then
                        TopicAck, []
                    else
                        let matched =
                            state.TSubs |> List.filter (fun s -> matchAccepts s.SubMatch topic payload)

                        TopicAck, [ TEvPublished(id, matched) ] }

    let topicEntity: EntityDef<TopicCommand, TopicEvent, TopicState, TopicReply> =
        Entity.define "grid/topic" topicDecider

    // ── The wake signal (typed-signal park, the approval-gate mechanic) ──

    let wakeSignalName (topic: string) : string = "grid/topic-wake/" + topic

    let wakeSignal (topic: string) : Signal<TopicWake> =
        Signal.define<TopicWake> (wakeSignalName topic)

    /// The parked wait_for trace text: names the topic (the trace is the
    /// coordination) plus the match, mirroring the ratified Ops example
    /// ("findings: topic == 's2-perf'").
    let waitText (topic: string) (matchText: string option) : string =
        match matchText with
        | Some text -> topic + ": " + text
        | None -> topic

    /// The wake turn's input: the published event plus the waiter's
    /// self-prompt, one line (the corpus records it verbatim through its
    /// recording tool).
    let wakeInputText (wake: TopicWake) (selfPrompt: string option) : string =
        match selfPrompt with
        | Some prompt -> wake.WakePayload + " -- " + prompt
        | None -> wake.WakePayload

    // ── The Publish service ("grid/publish") ─────────────────────────────

    let serviceName = "grid/publish"

    /// Turn-side subscribe step: durable FIFO admission into the topic's
    /// inbox. Send (not Call): the ack means durably admitted, and FIFO
    /// order — not a reply — is what publish correctness needs.
    let subscribeStep (client: Client) : Step<SubscribeIn, unit> =
        Step.define "topic/subscribe" (fun (input: SubscribeIn) ->
            topicEntity.Send client input.SbTopic (TSubscribe(input.SbRun, input.SbSignal, input.SbMatch)))

    /// Publish-service step 1: durably enqueue the publish into the topic's
    /// FIFO inbox (behind every already-admitted subscription).
    let enqueueStep (client: Client) : Step<PublishIn, unit> =
        Step.define "topic/enqueue" (fun (input: PublishIn) ->
            topicEntity.Send client input.PubTopic (TPublish(input.PubId, input.PubPayload)))

    /// Publish-service step 2: observe the topic entity's fold — a
    /// client-side journal read (never blocks the worker pass, never takes
    /// the fence); the publish program sleeps durably between polls.
    let pollStep (client: Client) : Step<TopicPollIn, TopicPollOut> =
        Step.define "topic/poll" (fun (input: TopicPollIn) ->
            async {
                let! state = topicEntity.State client input.PlTopic Latest

                return
                    match state.TPubs |> List.tryFind (fun p -> p.PubRecId = input.PlId) with
                    | Some pub ->
                        { PlFound = true
                          PlSubs = pub.PubRecMatched }
                    | None -> { PlFound = false; PlSubs = [] }
            })

    /// Publish-service step 3: fan the event out — Run.Signal each matched
    /// subscriber's parked run (durable appends; a crash-window re-run
    /// re-signals, and a park consumes exactly one delivery while the wake
    /// turn's identity dedupes the rest).
    let deliverStep (client: Client) : Step<DeliverIn, unit> =
        Step.define "topic/deliver" (fun (input: DeliverIn) ->
            async {
                for sub in input.DlSubs do
                    let run = Client.attach<string> client (Id sub.SubRun)

                    do!
                        run.Signal
                            (Signal.define<TopicWake> sub.SubSignal)
                            { WakeTopic = input.DlTopic
                              WakePayload = input.DlPayload }
            })

    /// The Publish service program: enqueue (FIFO, behind all prior
    /// subscriptions) → await the fold (journaled poll + escalating durable
    /// sleep; the sleep yields the tick so the worker pass can drive the
    /// topic entity — see the fold-poll cadence note in the header) →
    /// deliver to the matched subscribers.
    let publishProgram
        (enqueue: Step<PublishIn, unit>)
        (poll: Step<TopicPollIn, TopicPollOut>)
        (deliver: Step<DeliverIn, unit>)
        (input: PublishIn)
        : Workflow<unit> =

        let rec awaitFold (attempt: int) : Workflow<TopicSub list> =
            workflow {
                let! polled = poll.Call { PlTopic = input.PubTopic; PlId = input.PubId }

                if polled.PlFound then
                    return polled.PlSubs
                elif attempt >= pollAttemptBudget then
                    return
                        raise (
                            DurableStepFailed(
                                StepError.Terminal(
                                    "Firegrid: publish '"
                                    + input.PubId
                                    + "' was not folded by topic '"
                                    + input.PubTopic
                                    + "' within the poll budget"
                                )
                            )
                        )
                else
                    do! Workflow.sleep (Duration.seconds (pollDelaySeconds attempt))
                    return! awaitFold (attempt + 1)
            }

        workflow {
            do! enqueue.Call input
            let! matched = awaitFold 0

            do!
                deliver.Call
                    { DlTopic = input.PubTopic
                      DlPayload = input.PubPayload
                      DlSubs = matched }
        }

    let publishService
        (enqueue: Step<PublishIn, unit>)
        (poll: Step<TopicPollIn, TopicPollOut>)
        (deliver: Step<DeliverIn, unit>)
        : ServiceDef<PublishIn, unit> =
        Service.define serviceName (publishProgram enqueue poll deliver)

    // ── Ingress: durable ack, zero worker involvement ─────────────────────
    //
    // `Grid.Publish` / `Session.Deliver` ride the Publish service's
    // durable-admission half: starting the service's execution journals it
    // with NO worker involved (the webhook law publishes while every
    // worker is stopped), and a worker executes the fan-out later. The
    // execution id doubles as the topic-entity dedupe id, so a crash-window
    // re-admission cannot double-publish.

    let ingress (client: Client) (pubId: string) (topic: string) (payload: string) : Async<unit> =
        async {
            let decl = Workflow.declare<PublishIn, unit> serviceName

            let! _run =
                decl.Start
                    client
                    { PubId = pubId
                      PubTopic = topic
                      PubPayload = payload }
                    (Id pubId)

            return ()
        }

    /// System-level ingress (webhooks, transports): one publish per call.
    let ingressAuto (client: Client) (topic: string) (payload: string) : Async<unit> =
        ingress client ("pub/ing-" + entropy ()) topic payload

    /// Turn-side publish step: the model's publish move — the id derives
    /// from the turn and move position, so replay and crash-window re-runs
    /// admit the same execution.
    let publishMoveStep (client: Client) : Step<PublishIn, unit> =
        Step.define "topic/publish" (fun (input: PublishIn) ->
            ingress client input.PubId input.PubTopic input.PubPayload)
