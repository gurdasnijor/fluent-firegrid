namespace Firegrid.Store.Foundation.Durable

[<RequireQualifiedAccess>]
module Activities =
    let create name input : Activity = { Name = name; Input = input }

[<RequireQualifiedAccess>]
module DurableTask =
    let activity name input =
        Activities.create name input |> RaceActivity

    let timer deadline = Timer deadline |> RaceEvent

    let signal name = Signal name |> RaceEvent

[<RequireQualifiedAccess>]
module Workflow =
    let call name input =
        Activities.create name input |> Durable.perform

    let all activities =
        activities |> List.ofSeq |> Durable.performAll

    let waitForSignal name = Signal name |> Durable.await

    let sleepUntil deadline =
        Timer deadline |> Durable.await |> Durable.map ignore

    let any tasks = tasks |> List.ofSeq |> Durable.whenAny

    let currentTime = Durable.currentTime

    let log message = Durable.log message

    /// Journaled fire-and-forget step invocation: the send is journaled and
    /// delivered at-least-once, but the caller never awaits completion and
    /// handler failures never surface to the calling workflow.
    let send name input =
        Activities.create name input |> Durable.send

    /// Start a child workflow from a journaled decision and resume with the
    /// child's terminal result once it is delivered back.
    let callChild workflow input = Durable.performChild workflow input

    /// Terminal generation rollover: journal a WorkflowContinuedAsNew record
    /// carrying the next generation's input; the next generation runs as a
    /// fresh instance with a fresh journal.
    let continueAsNew nextInput = Durable.continueAsNew nextInput

type DurableBuilder() =
    member _.Return value = Durable.result value

    member _.ReturnFrom program = program

    member _.Bind(program, binder) = Durable.bind binder program

    member _.Delay(generator: unit -> Durable<'a>) = generator ()

    member _.Zero() = Durable.result ()

    member _.Combine(first: Durable<unit>, second: Durable<'a>) = Durable.bind (fun () -> second) first

[<AutoOpen>]
module DurableSyntax =
    let durable = DurableBuilder()
