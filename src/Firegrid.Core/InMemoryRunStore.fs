namespace Firegrid.Core

open System.Collections.Generic
open Fable.Core

/// In-memory backing store (`run-store/in-memory.ts`).
[<RequireQualifiedAccess>]
module InMemoryRunStore =

    type InMemoryRunStoreOptions =
        { Ttl: float option }

        static member Empty = { Ttl = None }

    type private Subscriber = WorkflowEvent -> int -> unit

    /// `inMemoryRunStore(options)` — returns a `RunStore` with subscribe.
    let create (options: InMemoryRunStoreOptions) : RunStore =
        // ttl ?? 60 * 60 * 1000
        let ttl = options.Ttl |> Option.defaultValue (60.0 * 60.0 * 1000.0)

        let runs = Dictionary<string, RunState>()
        let logs = Dictionary<string, ResizeArray<WorkflowEvent>>()
        let expirations = Dictionary<string, CoreSdk.TimeoutHandle>()
        let subscribers = Dictionary<string, HashSet<Subscriber>>()

        let scheduleExpiry (runId: string) (state: RunState option) =
            match expirations.TryGetValue runId with
            | true, existing -> CoreSdk.clearTimeout existing
            | _ -> ()

            // Paused runs are intentional persistence — exempt from TTL.
            match state with
            | Some s when s.Status = "paused" -> ()
            | _ ->
                let handle =
                    CoreSdk.setTimeout
                        (fun () ->
                            runs.Remove runId |> ignore
                            logs.Remove runId |> ignore
                            expirations.Remove runId |> ignore
                            subscribers.Remove runId |> ignore)
                        ttl

                expirations[runId] <- handle

        let getRunStateOpt (runId: string) : RunState option =
            match runs.TryGetValue runId with
            | true, v -> Some v
            | _ -> None

        { GetRunState = fun runId -> CoreSdk.promiseResolve (getRunStateOpt runId)
          SetRunState =
            fun runId state ->
                runs[runId] <- state
                scheduleExpiry runId (Some state)
                CoreSdk.promiseResolveUnit ()
          DeleteRun =
            fun runId _reason ->
                runs.Remove runId |> ignore
                logs.Remove runId |> ignore

                match expirations.TryGetValue runId with
                | true, handle -> CoreSdk.clearTimeout handle
                | _ -> ()

                expirations.Remove runId |> ignore
                subscribers.Remove runId |> ignore
                CoreSdk.promiseResolveUnit ()
          AppendEvent =
            fun runId expectedNextIndex event ->
                let log =
                    match logs.TryGetValue runId with
                    | true, l -> l
                    | _ -> ResizeArray<WorkflowEvent>()

                if float log.Count <> expectedNextIndex then
                    let existing =
                        let idx = int expectedNextIndex

                        if idx >= 0 && idx < log.Count then
                            Some(WorkflowEvent.toObj log[idx])
                        else
                            None

                    promise { return raise (LogConflictError.create runId expectedNextIndex existing) }
                else
                    log.Add event
                    logs[runId] <- log
                    scheduleExpiry runId (getRunStateOpt runId)

                    match subscribers.TryGetValue runId with
                    | true, subs ->
                        let index = log.Count - 1

                        for cb in List.ofSeq subs do
                            try
                                cb event index
                            with _ ->
                                () // Subscriber errors must not break the append.
                    | _ -> ()

                    CoreSdk.promiseResolveUnit ()
          GetEvents =
            fun runId ->
                let result =
                    match logs.TryGetValue runId with
                    | true, l -> l.ToArray()
                    | _ -> [||]

                CoreSdk.promiseResolve result
          Subscribe =
            Some(fun runId fromIndex onEvent ->
                let log =
                    match logs.TryGetValue runId with
                    | true, l -> l
                    | _ -> ResizeArray<WorkflowEvent>()

                for i in fromIndex .. log.Count - 1 do
                    try
                        onEvent log[i] i
                    with _ ->
                        ()

                let subs =
                    match subscribers.TryGetValue runId with
                    | true, s -> s
                    | _ ->
                        let s = HashSet<Subscriber>()
                        subscribers[runId] <- s
                        s

                subs.Add onEvent |> ignore

                fun () ->
                    subs.Remove onEvent |> ignore

                    if subs.Count = 0 then
                        subscribers.Remove runId |> ignore) }
