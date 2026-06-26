namespace Firegrid.Runtime

open Fable.Core
open Firegrid.Core

/// `createRunStoreAdapter` — wraps a `WorkflowRunStoreAdapterStore` into Core's
/// `RunStore`.
[<RequireQualifiedAccess>]
module RunStoreAdapter =

    let create (store: WorkflowRunStoreAdapterStore) : WorkflowRunStoreAdapter =
        { GetRunState = fun runId -> store.LoadRunState runId

          SetRunState = fun _runId state -> store.SaveRunState { State = state }

          DeleteRun = fun runId reason -> store.DeleteRun runId reason

          AppendEvent =
            fun runId expectedNextIndex event ->
                promise {
                    let! _ =
                        store.AppendEvents
                            { RunId = runId
                              ExpectedNextIndex = expectedNextIndex
                              Events = [| event |] }

                    return ()
                }

          GetEvents =
            fun runId ->
                promise {
                    let! events = store.ReadEvents { RunId = runId; FromIndex = None }
                    return events |> Array.map (fun e -> e.Event)
                }

          Subscribe =
            match store.SubscribeEvents with
            | Some subscribeEvents ->
                Some(fun runId fromIndex onEvent -> subscribeEvents runId fromIndex onEvent)
            | None -> None }
