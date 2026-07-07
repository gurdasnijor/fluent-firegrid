module Firegrid.Store.Exports

open Firegrid.Log
open Firegrid.Store

let createS2Runtime (config: S2ObjectStateBackendConfig) : S2Runtime = Runtime.create config

let objectStateStreamName (config: S2ObjectStateBackendConfig) (address: S2ObjectStateAddress) : string =
    let ns = config.Namespace |> Option.defaultValue "default"
    Naming.objectStateStreamName ns address

let objectInvocationStreamName (config: S2ObjectStateBackendConfig) (address: S2ObjectStateAddress) : string =
    let ns = config.Namespace |> Option.defaultValue "default"
    Naming.objectInvocationStreamName ns address

let delayedStartStreamName (config: S2ObjectStateBackendConfig) : string =
    let ns = config.Namespace |> Option.defaultValue "default"
    Naming.delayedStartStreamName ns

let stateStreamTarget (runtime: S2Runtime) (address: S2ObjectStateAddress) : S2StreamRef =
    S2ObjectState.target runtime address

let appendStateEventJson (runtime: S2Runtime) (append: S2StateAppend) : Async<S2.AppendAck> =
    S2ObjectState.appendEventJson runtime append

let readStateEventJson (runtime: S2Runtime) (read: S2StateRead) : Async<string list> =
    S2ObjectState.readEventJson runtime read

let runEventsStreamName (runtime: S2Runtime) (runId: RunId) : string =
    WorkflowLog.eventsStreamName runtime runId

let appendEvents (runtime: S2Runtime) (args: AppendEventsArgs) : Async<AppendEventsResult> =
    WorkflowLog.appendEvents runtime args

let readEvents (runtime: S2Runtime) (args: ReadEventsArgs) : Async<StoredWorkflowEvent list> =
    WorkflowLog.readEvents runtime args
