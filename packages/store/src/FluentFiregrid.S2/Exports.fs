module Firegrid.FluentFiregrid.S2.Exports

open Effect
open Firegrid.FluentFiregrid.S2

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

let appendStateEventJson (runtime: S2Runtime) (append: S2StateAppend) : Effect<S2AppendAck, S2Error, unit> =
    S2ObjectState.appendEventJson runtime append

let readStateEventJson (runtime: S2Runtime) (read: S2StateRead) : Effect<string list, S2Error, unit> =
    S2ObjectState.readEventJson runtime read
