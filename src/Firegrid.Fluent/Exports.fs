module Firegrid.Fluent.Exports

open Effect
open Fable.Core.JS
open Firegrid.Core
open Firegrid.Runtime
open Firegrid.Fluent

// index.ts also re-exports `FluentFiregridError` from `@firegrid/core`. F#/Fable
// consumers should `open Firegrid.Core` for that surface; the fluent-specific
// surface follows, mirroring `src/index.ts`.

// ===== error =====
let fluentFiregridError (message: string) : exn = FluentFiregridError.create message

// ===== bindTanStack =====
let workflowIdForHandler = BindTanStack.workflowIdForHandler

let bindFluentDefinitions (definitions: Definition[]) (options: FluentDefinitionBindingOptions) : WorkflowRegistrationMap =
    BindTanStack.bindFluentDefinitions definitions options

let createTanStackExternalSignalBinding = BindTanStack.createTanStackExternalSignalBinding
let createTanStackRuntimeBinding = BindTanStack.createTanStackRuntimeBinding

// ===== clients =====
let duration = Clients.duration
let rpc = Clients.rpc
let invocation = Clients.invocation
let genericCall = Clients.genericCall
let genericCallWithBinding = Clients.genericCallWithBinding
let genericSend = Clients.genericSend
let genericSendWithBinding = Clients.genericSendWithBinding
let attach = Clients.attach
let attachWithBinding = Clients.attachWithBinding
let client = Clients.client
let sendClient = Clients.sendClient
let serviceClient = Clients.serviceClient
let serviceClientWithBinding = Clients.serviceClientWithBinding
let workflowClient = Clients.workflowClient
let workflowClientWithBinding = Clients.workflowClientWithBinding
let sendServiceClient = Clients.sendServiceClient
let sendServiceClientWithBinding = Clients.sendServiceClientWithBinding
let serviceSendClient = Clients.serviceSendClient
let sendWorkflowClient = Clients.sendWorkflowClient
let workflowSendClient = Clients.workflowSendClient
let objectClient = Clients.objectClient
let objectClientWithKey = Clients.objectClientWithKey
let objectClientWithBinding = Clients.objectClientWithBinding
let objectClientWithBindingKey = Clients.objectClientWithBindingKey
let sendObjectClient = Clients.sendObjectClient
let sendObjectClientWithKey = Clients.sendObjectClientWithKey
let sendObjectClientWithBinding = Clients.sendObjectClientWithBinding
let sendObjectClientWithBindingKey = Clients.sendObjectClientWithBindingKey
let objectSendClient = Clients.objectSendClient

// ===== combinators =====
let orTimeout = Combinators.orTimeout

// ===== context =====
let fluentContextFromTanStack = Context.fluentContextFromTanStack
let fluentDurableContextTag = FluentDurableContext.tag

// ===== definitions =====
let cron = Definitions.cron

module every =
    let milliseconds = Definitions.every.milliseconds
    let seconds = Definitions.every.seconds
    let minutes = Definitions.every.minutes
    let hours = Definitions.every.hours

let json = Definitions.json
let schemas = Definitions.schemas
let serdes = Definitions.serdes
let schedule = Definitions.schedule
let service = Definitions.service
let object = Definitions.object
let workflow = Definitions.workflow

// ===== externalEvents =====
let decodeAwakeableToken = ExternalEvents.decodeAwakeableToken
let awakeable = ExternalEvents.awakeable
let workflowEvent = ExternalEvents.workflowEvent
let resolveAwakeable = ExternalEvents.resolveAwakeable
let resolveAwakeableWithBinding = ExternalEvents.resolveAwakeableWithBinding
let rejectAwakeable = ExternalEvents.rejectAwakeable
let rejectAwakeableWithBinding = ExternalEvents.rejectAwakeableWithBinding
let resolveWorkflowEvent = ExternalEvents.resolveWorkflowEvent
let resolveWorkflowEventWithBinding = ExternalEvents.resolveWorkflowEventWithBinding

// ===== interface (also exposed as `iface` in index.ts) =====
module iface =
    let service = Interface.service
    let object = Interface.object
    let workflow = Interface.workflow
    let implement = Interface.implement
    let json = Interface.json
    let schemas = Interface.schemas
    let serdes = Interface.serdes

let implement = Interface.implement

// ===== run =====
let run = Run.run
let sleep = Run.sleep
let sleepUntil = Run.sleepUntil
let waitForSignal = Run.waitForSignal
let objectKey = Run.objectKey

// ===== state / statePredicate =====
let cel = StatePredicate.cel
let celExpr = StatePredicate.celExpr
let validateStatePredicate = StatePredicate.validateStatePredicate
let evaluateStatePredicate = StatePredicate.evaluateStatePredicate

let table = State.table
let state = State.state
let celFor = State.celFor
let stateIndexKey = State.stateIndexKey
let statePredicateEnvironment = State.statePredicateEnvironment
let validateStatePredicateForEnvironment = State.validateStatePredicateForEnvironment
let validateStatePredicateForTable = State.validateStatePredicateForTable

// ===== http (also exported via the "./http" subpath in the TS) =====
let createAwakeableHttpClient = Http.createAwakeableHttpClient

let createFluentHttpHandler (options: FluentHttpHandlerOptions) : (obj -> Promise<obj>) =
    Http.createFluentHttpHandler options
