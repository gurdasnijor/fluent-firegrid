module Firegrid.Fluent.Exports

open Firegrid.Fluent

let workflow name = Definition.workflow name
let service name = Definition.service name
let object name = Definition.object name
let state name = State.state name
let run value = Run.sync value
let bindFluentDefinitions definitions = definitions
let workflowIdForHandler name = name
