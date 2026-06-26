namespace Firegrid.Fluent

open Firegrid.Core

type StateBinding =
    { Name: string
      Predicate: StatePredicate option }

[<RequireQualifiedAccess>]
module State =
    let state name = { Name = name; Predicate = None }
