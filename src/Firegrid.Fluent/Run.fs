namespace Firegrid.Fluent

open Effect
open Firegrid.Core

type RunOptions =
    { IdempotencyKey: string option }

[<RequireQualifiedAccess>]
module Run =
    let sync value : Effect<'A, FluentFiregridError, unit> = Effect.succeed value
