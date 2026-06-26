namespace Firegrid.Core

type StatePredicate =
    { Expression: string
      Description: string option }

[<RequireQualifiedAccess>]
module StatePredicate =
    let create expression =
        { Expression = expression
          Description = None }
