namespace Firegrid.Core

open Fable.Core.JsInterop

/// Tagged result helpers (`result.ts`). Produce JS objects with the `{ ok }`
/// discriminant merged with the supplied data, matching the TS spread shape.
[<RequireQualifiedAccess>]
module Result =

    /// `succeed(data)` → `{ ok: true, ...data }`.
    let succeed (data: obj) : obj =
        let o = createObj [ "ok" ==> true ]
        CoreSdk.objectAssign o data |> ignore
        o

    /// `fail(reason)` → `{ ok: false, reason }`.
    let fail (reason: string) : obj =
        createObj [ "ok" ==> false; "reason" ==> reason ]
