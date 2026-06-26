namespace Firegrid.Core

open Fable.Core
open Fable.Core.JsInterop

/// Minimal JSON Patch (RFC 6902) operation. Port of `state-diff.ts`'s
/// `Operation` union — `replace`/`add` carry a value, `remove` does not.
type Operation =
    | Replace of path: string * value: obj
    | Add of path: string * value: obj
    | Remove of path: string

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Operation =

    /// JS-object form `{ op, path, value? }` for the wire / log payload.
    let toObj (op: Operation) : obj =
        match op with
        | Replace(path, value) -> createObj [ "op" ==> "replace"; "path" ==> path; "value" ==> value ]
        | Add(path, value) -> createObj [ "op" ==> "add"; "path" ==> path; "value" ==> value ]
        | Remove path -> createObj [ "op" ==> "remove"; "path" ==> path ]

[<RequireQualifiedAccess>]
module StateDiff =

    /// `isObject` — value is non-null and `typeof === 'object'`.
    let isObject (value: obj) : bool =
        not (isNull value) && CoreSdk.isTypeofObject value

    /// `escapeJsonPointer` — escape `~` then `/` per RFC 6901.
    let escapeJsonPointer (segment: string) : string =
        segment.Replace("~", "~0").Replace("/", "~1")

    /// `normalizeValue` — coerce `undefined` to `null` recursively so the
    /// serialized op is always well-formed RFC 6902.
    let rec normalizeValue (value: obj) : obj =
        if CoreSdk.isUndefined value then
            box null
        elif CoreSdk.isArray value then
            let arr = unbox<obj[]> value
            box (Array.map normalizeValue arr)
        elif isObject value then
            let out = createObj []

            for (k, v) in CoreSdk.objectEntries value do
                CoreSdk.setProp out k (normalizeValue v)

            out
        else
            value

    /// `snapshotState` — deep clone a state object via `structuredClone`.
    let snapshotState<'T> (state: 'T) : 'T = CoreSdk.structuredClone state

    let rec private diff (prev: obj) (next: obj) (path: string) : Operation list =
        if CoreSdk.objectIs prev next then
            []
        else
            let prevIsObj = isObject prev
            let nextIsObj = isObject next

            // One is a primitive (or null), or array-ness disagrees — replace node.
            if not prevIsObj
               || not nextIsObj
               || (CoreSdk.isArray prev <> CoreSdk.isArray next) then
                [ Replace((if path = "" then "" else path), normalizeValue next) ]
            elif CoreSdk.isArray prev && CoreSdk.isArray next then
                let prevLen = CoreSdk.arrayLength prev
                let nextLen = CoreSdk.arrayLength next

                if prevLen <> nextLen then
                    [ Replace((if path = "" then "" else path), normalizeValue next) ]
                else
                    [ for i in 0 .. prevLen - 1 do
                          yield!
                              diff
                                  (CoreSdk.arrayItem<obj> prev i)
                                  (CoreSdk.arrayItem<obj> next i)
                                  (sprintf "%s/%d" path i) ]
            else
                // Both plain objects.
                let prevKeys = CoreSdk.objectKeys prev
                let nextKeys = CoreSdk.objectKeys next

                let allKeys =
                    let seen = System.Collections.Generic.HashSet<string>()
                    let ordered = ResizeArray<string>()

                    for k in Array.append prevKeys nextKeys do
                        if seen.Add k then
                            ordered.Add k

                    ordered

                [ for key in allKeys do
                      let subPath = sprintf "%s/%s" path (escapeJsonPointer key)
                      let prevHas = CoreSdk.hasOwnProperty prev key
                      let nextHas = CoreSdk.hasOwnProperty next key

                      if prevHas && nextHas then
                          yield! diff (CoreSdk.prop<obj> prev key) (CoreSdk.prop<obj> next key) subPath
                      elif nextHas then
                          yield Add(subPath, normalizeValue (CoreSdk.prop<obj> next key))
                      else
                          yield Remove subPath ]

    /// `diffState` — produce an RFC 6902 patch from `prev` to `next`.
    let diffState (prev: obj) (next: obj) : Operation list = diff prev next ""
