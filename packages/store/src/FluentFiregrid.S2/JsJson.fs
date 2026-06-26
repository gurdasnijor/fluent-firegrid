namespace Firegrid.FluentFiregrid.S2

open Fable.Core

[<RequireQualifiedAccess>]
module JsJson =

    [<Emit("JSON.stringify($0)")>]
    let stringify (_value: obj) : string = jsNative

    [<Emit("JSON.parse($0)")>]
    let parse<'A> (_value: string) : 'A = jsNative

    [<Emit("$0 == null")>]
    let isNullish (_value: obj) : bool = jsNative

    [<Emit("$0[$1]")>]
    let prop<'T> (_value: obj) (_key: string) : 'T = jsNative

    [<Emit("String($0)")>]
    let stringValue (_value: obj) : string = jsNative

    [<Emit("Number($0)")>]
    let numberValue (_value: obj) : float = jsNative

    let optionalStringProp (key: string) (value: obj) : string option =
        let raw = prop<obj> value key

        if isNullish raw then None else Some(stringValue raw)

    let stringProp (key: string) (value: obj) : string =
        prop<obj> value key |> stringValue

    let numberProp (key: string) (value: obj) : float =
        prop<obj> value key |> numberValue
