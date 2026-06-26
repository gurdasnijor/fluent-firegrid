namespace Firegrid.Fluent

open Effect
open Fable.Core
open Fable.Core.JsInterop
open Firegrid.Core

// ============================================================
// statePredicate.ts — CEL state predicate builders + validation
// ============================================================

/// `CelStatePredicate` / `StatePredicate`.
type CelStatePredicate =
    { Language: string // "cel"
      Expression: string }

/// `StatePredicate = CelStatePredicate`.
type StatePredicate = CelStatePredicate

/// `CelExpressionNode` — held as a record of functions. `Expression` is the
/// built CEL string; the combinators wrap it.
type CelExpressionNode =
    { Expression: string
      And: CelExpressionInput -> CelExpressionNode
      Not: unit -> CelExpressionNode
      Or: CelExpressionInput -> CelExpressionNode }

/// `CelExpressionInput = CelExpressionNode | CelStatePredicate | string`.
/// Held as `obj` and resolved by `expressionText` (matching the TS structural
/// dispatch on `typeof input === "string"` vs `.expression`).
and CelExpressionInput = obj

/// `CelLiteral = boolean | null | number | string`. Held as `obj`.
type CelLiteral = obj

/// `CelFieldExpression extends CelExpressionNode` with comparison builders.
type CelFieldExpression =
    { Node: CelExpressionNode
      Eq: CelLiteral -> CelExpressionNode
      GreaterThan: CelLiteral -> CelExpressionNode
      GreaterThanOrEqual: CelLiteral -> CelExpressionNode
      In: CelLiteral[] -> CelExpressionNode
      LessThan: CelLiteral -> CelExpressionNode
      LessThanOrEqual: CelLiteral -> CelExpressionNode
      NotEq: CelLiteral -> CelExpressionNode }

/// `CelExpressionBuilder` — the `change` / `old` / `row` scopes. `old` and
/// `row` are JS Proxy objects so `builder.row.anyField` resolves to a
/// `CelFieldExpression`, matching the TS `Proxy` exactly.
type CelExpressionBuilder =
    { Change: {| key: CelFieldExpression
                 operation: CelFieldExpression
                 table: CelFieldExpression |}
      Old: obj // Proxy<Record<string, CelFieldExpression>>
      Row: obj }

/// `StatePredicateContext`.
type StatePredicateContext =
    { Row: obj
      Old: obj option
      Vars: obj option
      Change: obj option }

[<RequireQualifiedAccess>]
module StatePredicate =

    /// `expressionText(input)` — `typeof input === "string" ? input : input.expression`.
    let expressionText (input: CelExpressionInput) : string =
        if FluentSdk.isString input then
            FluentSdk.stringValue input
        else
            FluentSdk.prop<string> input "expression"

    /// `literalText(value)` — `typeof value === "string" ? JSON.stringify(value) : String(value)`.
    let literalText (value: CelLiteral) : string =
        if FluentSdk.isString value then
            FluentSdk.jsonStringify value
        else
            FluentSdk.stringValue value

    /// `celNode(expression)`.
    let rec celNode (expression: string) : CelExpressionNode =
        { Expression = expression
          And = fun other -> celNode (sprintf "(%s) && (%s)" expression (expressionText other))
          Not = fun () -> celNode (sprintf "!(%s)" expression)
          Or = fun other -> celNode (sprintf "(%s) || (%s)" expression (expressionText other)) }

    /// `celField(path)` — spreads `celNode(path)` and adds comparisons.
    let celField (path: string) : CelFieldExpression =
        let node = celNode path

        { Node = node
          Eq = fun value -> celNode (sprintf "%s == %s" path (literalText value))
          GreaterThan = fun value -> celNode (sprintf "%s > %s" path (literalText value))
          GreaterThanOrEqual = fun value -> celNode (sprintf "%s >= %s" path (literalText value))
          In =
            fun values ->
                let joined = values |> Array.map literalText |> String.concat ", "
                celNode (sprintf "%s in [%s]" path joined)
          LessThan = fun value -> celNode (sprintf "%s < %s" path (literalText value))
          LessThanOrEqual = fun value -> celNode (sprintf "%s <= %s" path (literalText value))
          NotEq = fun value -> celNode (sprintf "%s != %s" path (literalText value)) }

    /// `celFieldScope(scope)` — a JS Proxy whose `get(prop)` returns
    /// `celField(`${scope}.${prop}`)`. Implemented via Emit to mirror the TS Proxy.
    [<Emit("new Proxy({}, { get: (_t, prop) => ($1)(($0) + '.' + String(prop)) })")>]
    let private fieldScopeProxy (_scope: string) (_celField: string -> CelFieldExpression) : obj = jsNative

    let celFieldScope (scope: string) : obj = fieldScopeProxy scope celField

    /// `createCelExpressionBuilder()`.
    let createCelExpressionBuilder () : CelExpressionBuilder =
        { Change =
            {| key = celField "change.key"
               operation = celField "change.operation"
               table = celField "change.table" |}
          Old = celFieldScope "old"
          Row = celFieldScope "row" }

    /// `celPredicate(expression)`.
    let private celPredicate (expression: string) : StatePredicate =
        { Expression = expression; Language = "cel" }

    /// `cel(expression)` — direct CEL string factory.
    let cel (expression: string) : StatePredicate = celPredicate expression

    /// `cel.expr(build)` — build a predicate from the expression builder.
    let celExpr (build: CelExpressionBuilder -> CelExpressionInput) : StatePredicate =
        celPredicate (expressionText (build (createCelExpressionBuilder ())))

    /// `createCelEnvironment()`.
    let createCelEnvironment () : FluentSdk.CelEnvironment = FluentSdk.createEnvironment ()

    /// `causeMessage(cause)`.
    let private causeMessage (cause: obj) : string =
        if FluentSdk.isError cause then
            FluentSdk.errorMessage cause
        else
            FluentSdk.stringValue cause

    [<Emit("(() => { throw ($0); })()")>]
    let private throwValue (_value: obj) : 'a = jsNative

    // `Effect.try` is emulated as `Effect.sync` (deferred work) producing an
    // F# `Result`, then `Effect.flatMap` into `succeed`/`fail` — keeping the
    // catch lazy (runs on execution) and the error a Core `FluentFiregridError`.
    let private tryEffect (work: unit -> 'a) (onCatch: obj -> exn) : Effect<'a, exn, 'R> =
        Effect.sync (fun () ->
            try
                Ok(work ())
            with cause ->
                Error(onCatch (box cause)))
        |> Effect.flatMap (function
            | Ok value -> Effect.succeed value
            | Error err -> Effect.fail err)

    /// `validateStatePredicate(predicate)` — checks the CEL expression compiles
    /// and is of type `bool`. Errors map to Core `FluentFiregridError`.
    let validateStatePredicate (predicate: StatePredicate) : Effect<unit, exn, 'R> =
        tryEffect
            (fun () ->
                let result = FluentSdk.check (createCelEnvironment ()) predicate.Expression

                if not (FluentSdk.prop<bool> result "valid") then
                    let err = FluentSdk.prop<obj> result "error"

                    if FluentSdk.isNullish err then
                        throwValue (box (System.Exception "invalid CEL expression"))
                    else
                        throwValue err

                let resultType = FluentSdk.prop<obj> result "type"

                if FluentSdk.stringValue resultType <> "bool" then
                    let displayed =
                        if FluentSdk.isNullish resultType then
                            "unknown"
                        else
                            FluentSdk.stringValue resultType

                    throwValue (
                        box (System.Exception(sprintf "state wait CEL expression must evaluate to bool, got %s" displayed))
                    ))
            (fun cause ->
                FluentFiregridError.createWithCause
                    (sprintf "invalid state wait predicate: %s" (causeMessage cause))
                    cause)

    /// `evaluateStatePredicate(predicate, context)`.
    let evaluateStatePredicate (predicate: StatePredicate) (context: StatePredicateContext) : Effect<bool, exn, 'R> =
        validateStatePredicate predicate
        |> Effect.flatMap (fun () ->
            tryEffect
                (fun () ->
                    // `{ ...context.vars, ...context }` — context fields override vars.
                    let vars =
                        match context.Vars with
                        | Some v -> v
                        | None -> FluentSdk.emptyObject ()

                    let ctxObj =
                        let o = createObj [ "row" ==> context.Row ]
                        match context.Old with
                        | Some v -> FluentSdk.setProp o "old" v
                        | None -> ()
                        match context.Vars with
                        | Some v -> FluentSdk.setProp o "vars" v
                        | None -> ()
                        match context.Change with
                        | Some v -> FluentSdk.setProp o "change" v
                        | None -> ()
                        o

                    let merged = FluentSdk.assign2 vars ctxObj
                    let result = FluentSdk.evaluate (createCelEnvironment ()) predicate.Expression merged

                    if not (FluentSdk.isBoolean result) then
                        throwValue (
                            box (
                                System.Exception(
                                    sprintf "state wait CEL expression must evaluate to bool, got %s" (FluentSdk.typeOf result)
                                )
                            )
                        )

                    unbox<bool> result)
                (fun cause ->
                    FluentFiregridError.createWithCause
                        (sprintf "failed to evaluate state wait predicate: %s" (causeMessage cause))
                        cause))
