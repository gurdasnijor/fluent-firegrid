namespace Firegrid.Fluent

open Effect
open Fable.Core
open Fable.Core.JsInterop
open Firegrid.Core

// ============================================================
// state.ts — object state tables + CEL state waits.
//
// REDUCTIONS (effect/Schema + effect/SchemaAST):
//  * `Table`/`primaryKey`/`TableClass`/`Schema.Struct` are reduced. A table is a
//    plain record `Table = { TableName; PkField; Fields }` built from a field
//    spec instead of a `Schema.Struct`. `primaryKey`/`primitiveFieldType`/
//    `predicateFieldsFor` SchemaAST introspection is replaced by an explicit
//    field-type list supplied by the caller.
//  * `ChangeMessage` encode/decode (effect/Schema codecs over JSON) are reduced
//    to JSON pass-through (`JSON.parse`/`JSON.stringify`); the discriminant
//    predicates (`isChange`/`isControl`/`isReadJournaled`) are ported faithfully.
//  * `encodeRowFor`/`decodeRowFor` are pass-through (no Schema codec); the row is
//    returned unchanged.
//  * `primaryKeyOf` is ported faithfully (reads `row[pkField]`, validates string).
//  All pure string/predicate/CEL/index-key logic is ported faithfully.
// ============================================================

/// A predicate field type — "boolean" | "number" | "string" | "unknown".
type StatePredicateFieldType = string

/// `StatePredicateField`.
type StatePredicateField =
    { Name: string
      Type: StatePredicateFieldType }

/// Reduced table descriptor (replaces the Schema-driven `TableClass`). `Fields`
/// is the explicit field spec (name + primitive type) used for predicate
/// validation; `PkField` is the primary-key field name.
type Table =
    { TableName: string
      PkField: string
      Fields: StatePredicateField[] }

/// `StatePredicateEnvironment`.
type StatePredicateEnvironment =
    { Change: {| key: StatePredicateField
                 operation: StatePredicateField
                 table: StatePredicateField |}
      EnvironmentVersion: string
      Old: obj // Record<string, StatePredicateField>
      Row: obj
      Table: string }

/// `StateWaitOptions`.
type StateWaitOptions =
    { Name: string
      TimeoutMs: float option
      When: StatePredicate }

/// `StateIndexWaitOptions<Row>`.
type StateIndexWaitOptions =
    { Index: string[]
      Name: string
      TimeoutMs: float option
      Vars: obj // Record<string, unknown>
      Where: StatePredicate }

/// `StateBinding<Row, Key>`.
type StateBinding =
    { Get: string -> Effect<obj option, exn, Context>
      Set: obj -> Effect<unit, exn, Context>
      Delete: string -> Effect<unit, exn, Context>
      WaitForKey: string -> StateWaitOptions -> Effect<obj, exn, Context>
      WaitForIndex: StateIndexWaitOptions -> Effect<obj, exn, Context> }

/// `TableCelFactory<Tbl>`.
type TableCelFactory =
    { Environment: StatePredicateEnvironment
      Expr: (CelExpressionBuilder -> CelExpressionInput) -> StatePredicate
      Cel: string -> StatePredicate }

[<RequireQualifiedAccess>]
module State =

    // ── table construction (reduced) ──────────────────────────────────

    /// Build a reduced `Table` from a name, primary-key field, and field specs.
    /// Replaces `Table(name)(fields)` + `primaryKey(...)` annotation.
    let table (name: string) (pkField: string) (fields: StatePredicateField[]) : Table =
        { TableName = name
          PkField = pkField
          Fields = fields }

    // ── signal names ──────────────────────────────────────────────────

    let stateWaitSignalName (table: string) (key: string) (name: string) : string =
        sprintf "__firegrid_state_wait:%s:%s:%s" table key name

    let stateIndexWaitSignalName (table: string) (indexKey: string) (name: string) : string =
        sprintf "__firegrid_state_wait:%s:index:%s:%s" table indexKey name

    /// `isStateWaitTimedOutPayload(value)`.
    let isStateWaitTimedOutPayload (value: obj) : bool =
        FluentSdk.isObject value
        && FluentSdk.stringValue (FluentSdk.prop<obj> value "_tag") = "StateWaitTimedOut"

    // ── stableJson (faithful) ─────────────────────────────────────────

    /// `stableJson(value)` — deterministic JSON with sorted object keys.
    let rec stableJson (value: obj) : string =
        if FluentSdk.isArray value then
            let arr = unbox<obj[]> value
            sprintf "[%s]" (arr |> Array.map stableJson |> String.concat ",")
        elif FluentSdk.isObject value then
            let entries =
                FluentSdk.objectEntries value
                |> Array.sortWith (fun (left, _) (right, _) -> FluentSdk.localeCompare left right)
                |> Array.map (fun (key, nested) -> sprintf "%s:%s" (FluentSdk.jsonStringify (box key)) (stableJson nested))

            sprintf "{%s}" (entries |> String.concat ",")
        else
            FluentSdk.jsonStringify value

    /// `stateIndexKey(index, vars)`.
    let stateIndexKey (index: string[]) (vars: obj) : string =
        index
        |> Array.map (fun field -> sprintf "%s=%s" field (stableJson (FluentSdk.prop<obj> vars field)))
        |> String.concat "&"

    // ── referenced field scan (faithful regex) ────────────────────────

    let stripCelStringLiterals (expression: string) : string = FluentSdk.stripCelStringLiterals expression

    /// `referencedStateFields(expression)` → `[{ scope; field }]`.
    let referencedStateFields (expression: string) : (string * string)[] =
        let stripped = stripCelStringLiterals expression
        // Each entry is (scope, field), preserving match order.
        FluentSdk.scanStateFieldRefs stripped

    // ── predicate environment (reduced introspection) ─────────────────

    /// Field map from the reduced table's explicit field spec, sorted by name.
    let private predicateFieldsFor (table: Table) : obj =
        let entries =
            table.Fields
            |> Array.map (fun f -> (f.Name, { Name = f.Name; Type = f.Type }))
            |> Array.sortWith (fun (left, _) (right, _) -> FluentSdk.localeCompare left right)
            |> Array.map (fun (name, field) ->
                (name, createObj [ "name" ==> field.Name; "type" ==> field.Type ]))

        FluentSdk.objectFromEntries entries

    /// `predicateEnvironmentVersion(table, fields)`.
    let private predicateEnvironmentVersion (table: string) (fields: obj) : string =
        let parts =
            FluentSdk.objectValues fields
            |> Array.map (fun field ->
                sprintf "%s:%s" (FluentSdk.prop<string> field "name") (FluentSdk.prop<string> field "type"))

        sprintf "table:%s:%s" table (parts |> String.concat ",")

    /// `statePredicateEnvironment(table)`.
    let statePredicateEnvironment (table: Table) : StatePredicateEnvironment =
        let fields = predicateFieldsFor table

        { Change =
            {| key = { Name = "key"; Type = "string" }
               operation = { Name = "operation"; Type = "string" }
               table = { Name = "table"; Type = "string" } |}
          EnvironmentVersion = predicateEnvironmentVersion table.TableName fields
          Old = fields
          Row = fields
          Table = table.TableName }

    // ── predicate validation against environment ──────────────────────

    /// `validateStatePredicateForEnvironment(predicate, environment)`.
    let validateStatePredicateForEnvironment
        (predicate: StatePredicate)
        (environment: StatePredicateEnvironment)
        : Effect<unit, exn, 'R> =
        StatePredicate.validateStatePredicate predicate
        |> Effect.flatMap (fun () ->
            let scopeMap (scope: string) : obj =
                if scope = "old" then environment.Old else environment.Row

            let unknownReferences =
                referencedStateFields predicate.Expression
                |> Array.filter (fun (scope, field) -> FluentSdk.isUndefined (FluentSdk.prop<obj> (scopeMap scope) field))

            if unknownReferences.Length > 0 then
                let formatted =
                    unknownReferences
                    |> Array.map (fun (scope, field) -> sprintf "%s.%s" scope field)
                    |> String.concat ", "

                Effect.fail (
                    FluentFiregridError.create (
                        sprintf
                            "invalid state wait predicate for table %s: unknown field reference %s"
                            environment.Table
                            formatted
                    )
                )
            else
                Effect.succeed ())

    /// `validateStatePredicateForTable(table, predicate)`.
    let validateStatePredicateForTable (table: Table) (predicate: StatePredicate) : Effect<unit, exn, 'R> =
        validateStatePredicateForEnvironment predicate (statePredicateEnvironment table)

    /// `celFor(table)`.
    let celFor (table: Table) : TableCelFactory =
        let environment = statePredicateEnvironment table

        { Environment = environment
          Cel = fun expression -> StatePredicate.cel expression
          Expr = fun build -> StatePredicate.celExpr build }

    // ── index wait validation (faithful) ──────────────────────────────

    let private validateStateIndexWait
        (table: Table)
        (options: StateIndexWaitOptions)
        : Effect<{| environment: StatePredicateEnvironment
                    indexKey: string |}, exn, Context> =
        let environment = statePredicateEnvironment table

        if options.Index.Length = 0 then
            Effect.fail (FluentFiregridError.create "state.waitFor index waits require at least one index field")
        else
            let missingFields =
                options.Index
                |> Array.filter (fun field -> FluentSdk.isUndefined (FluentSdk.prop<obj> environment.Row field))

            if missingFields.Length > 0 then
                Effect.fail (
                    FluentFiregridError.create (
                        sprintf
                            "state.waitFor index for table %s references unknown field %s"
                            table.TableName
                            (missingFields |> String.concat ", ")
                    )
                )
            else
                let missingVars =
                    options.Index
                    |> Array.filter (fun field -> FluentSdk.isUndefined (FluentSdk.prop<obj> options.Vars field))

                if missingVars.Length > 0 then
                    Effect.fail (
                        FluentFiregridError.create (
                            sprintf
                                "state.waitFor index for table %s requires vars for %s"
                                table.TableName
                                (missingVars |> String.concat ", ")
                        )
                    )
                else
                    validateStatePredicateForEnvironment options.Where environment
                    |> Effect.map (fun () ->
                        {| environment = environment
                           indexKey = stateIndexKey options.Index options.Vars |})

    // ── row encode / decode (reduced to pass-through) ─────────────────

    let private encodeRowFor (_table: Table) (row: obj) : Effect<obj, exn, Context> = Effect.succeed row

    let private decodeRowFor (_table: Table) (value: obj) : Effect<obj, exn, Context> = Effect.succeed value

    /// `primaryKeyOf(table, row)` — faithful: read `row[pkField]`, validate.
    let private primaryKeyOf (table: Table) (row: obj) : Effect<string, exn, Context> =
        Effect.sync (fun () ->
            let value = FluentSdk.prop<obj> row table.PkField

            if not (FluentSdk.isString value) || FluentSdk.stringValue value = "" then
                Error(
                    FluentFiregridError.create (
                        sprintf "primary key %s for table %s must be a non-empty string" table.PkField table.TableName
                    )
                )
            else
                Ok(FluentSdk.stringValue value))
        |> Effect.flatMap (function
            | Ok v -> Effect.succeed v
            | Error err -> Effect.fail err)

    // ── backend access ────────────────────────────────────────────────

    let private withStateBackend
        (operation: string)
        (body: ObjectStateBackend -> FluentDurableContextService -> Effect<'A, exn, Context>)
        : Effect<'A, exn, Context> =
        FluentDurableContext.withContext (fun ctx ->
            match ctx.State with
            | None ->
                Effect.fail (
                    FluentFiregridError.create (sprintf "%s can only be used in stateful object handlers" operation)
                )
            | Some backend -> body backend ctx)

    // ── waitForKey (faithful) ─────────────────────────────────────────

    let private waitForKey (table: Table) (key: string) (options: StateWaitOptions) : Effect<obj, exn, Context> =
        withStateBackend "state.waitFor" (fun backend ctx ->
            match backend.WaitFor with
            | None -> Effect.fail (FluentFiregridError.create "state.waitFor is not supported by this state backend")
            | Some backendWaitFor ->
                let predicateEnvironment = statePredicateEnvironment table

                validateStatePredicateForEnvironment options.When predicateEnvironment
                |> Effect.flatMap (fun () ->
                    let waitId =
                        ctx.StateOperationId
                        |> Option.map (fun f -> f { Kind = "waitFor"; Table = table.TableName; Key = key })

                    let signalName = stateWaitSignalName table.TableName key options.Name

                    // timeoutAt = (now + timeoutMs) when both timeoutMs and now exist.
                    let timeoutAtEffect: Effect<float option, exn, Context> =
                        match options.TimeoutMs, ctx.Now with
                        | Some timeoutMs, Some now ->
                            let nowOptions =
                                waitId
                                |> Option.map (fun w ->
                                    { DeterministicValueOptions.Empty with Id = Some(sprintf "%s:timeoutAt" w) })

                            now nowOptions |> Effect.map (fun t -> Some(t + timeoutMs))
                        | _ -> Effect.succeed None

                    timeoutAtEffect
                    |> Effect.flatMap (fun timeoutAt ->
                        let backendOptions: StateWaitBackendOptions =
                            { EnvironmentVersion = Some predicateEnvironment.EnvironmentVersion
                              Name = options.Name
                              SignalName = signalName
                              TimeoutAt = timeoutAt
                              TimeoutMs = options.TimeoutMs
                              WaitId = waitId }

                        backendWaitFor table.TableName key options.When backendOptions
                        |> Effect.flatMap (fun registered ->
                            match registered with
                            | Some value -> decodeRowFor table value
                            | None ->
                                let waitOptions =
                                    waitId
                                    |> Option.map (fun w -> { WaitForEventOptions.Empty with Id = Some w })

                                ctx.WaitForSignal signalName waitOptions
                                |> Effect.flatMap (fun value ->
                                    if isStateWaitTimedOutPayload value then
                                        Effect.fail (
                                            FluentFiregridError.create (sprintf "state.waitFor %s timed out" options.Name)
                                        )
                                    else
                                        decodeRowFor table value)))))

    // ── waitForIndex (faithful) ───────────────────────────────────────

    let private waitForIndex (table: Table) (options: StateIndexWaitOptions) : Effect<obj, exn, Context> =
        withStateBackend "state.waitFor" (fun backend ctx ->
            match backend.WaitForIndex with
            | None ->
                Effect.fail (
                    FluentFiregridError.create "state.waitFor index waits are not supported by this state backend"
                )
            | Some backendWaitForIndex ->
                validateStateIndexWait table options
                |> Effect.flatMap (fun validated ->
                    let waitId =
                        ctx.StateOperationId
                        |> Option.map (fun f ->
                            f
                                { Kind = "waitFor"
                                  Table = table.TableName
                                  Key = sprintf "index:%s" validated.indexKey })

                    let signalName = stateIndexWaitSignalName table.TableName validated.indexKey options.Name

                    let timeoutAtEffect: Effect<float option, exn, Context> =
                        match options.TimeoutMs, ctx.Now with
                        | Some timeoutMs, Some now ->
                            let nowOptions =
                                waitId
                                |> Option.map (fun w ->
                                    { DeterministicValueOptions.Empty with Id = Some(sprintf "%s:timeoutAt" w) })

                            now nowOptions |> Effect.map (fun t -> Some(t + timeoutMs))
                        | _ -> Effect.succeed None

                    timeoutAtEffect
                    |> Effect.flatMap (fun timeoutAt ->
                        let backendOptions: StateIndexWaitBackendOptions =
                            { EnvironmentVersion = Some validated.environment.EnvironmentVersion
                              Name = options.Name
                              SignalName = signalName
                              TimeoutAt = timeoutAt
                              TimeoutMs = options.TimeoutMs
                              WaitId = waitId
                              Index = options.Index
                              IndexKey = validated.indexKey
                              Vars = options.Vars }

                        backendWaitForIndex table.TableName options.Where backendOptions
                        |> Effect.flatMap (fun registered ->
                            match registered with
                            | Some value -> decodeRowFor table value
                            | None ->
                                let waitOptions =
                                    waitId
                                    |> Option.map (fun w -> { WaitForEventOptions.Empty with Id = Some w })

                                ctx.WaitForSignal signalName waitOptions
                                |> Effect.flatMap (fun value ->
                                    if isStateWaitTimedOutPayload value then
                                        Effect.fail (
                                            FluentFiregridError.create (sprintf "state.waitFor %s timed out" options.Name)
                                        )
                                    else
                                        decodeRowFor table value)))))

    /// `state(table)` — the `StateBinding`.
    let state (table: Table) : StateBinding =
        { Get =
            fun key ->
                withStateBackend "state.get" (fun backend ctx ->
                    let readId =
                        ctx.StateOperationId
                        |> Option.map (fun f -> f { Kind = "get"; Table = table.TableName; Key = key })

                    backend.Get table.TableName key readId
                    |> Effect.flatMap (fun value ->
                        match value with
                        | None -> Effect.succeed (None: obj option)
                        | Some v -> decodeRowFor table v |> Effect.map Some))
          Set =
            fun row ->
                withStateBackend "state.set" (fun backend ctx ->
                    primaryKeyOf table row
                    |> Effect.flatMap (fun key ->
                        encodeRowFor table row
                        |> Effect.flatMap (fun encoded ->
                            let opId =
                                ctx.StateOperationId
                                |> Option.map (fun f -> f { Kind = "set"; Table = table.TableName; Key = key })

                            backend.Set table.TableName key encoded opId)))
          Delete =
            fun key ->
                withStateBackend "state.delete" (fun backend ctx ->
                    let opId =
                        ctx.StateOperationId
                        |> Option.map (fun f -> f { Kind = "delete"; Table = table.TableName; Key = key })

                    backend.Delete table.TableName key opId)
          WaitForKey = fun key options -> waitForKey table key options
          WaitForIndex = fun options -> waitForIndex table options }
