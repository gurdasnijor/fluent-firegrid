namespace Firegrid.Runtime

open System.Collections.Generic
open Fable.Core
open Firegrid.Core

/// `schedule-materializer.ts` — cron parser + `materializeWorkflowSchedules`.
[<RequireQualifiedAccess>]
module ScheduleMaterializer =

    let private DEFAULT_CRON_LOOKBACK_MS = 32.0 * 24.0 * 60.0 * 60.0 * 1000.0

    type MaterializeWorkflowSchedulesOptions =
        { Now: float option
          CronLookbackMs: float option }

        static member Empty = { Now = None; CronLookbackMs = None }

    /// `MaterializedWorkflowSchedule` — discriminated on `kind`.
    type MaterializedWorkflowSchedule =
        | Materialized of workflowId: string * scheduleId: ScheduleId * fireAt: float * schedule: WorkflowScheduleSpec
        | Disabled of workflowId: string * scheduleId: ScheduleId * schedule: WorkflowScheduleSpec
        | NotDue of workflowId: string * scheduleId: ScheduleId * schedule: WorkflowScheduleSpec

    // ── cron parsing ──────────────────────────────────────────────────

    type private ParsedCronField =
        { Wildcard: bool
          Values: HashSet<int> }

    type private ParsedCronExpression =
        { Minute: ParsedCronField
          Hour: ParsedCronField
          DayOfMonth: ParsedCronField
          Month: ParsedCronField
          DayOfWeek: ParsedCronField }

    type private CronRange = { Start: int; End: int }

    let private parseCronNumber (value: string) (min: int) (max: int) : int =
        let parsed = RuntimeSdk.numberValue (box value)

        if not (RuntimeSdk.numberIsInteger (box parsed)) || parsed < float min || parsed > float max then
            failwithf "Invalid cron value \"%s\"." value

        int parsed

    let private parseCronRange (range: string) (min: int) (max: int) : CronRange =
        if range = "*" then
            { Start = min; End = max }
        else
            let bounds = range.Split('-')

            if bounds.Length = 1 then
                let value = parseCronNumber bounds[0] min max
                { Start = value; End = value }
            elif bounds.Length = 2 then
                let start = parseCronNumber bounds[0] min max
                let endVal = parseCronNumber bounds[1] min max

                if endVal < start then
                    failwithf "Invalid cron range \"%s\"." range

                { Start = start; End = endVal }
            else
                failwithf "Invalid cron range \"%s\"." range

    let private parseCronField
        (field: string)
        (min: int)
        (max: int)
        (normalize: int -> int)
        : ParsedCronField =
        let values = HashSet<int>()
        let parts = field.Split(',')

        for part in parts do
            let slashParts = part.Split('/')
            let rangePart = slashParts[0]

            let step =
                if slashParts.Length < 2 then
                    1.0
                else
                    RuntimeSdk.numberValue (box slashParts[1])

            if not (RuntimeSdk.numberIsInteger (box step)) || step <= 0.0 then
                failwithf "Invalid cron step \"%s\"." part

            let range = parseCronRange rangePart min max
            let mutable value = range.Start

            while value <= range.End do
                values.Add(normalize value) |> ignore
                value <- value + int step

        { Wildcard = field = "*"
          Values = values }

    let private normalizeDayOfWeek (value: int) : int = if value = 7 then 0 else value

    let private identity (value: int) : int = value

    let private parseCronExpression (expression: string) : ParsedCronExpression =
        let fields = RuntimeSdk.splitRegex (RuntimeSdk.trim expression) "\\s+"

        if fields.Length <> 5 then
            failwithf "Workflow cron schedules must use five fields. Received \"%s\"." expression

        { Minute = parseCronField fields[0] 0 59 identity
          Hour = parseCronField fields[1] 0 23 identity
          DayOfMonth = parseCronField fields[2] 1 31 identity
          Month = parseCronField fields[3] 1 12 identity
          DayOfWeek = parseCronField fields[4] 0 7 normalizeDayOfWeek }

    let private matchesCron (cron: ParsedCronExpression) (date: RuntimeSdk.JsDate) : bool =
        let dayOfMonthMatches = cron.DayOfMonth.Values.Contains(RuntimeSdk.getUTCDate date)
        let dayOfWeekMatches = cron.DayOfWeek.Values.Contains(RuntimeSdk.getUTCDay date)

        let dayMatches =
            if not cron.DayOfMonth.Wildcard && not cron.DayOfWeek.Wildcard then
                dayOfMonthMatches || dayOfWeekMatches
            else
                dayOfMonthMatches && dayOfWeekMatches

        cron.Minute.Values.Contains(RuntimeSdk.getUTCMinutes date)
        && cron.Hour.Values.Contains(RuntimeSdk.getUTCHours date)
        && dayMatches
        && cron.Month.Values.Contains(RuntimeSdk.getUTCMonth date + 1)

    let private floorToMinute (timestamp: float) : float =
        RuntimeSdk.mathFloor (timestamp / 60000.0) * 60000.0

    let private getPreviousCronFireAt
        (expression: string)
        (timezone: string option)
        (now: float)
        (lookbackMs: float)
        : float option =
        match timezone with
        | Some tz when tz <> "UTC" ->
            failwithf "Workflow cron schedules are materialized in UTC. Received timezone \"%s\"." tz
        | _ -> ()

        let cron = parseCronExpression expression
        let start = floorToMinute now
        let endTs = start - lookbackMs

        let mutable timestamp = start
        let mutable result = None

        while result.IsNone && timestamp >= endTs do
            if matchesCron cron (RuntimeSdk.newDate timestamp) then
                result <- Some timestamp
            else
                timestamp <- timestamp - 60000.0

        result

    let private getDueFireAt (schedule: WorkflowScheduleSpec) (now: float) (cronLookbackMs: float) : float option =
        match schedule with
        | IntervalSpec(everyMs, _) ->
            if not (RuntimeSdk.numberIsFinite (box everyMs)) || everyMs <= 0.0 then
                failwith "Interval workflow schedules must use a positive everyMs."

            Some(RuntimeSdk.mathFloor (now / everyMs) * everyMs)
        | CronSpec(expression, timezone) -> getPreviousCronFireAt expression timezone now cronLookbackMs

    // ── materialization ───────────────────────────────────────────────

    let private getScheduleId (workflowId: string) (definition: WorkflowScheduleDefinition) (index: int) : ScheduleId =
        match definition.Id with
        | Some id -> id
        | None -> sprintf "%s:%d" workflowId index

    let private resolveScheduleInput (input: obj option) : JS.Promise<obj> =
        promise {
            match input with
            | None -> return RuntimeSdk.undefinedValue
            | Some v ->
                if RuntimeSdk.isTypeofFunction v then
                    let fn = v :?> (unit -> obj)
                    let! resolved = RuntimeSdk.promiseResolve (fn ())
                    return resolved
                else
                    return v
        }

    /// `materializeWorkflowSchedules(runtime, options)`.
    let materialize
        (runtime: WorkflowRuntimeDefinition)
        (options: MaterializeWorkflowSchedulesOptions)
        : JS.Promise<MaterializedWorkflowSchedule[]> =
        promise {
            let now = options.Now |> Option.defaultValue (RuntimeSdk.nowMillis ())
            let cronLookbackMs = options.CronLookbackMs |> Option.defaultValue DEFAULT_CRON_LOOKBACK_MS
            let materialized = ResizeArray<MaterializedWorkflowSchedule>()

            if not (RuntimeSdk.numberIsFinite (box cronLookbackMs)) || cronLookbackMs < 0.0 then
                failwith "Workflow cron lookback must be a non-negative number."

            for (workflowId, registrationObj) in RuntimeSdk.objectEntries runtime.Workflows do
                let registration = registrationObj :?> WorkflowRegistration
                let schedules = registration.Schedules |> Option.defaultValue [||]

                for index in 0 .. schedules.Length - 1 do
                    let definition = schedules[index]
                    let scheduleId = getScheduleId workflowId definition index

                    if definition.Enabled = Some false then
                        do!
                            runtime.Store.UpsertSchedule
                                { ScheduleId = scheduleId
                                  WorkflowId = workflowId
                                  WorkflowVersion = registration.Version
                                  Schedule = definition.Schedule
                                  OverlapPolicy = definition.OverlapPolicy |> Option.defaultValue "skip"
                                  Input = None
                                  NextFireAt = None
                                  Enabled = false
                                  Now = now }

                        materialized.Add(Disabled(workflowId, scheduleId, definition.Schedule))
                    else
                        let fireAt = getDueFireAt definition.Schedule now cronLookbackMs

                        match fireAt with
                        | None -> materialized.Add(NotDue(workflowId, scheduleId, definition.Schedule))
                        | Some fire ->
                            let! resolvedInput = resolveScheduleInput definition.Input

                            do!
                                runtime.Store.UpsertSchedule
                                    { ScheduleId = scheduleId
                                      WorkflowId = workflowId
                                      WorkflowVersion = registration.Version
                                      Schedule = definition.Schedule
                                      OverlapPolicy = definition.OverlapPolicy |> Option.defaultValue "skip"
                                      Input = Some resolvedInput
                                      NextFireAt = Some fire
                                      Enabled = true
                                      Now = now }

                            materialized.Add(Materialized(workflowId, scheduleId, fire, definition.Schedule))

            return materialized.ToArray()
        }
