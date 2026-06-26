import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as SchemaAST from "effect/SchemaAST"

import { stateIndexKey } from "@firegrid/core/state"
import { FluentDurableContext, type FluentDurableContextService, type ObjectStateBackend } from "./context.ts"
import { FluentFiregridError } from "./error.ts"
export {
  ChangeMessage,
  MaterializedState,
  type StateChangeMessage,
  type StateControl,
  type StateControlMessage,
  stateIndexKey,
  type StateMessage,
  type StateOperation,
  type StateReadJournaledMessage
} from "@firegrid/core/state"
export {
  cel,
  type CelExpressionBuilder,
  type CelExpressionInput,
  type CelExpressionNode,
  type CelFactory,
  type CelFieldExpression,
  type CelLiteral,
  type CelStatePredicate,
  evaluateStatePredicate,
  type StatePredicate,
  type StatePredicateContext,
  validateStatePredicate
} from "./statePredicate.ts"
import {
  cel,
  type CelExpressionBuilder,
  type CelExpressionInput,
  type CelFieldExpression,
  type StatePredicate,
  validateStatePredicate
} from "./statePredicate.ts"

const PrimaryKeyAnnotation = "@firegrid/fluent/primaryKey"
const readPrimaryKey = SchemaAST.resolveAt<boolean>(PrimaryKeyAnnotation)

export const primaryKey = <S extends Schema.Top>(schema: S): S => schema.annotate({ [PrimaryKeyAnnotation]: true }) as S

const findPrimaryKey = (schema: Schema.Struct<Schema.Struct.Fields>): string => {
  const entry = Object.entries(schema.fields).find(([, field]) => readPrimaryKey(field.ast) === true)
  if (entry === undefined) {
    throw new Error("table schema has no primaryKey-annotated field")
  }
  return entry[0]
}

export interface TableClass<Fields extends Schema.Struct.Fields> {
  new(): object
  readonly tableName: string
  readonly schema: Schema.Struct<Fields>
  readonly pkField: string
  readonly Row: Schema.Struct<Fields>["Type"]
}

export type AnyTable = TableClass<Schema.Struct.Fields>

export type RowOf<T extends AnyTable> = T["Row"]

export const Table =
  <_Self = never>(name: string) => <const Fields extends Schema.Struct.Fields>(fields: Fields): TableClass<Fields> => {
    const schema = Schema.Struct(fields)
    const pkField = findPrimaryKey(schema)
    class TableImpl {
      declare readonly _tableBrand: _Self
      static readonly tableName = name
      static readonly schema = schema
      static readonly pkField = pkField
    }
    return TableImpl as unknown as TableClass<Fields>
  }

export interface StateBinding<Row, Key extends string = string> {
  readonly get: (key: Key) => Effect.Effect<Option.Option<Row>, FluentFiregridError, FluentDurableContext>
  readonly set: (row: Row) => Effect.Effect<void, FluentFiregridError, FluentDurableContext>
  readonly delete: (key: Key) => Effect.Effect<void, FluentFiregridError, FluentDurableContext>
  readonly waitFor: {
    (key: Key, options: StateWaitOptions): Effect.Effect<Row, FluentFiregridError, FluentDurableContext>
    (options: StateIndexWaitOptions<Row>): Effect.Effect<Row, FluentFiregridError, FluentDurableContext>
  }
}

export interface StateWaitOptions {
  readonly name: string
  readonly timeoutMs?: number
  readonly when: StatePredicate
}

export interface StateIndexWaitOptions<Row> {
  readonly index: ReadonlyArray<Extract<keyof Row, string>>
  readonly name: string
  readonly timeoutMs?: number
  readonly vars: Readonly<Partial<Record<Extract<keyof Row, string>, unknown>> & Record<string, unknown>>
  readonly where: StatePredicate
}

export type StatePredicateFieldType = "boolean" | "number" | "string" | "unknown"

export interface StatePredicateField {
  readonly name: string
  readonly type: StatePredicateFieldType
}

export interface StatePredicateEnvironment {
  readonly change: {
    readonly key: StatePredicateField
    readonly operation: StatePredicateField
    readonly table: StatePredicateField
  }
  readonly environmentVersion: string
  readonly old: Readonly<Record<string, StatePredicateField>>
  readonly row: Readonly<Record<string, StatePredicateField>>
  readonly table: string
}

type StringKeyOf<A> = Extract<keyof A, string>

export type TableCelExpressionBuilder<Row> =
  & Omit<CelExpressionBuilder, "old" | "row">
  & {
    readonly old: { readonly [Key in StringKeyOf<Row>]: CelFieldExpression }
    readonly row: { readonly [Key in StringKeyOf<Row>]: CelFieldExpression }
  }

export interface TableCelFactory<Tbl extends AnyTable> {
  readonly environment: StatePredicateEnvironment
  readonly expr: (build: (builder: TableCelExpressionBuilder<RowOf<Tbl>>) => CelExpressionInput) => StatePredicate
  (expression: string): StatePredicate
}

const stateWaitSignalName = (table: string, key: string, name: string): string =>
  `__firegrid_state_wait:${table}:${key}:${name}`

const stateIndexWaitSignalName = (table: string, indexKey: string, name: string): string =>
  `__firegrid_state_wait:${table}:index:${indexKey}:${name}`

interface StateWaitTimedOutPayload {
  readonly _tag: "StateWaitTimedOut"
  readonly name: string
}

const isStateWaitTimedOutPayload = (value: unknown): value is StateWaitTimedOutPayload =>
  typeof value === "object"
  && value !== null
  && (value as { readonly _tag?: unknown })._tag === "StateWaitTimedOut"

const primitiveFieldType = (ast: SchemaAST.AST): StatePredicateFieldType => {
  const typeAst = SchemaAST.toType(ast)
  if (SchemaAST.isString(typeAst)) return "string"
  if (SchemaAST.isNumber(typeAst)) return "number"
  if (SchemaAST.isBoolean(typeAst)) return "boolean"
  if (SchemaAST.isLiteral(typeAst)) {
    const literalType = typeof typeAst.literal
    if (literalType === "string" || literalType === "number" || literalType === "boolean") {
      return literalType
    }
  }
  if (SchemaAST.isUnion(typeAst)) {
    const fieldTypes = new Set(typeAst.types.map(primitiveFieldType).filter((fieldType) => fieldType !== "unknown"))
    return fieldTypes.size === 1 ? Array.from(fieldTypes)[0]! : "unknown"
  }
  return "unknown"
}

const predicateFieldsFor = (table: AnyTable): Readonly<Record<string, StatePredicateField>> => {
  const fields: Array<readonly [string, StatePredicateField]> = Object.entries(table.schema.fields)
    .map(([name, field]) => [
      name,
      {
        name,
        type: primitiveFieldType(field.ast)
      } satisfies StatePredicateField
    ])
  return Object.fromEntries(fields.sort(([left], [right]) => left.localeCompare(right)))
}

const predicateEnvironmentVersion = (
  table: string,
  fields: Readonly<Record<string, StatePredicateField>>
): string => `table:${table}:${Object.values(fields).map((field) => `${field.name}:${field.type}`).join(",")}`

export const statePredicateEnvironment = (table: AnyTable): StatePredicateEnvironment => {
  const fields = predicateFieldsFor(table)
  return {
    change: {
      key: { name: "key", type: "string" },
      operation: { name: "operation", type: "string" },
      table: { name: "table", type: "string" }
    },
    environmentVersion: predicateEnvironmentVersion(table.tableName, fields),
    old: fields,
    row: fields,
    table: table.tableName
  }
}

const stripCelStringLiterals = (expression: string): string =>
  expression.replace(/"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'/g, "")

const referencedStateFields = (
  expression: string
): ReadonlyArray<{ readonly scope: "old" | "row"; readonly field: string }> => {
  const references = new Array<{ readonly scope: "old" | "row"; readonly field: string }>()
  const stripped = stripCelStringLiterals(expression)
  const pattern = /\b(row|old)\.([A-Za-z_][A-Za-z0-9_]*)/g
  let match: RegExpExecArray | null
  while ((match = pattern.exec(stripped)) !== null) {
    references.push({ field: match[2]!, scope: match[1] as "old" | "row" })
  }
  return references
}

export const validateStatePredicateForEnvironment: (
  predicate: StatePredicate,
  environment: StatePredicateEnvironment
) => Effect.Effect<void, FluentFiregridError> = Effect.fn("validateStatePredicateForEnvironment")(function*(
  predicate,
  environment
) {
  yield* validateStatePredicate(predicate)
  const unknownReferences = referencedStateFields(predicate.expression).filter(
    ({ field, scope }) => environment[scope][field] === undefined
  )
  if (unknownReferences.length > 0) {
    const formatted = unknownReferences.map(({ field, scope }) => `${scope}.${field}`).join(", ")
    return yield* new FluentFiregridError({
      message: `invalid state wait predicate for table ${environment.table}: unknown field reference ${formatted}`
    })
  }
})

export const validateStatePredicateForTable: (
  table: AnyTable,
  predicate: StatePredicate
) => Effect.Effect<void, FluentFiregridError> = Effect.fn("validateStatePredicateForTable")(function*(
  table,
  predicate
) {
  yield* validateStatePredicateForEnvironment(predicate, statePredicateEnvironment(table))
})

export const celFor = <Tbl extends AnyTable>(table: Tbl): TableCelFactory<Tbl> => {
  const environment = statePredicateEnvironment(table)
  return Object.assign((expression: string) => cel(expression), {
    environment,
    expr: (build: (builder: TableCelExpressionBuilder<RowOf<Tbl>>) => CelExpressionInput): StatePredicate =>
      cel.expr((builder) => build(builder as TableCelExpressionBuilder<RowOf<Tbl>>))
  })
}

const validateStateIndexWait = (
  table: AnyTable,
  options: StateIndexWaitOptions<unknown>
): Effect.Effect<{ readonly environment: StatePredicateEnvironment; readonly indexKey: string }, FluentFiregridError> =>
  Effect.gen(function*() {
    const environment = statePredicateEnvironment(table)
    if (options.index.length === 0) {
      return yield* new FluentFiregridError({ message: "state.waitFor index waits require at least one index field" })
    }
    const missingFields = options.index.filter((field) => environment.row[field] === undefined)
    if (missingFields.length > 0) {
      return yield* new FluentFiregridError({
        message: `state.waitFor index for table ${table.tableName} references unknown field ${missingFields.join(", ")}`
      })
    }
    const missingVars = options.index.filter((field) => options.vars[field] === undefined)
    if (missingVars.length > 0) {
      return yield* new FluentFiregridError({
        message: `state.waitFor index for table ${table.tableName} requires vars for ${missingVars.join(", ")}`
      })
    }
    yield* validateStatePredicateForEnvironment(options.where, environment)
    return { environment, indexKey: stateIndexKey(options.index, options.vars) }
  })

const encodeRowFor = <Tbl extends AnyTable>(
  table: Tbl,
  row: RowOf<Tbl>
): Effect.Effect<unknown, FluentFiregridError> =>
  Schema.encodeUnknownEffect(table.schema as unknown as Schema.Codec<unknown, unknown, never, never>)(row).pipe(
    Effect.mapError((cause) =>
      new FluentFiregridError({ cause, message: `failed to encode row for table ${table.tableName}` })
    )
  )

const decodeRowFor = <Tbl extends AnyTable>(
  table: Tbl,
  value: unknown
): Effect.Effect<RowOf<Tbl>, FluentFiregridError> =>
  Schema.decodeUnknownEffect(table.schema as unknown as Schema.ConstraintCodec<RowOf<Tbl>, unknown, never, never>)(
    value
  ).pipe(
    Effect.mapError((cause) =>
      new FluentFiregridError({ cause, message: `failed to decode row for table ${table.tableName}` })
    )
  )

const primaryKeyOf = <Tbl extends AnyTable>(
  table: Tbl,
  row: RowOf<Tbl>
): Effect.Effect<string, FluentFiregridError> =>
  Effect.try({
    try: () => {
      const value = (row as Record<string, unknown>)[table.pkField]
      if (typeof value !== "string" || value === "") {
        throw new Error(`primary key ${table.pkField} for table ${table.tableName} must be a non-empty string`)
      }
      return value
    },
    catch: (cause) => {
      const message = cause instanceof Error
        ? cause.message
        : `failed to read primary key for table ${table.tableName}`
      return new FluentFiregridError({ cause, message })
    }
  })

const withStateBackend = <A>(
  operation: string,
  body: (backend: ObjectStateBackend, ctx: FluentDurableContextService) => Effect.Effect<A, FluentFiregridError>
): Effect.Effect<A, FluentFiregridError, FluentDurableContext> =>
  FluentDurableContext.pipe(
    Effect.flatMap((ctx) =>
      ctx.state === undefined
        ? Effect.fail(new FluentFiregridError({ message: `${operation} can only be used in stateful object handlers` }))
        : body(ctx.state, ctx)
    )
  )

export const state = <Tbl extends AnyTable>(table: Tbl): StateBinding<RowOf<Tbl>> => ({
  get: (key) =>
    withStateBackend("state.get", (backend, ctx) => {
      const readId = ctx.stateOperationId?.({ key, kind: "get", table: table.tableName })
      return backend.get(table.tableName, key, readId === undefined ? undefined : { readId }).pipe(
        Effect.flatMap((value) =>
          Option.isNone(value)
            ? Effect.succeed(Option.none<RowOf<Tbl>>())
            : decodeRowFor(table, value.value).pipe(Effect.map(Option.some))
        )
      )
    }),
  set: (row) =>
    withStateBackend("state.set", (backend, ctx) =>
      Effect.gen(function*() {
        const key = yield* primaryKeyOf(table, row)
        const encoded = yield* encodeRowFor(table, row)
        const opId = ctx.stateOperationId?.({ key, kind: "set", table: table.tableName })
        yield* backend.set(table.tableName, key, encoded, opId === undefined ? undefined : { opId })
      })),
  delete: (key) =>
    withStateBackend("state.delete", (backend, ctx) => {
      const opId = ctx.stateOperationId?.({ key, kind: "delete", table: table.tableName })
      return backend.delete(table.tableName, key, opId === undefined ? undefined : { opId })
    }),
  waitFor: ((first: string | StateIndexWaitOptions<RowOf<Tbl>>, second?: StateWaitOptions) =>
    typeof first === "string"
      ? waitForKey(table, first, second!)
      : waitForIndex(table, first)) as StateBinding<RowOf<Tbl>>["waitFor"]
})

const waitForKey = <Tbl extends AnyTable>(
  table: Tbl,
  key: string,
  options: StateWaitOptions
): Effect.Effect<RowOf<Tbl>, FluentFiregridError, FluentDurableContext> =>
  withStateBackend("state.waitFor", (backend, ctx) =>
    Effect.gen(function*() {
      if (backend.waitFor === undefined) {
        return yield* new FluentFiregridError({ message: "state.waitFor is not supported by this state backend" })
      }
      const predicateEnvironment = statePredicateEnvironment(table)
      yield* validateStatePredicateForEnvironment(options.when, predicateEnvironment)
      const waitId = ctx.stateOperationId?.({ key, kind: "waitFor", table: table.tableName })
      const signalName = stateWaitSignalName(table.tableName, key, options.name)
      const timeoutAt = options.timeoutMs === undefined || ctx.now === undefined
        ? undefined
        : (yield* ctx.now(waitId === undefined ? undefined : { id: `${waitId}:timeoutAt` })) + options.timeoutMs
      const registered = yield* backend.waitFor(table.tableName, key, options.when, {
        environmentVersion: predicateEnvironment.environmentVersion,
        name: options.name,
        signalName,
        ...(timeoutAt === undefined ? {} : { timeoutAt }),
        ...(options.timeoutMs === undefined ? {} : { timeoutMs: options.timeoutMs }),
        ...(waitId === undefined ? {} : { waitId })
      })
      if (Option.isSome(registered)) {
        return yield* decodeRowFor(table, registered.value)
      }
      const value = yield* ctx.waitForSignal<unknown>(signalName, waitId === undefined ? undefined : { id: waitId })
      if (isStateWaitTimedOutPayload(value)) {
        return yield* new FluentFiregridError({ message: `state.waitFor ${options.name} timed out` })
      }
      return yield* decodeRowFor(table, value)
    }))

const waitForIndex = <Tbl extends AnyTable>(
  table: Tbl,
  options: StateIndexWaitOptions<RowOf<Tbl>>
): Effect.Effect<RowOf<Tbl>, FluentFiregridError, FluentDurableContext> =>
  withStateBackend("state.waitFor", (backend, ctx) =>
    Effect.gen(function*() {
      if (backend.waitForIndex === undefined) {
        return yield* new FluentFiregridError({
          message: "state.waitFor index waits are not supported by this state backend"
        })
      }
      const validated = yield* validateStateIndexWait(table, options as StateIndexWaitOptions<unknown>)
      const waitId = ctx.stateOperationId?.({
        key: `index:${validated.indexKey}`,
        kind: "waitFor",
        table: table.tableName
      })
      const signalName = stateIndexWaitSignalName(table.tableName, validated.indexKey, options.name)
      const timeoutAt = options.timeoutMs === undefined || ctx.now === undefined
        ? undefined
        : (yield* ctx.now(waitId === undefined ? undefined : { id: `${waitId}:timeoutAt` })) + options.timeoutMs
      const registered = yield* backend.waitForIndex(table.tableName, options.where, {
        environmentVersion: validated.environment.environmentVersion,
        index: options.index,
        indexKey: validated.indexKey,
        name: options.name,
        signalName,
        ...(timeoutAt === undefined ? {} : { timeoutAt }),
        ...(options.timeoutMs === undefined ? {} : { timeoutMs: options.timeoutMs }),
        vars: options.vars,
        ...(waitId === undefined ? {} : { waitId })
      })
      if (Option.isSome(registered)) {
        return yield* decodeRowFor(table, registered.value)
      }
      const value = yield* ctx.waitForSignal<unknown>(signalName, waitId === undefined ? undefined : { id: waitId })
      if (isStateWaitTimedOutPayload(value)) {
        return yield* new FluentFiregridError({ message: `state.waitFor ${options.name} timed out` })
      }
      return yield* decodeRowFor(table, value)
    }))
