import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as SchemaAST from "effect/SchemaAST"

import { FluentDurableContext, type FluentDurableContextService, type ObjectStateBackend } from "./context.ts"
import { FluentFiregridError } from "./error.ts"
export {
  cel,
  type CelStatePredicate,
  evaluateStatePredicate,
  type StatePredicate,
  type StatePredicateContext,
  validateStatePredicate
} from "./statePredicate.ts"
import type { StatePredicate } from "./statePredicate.ts"

const PrimaryKeyAnnotation = "@firegrid/fluent-firegrid/primaryKey"
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

export type StateOperation = "insert" | "update" | "delete"
export type StateControl = "snapshot-start" | "snapshot-end" | "reset"

export interface StateChangeMessage {
  readonly type: string
  readonly key: string
  readonly value?: unknown
  readonly old_value?: unknown
  readonly headers: {
    readonly operation: StateOperation
    readonly callId?: string
    readonly ownerId?: string
    readonly txid?: string
  }
}

export interface StateControlMessage {
  readonly headers: {
    readonly control: StateControl
    readonly offset?: string
  }
}

export interface StateReadJournaledMessage {
  readonly type: string
  readonly key: string
  readonly value?: unknown
  readonly headers: {
    readonly callId?: string
    readonly ownerId?: string
    readonly present: boolean
    readonly read: "journaled"
    readonly readId: string
  }
}

export type StateMessage = StateChangeMessage | StateControlMessage | StateReadJournaledMessage

export namespace ChangeMessage {
  export const Operation = Schema.Literals(["insert", "update", "delete"])
  export type Operation = typeof Operation.Type

  export const Control = Schema.Literals(["snapshot-start", "snapshot-end", "reset"])
  export type Control = typeof Control.Type

  export const ChangeMessage = Schema.Struct({
    type: Schema.String,
    key: Schema.String,
    value: Schema.optional(Schema.Unknown),
    old_value: Schema.optional(Schema.Unknown),
    headers: Schema.Struct({
      operation: Operation,
      callId: Schema.optional(Schema.String),
      ownerId: Schema.optional(Schema.String),
      txid: Schema.optional(Schema.String)
    })
  })
  export type ChangeMessage = typeof ChangeMessage.Type

  export const ControlMessage = Schema.Struct({
    headers: Schema.Struct({
      control: Control,
      offset: Schema.optional(Schema.String)
    })
  })
  export type ControlMessage = typeof ControlMessage.Type

  export const ReadJournaledMessage = Schema.Struct({
    type: Schema.String,
    key: Schema.String,
    value: Schema.optional(Schema.Unknown),
    headers: Schema.Struct({
      callId: Schema.optional(Schema.String),
      ownerId: Schema.optional(Schema.String),
      present: Schema.Boolean,
      read: Schema.Literal("journaled"),
      readId: Schema.String
    })
  })
  export type ReadJournaledMessage = typeof ReadJournaledMessage.Type

  export const Message = Schema.Union([ChangeMessage, ControlMessage, ReadJournaledMessage])
  export type Message = typeof Message.Type

  export const isChange = (message: Message): message is ChangeMessage => "operation" in message.headers

  export const isControl = (message: Message): message is ControlMessage => "control" in message.headers

  export const isReadJournaled = (message: Message): message is ReadJournaledMessage => "read" in message.headers

  const Json = Schema.UnknownFromJsonString

  export const encode = (message: Message): Effect.Effect<string, Schema.SchemaError> =>
    Schema.encodeEffect(Message)(message).pipe(Effect.flatMap(Schema.encodeEffect(Json)))

  export const decode = (body: string): Effect.Effect<Message, Schema.SchemaError> =>
    Schema.decodeEffect(Json)(body).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Message)))
}

export class MaterializedState {
  private readonly byType = new Map<string, Map<string, unknown>>()

  static empty(): MaterializedState {
    return new MaterializedState()
  }

  apply(message: ChangeMessage.Message): void {
    if (ChangeMessage.isReadJournaled(message)) {
      return
    }
    if (ChangeMessage.isControl(message)) {
      if (message.headers.control !== "snapshot-end") this.byType.clear()
      return
    }
    const collection = this.collectionFor(message.type)
    if (message.headers.operation === "delete") {
      collection.delete(message.key)
    } else {
      collection.set(message.key, message.value)
    }
  }

  get(type: string, key: string): Option.Option<unknown> {
    return Option.fromNullishOr(this.byType.get(type)?.get(key))
  }

  values(type: string): ReadonlyArray<unknown> {
    const collection = this.byType.get(type)
    return collection === undefined ? [] : Array.from(collection.values())
  }

  entries(): ReadonlyArray<{ readonly type: string; readonly key: string; readonly value: unknown }> {
    return Array.from(this.byType.entries()).flatMap(([type, collection]) =>
      Array.from(collection.entries()).map(([key, value]) => ({ type, key, value }))
    )
  }

  private collectionFor(type: string): Map<string, unknown> {
    const existing = this.byType.get(type)
    if (existing !== undefined) {
      return existing
    }
    const created = new Map<string, unknown>()
    this.byType.set(type, created)
    return created
  }
}

export interface StateBinding<Row, Key extends string = string> {
  readonly get: (key: Key) => Effect.Effect<Option.Option<Row>, FluentFiregridError, FluentDurableContext>
  readonly set: (row: Row) => Effect.Effect<void, FluentFiregridError, FluentDurableContext>
  readonly delete: (key: Key) => Effect.Effect<void, FluentFiregridError, FluentDurableContext>
  readonly waitFor: (
    key: Key,
    options: StateWaitOptions
  ) => Effect.Effect<Row, FluentFiregridError, FluentDurableContext>
}

export interface StateWaitOptions {
  readonly name: string
  readonly timeoutMs?: number
  readonly when: StatePredicate
}

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
  Schema.decodeUnknownEffect(table.schema as unknown as Schema.Codec<unknown, unknown, never, never>)(value).pipe(
    Effect.mapError((cause) =>
      new FluentFiregridError({ cause, message: `failed to decode row for table ${table.tableName}` })
    )
  ) as Effect.Effect<RowOf<Tbl>, FluentFiregridError>

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
  waitFor: (key, options) =>
    withStateBackend("state.waitFor", (backend, ctx) => {
      if (backend.waitFor === undefined) {
        return Effect.fail(new FluentFiregridError({ message: "state.waitFor is not supported by this state backend" }))
      }
      const waitId = ctx.stateOperationId?.({ key, kind: "waitFor", table: table.tableName })
      return backend.waitFor(table.tableName, key, options.when, {
        name: options.name,
        ...(options.timeoutMs === undefined ? {} : { timeoutMs: options.timeoutMs }),
        ...(waitId === undefined ? {} : { waitId })
      }).pipe(Effect.flatMap((value) => decodeRowFor(table, value)))
    })
})
