import { Effect, Option, Ref, Schema, SchemaAST, Semaphore } from "effect"
import { AppendInput, AppendRecord, S2Client, S2Conflict, S2NotFound } from "effect-s2"
import * as ChangeMessage from "./ChangeMessage.ts"
import { MaterializedState } from "./MaterializedState.ts"
import { S2StreamDbError } from "./errors.ts"

// ── primary key (encoded in the schema) ──────────────────────────────────────

// Annotations are string-keyed in effect@4 (`Annotations.Annotations` is
// `{ readonly [x: string]: unknown }`). Attach via `.annotate`, read via the
// public `SchemaAST.resolveAt`.
const PrimaryKeyAnnotation = "effect-s2-stream-db/primaryKey"
const readPrimaryKey = SchemaAST.resolveAt<boolean>(PrimaryKeyAnnotation)

/**
 * Annotate a field schema as a table's primary key — encoded in the schema
 * itself, so a table is fully described by its `Schema.Struct`.
 *
 * @example
 * class Activity extends Table<Activity>("activities")({
 *   activityKey: Schema.String.pipe(primaryKey),
 *   result: Schema.Unknown,
 * }) {}
 */
export const primaryKey = <S extends Schema.Top>(schema: S): S =>
  schema.annotate({ [PrimaryKeyAnnotation]: true }) as S

const findPrimaryKey = (schema: Schema.Struct<Schema.Struct.Fields>): string => {
  const entry = Object.entries(schema.fields).find(([, field]) => readPrimaryKey(field.ast) === true)
  if (entry === undefined) {
    throw new Error("table schema has no primaryKey-annotated field")
  }
  return entry[0]
}

// ── Table — `class X extends Table<X>("name")({ fields }) {}` ─────────────────

/** The static shape of a Table class: it carries its `type` name, schema, and key. */
export interface TableClass<Fields extends Schema.Struct.Fields> {
  new (): object
  readonly tableName: string
  readonly schema: Schema.Struct<Fields>
  readonly pkField: string
  /** Phantom: the table's decoded row type. */
  readonly Row: Schema.Struct<Fields>["Type"]
}

/** Any table class. */
export type AnyTable = TableClass<Schema.Struct.Fields>

/** The decoded row type of a table class. */
export type RowOf<T extends AnyTable> = T["Row"]

/**
 * Define a table: one typed row collection within a `StreamDb`. `name` is the
 * table's relative path — its State-Protocol `type` discriminator within the
 * db's stream. The primary key is the `primaryKey`-annotated field.
 */
export const Table =
  <Self = never>(name: string) =>
  <const Fields extends Schema.Struct.Fields>(fields: Fields): TableClass<Fields> => {
    const schema = Schema.Struct(fields)
    const pkField = findPrimaryKey(schema)
    class TableImpl {
      static readonly tableName = name
      static readonly schema = schema
      static readonly pkField = pkField
    }
    return TableImpl as unknown as TableClass<Fields>
  }

// ── facade types ─────────────────────────────────────────────────────────────

export interface CollectionFacade<Row, Key extends string = string> {
  /** Append an insert. (No first-writer check — see `insertOrGet`.) */
  readonly insert: (row: Row) => Effect.Effect<void, S2StreamDbError>
  /**
   * Insert if absent, else return the existing row. Check-then-append under the
   * single owner; first-writer-wins holds only because there is one writer.
   */
  readonly insertOrGet: (row: Row) => Effect.Effect<InsertOrGetResult<Row>, S2StreamDbError>
  /** Insert or replace. */
  readonly upsert: (row: Row) => Effect.Effect<void, S2StreamDbError>
  /** Remove by key. */
  readonly delete: (key: Key) => Effect.Effect<void, S2StreamDbError>
  /** Latest value at `key`. */
  readonly get: (key: Key) => Effect.Effect<Option.Option<Row>, S2StreamDbError>
  /** Run a read-only query over all live rows. */
  readonly query: <A>(build: (rows: ReadonlyArray<Row>) => A) => Effect.Effect<A, S2StreamDbError>
}

export type InsertOrGetResult<Row> =
  | { readonly _tag: "Inserted" }
  | { readonly _tag: "Found"; readonly row: Row }

/** A record of named tables. */
export type Tables = Record<string, AnyTable>

/** Buffers writes across tables to commit them as one atomic batch. */
export interface Transaction<T extends Tables> {
  readonly insert: <K extends keyof T & string>(table: K, row: RowOf<T[K]>) => void
  readonly upsert: <K extends keyof T & string>(table: K, row: RowOf<T[K]>) => void
  readonly delete: <K extends keyof T & string>(table: K, key: string) => void
}

/** The opened db: its tables (by name) plus db-wide operations. */
export type StreamDbInstance<T extends Tables> =
  & { readonly [K in keyof T & string]: CollectionFacade<RowOf<T[K]>> }
  & {
    /** Commit writes across tables atomically (one S2 batch). */
    readonly transact: <A>(body: (tx: Transaction<T>) => A) => Effect.Effect<A, S2StreamDbError>
    /** In-stream snapshot + trim; preload cost becomes bounded by live-key count. */
    readonly compact: Effect.Effect<void, S2StreamDbError>
    /** Drop the stream entirely. */
    readonly drop: Effect.Effect<void, S2StreamDbError>
  }

// ── StreamDb — `class X extends StreamDb<X>("base")({ tables }) {}` ───────────

/** A key schema: decodes the db's instance key and encodes it to a path segment. */
export type KeySchema = Schema.Codec<any, string>

/** The static shape of a StreamDb class. */
export interface StreamDbClass<T extends Tables, Key extends KeySchema> {
  new (): object
  readonly basePath: string
  readonly key: Key
  readonly tables: T
  /**
   * Open the db over the stream `${basePath}/${encode(key)}` (one S2 stream).
   * The key is validated + encoded to its path segment through `key`'s schema.
   */
  readonly open: (key: Key["Type"]) => Effect.Effect<StreamDbInstance<T>, S2StreamDbError, S2Client>
}

/**
 * Define an S2 stream db: one S2 stream that aggregates named tables.
 *
 * `basePath` is the db's base stream path; the optional `key` schema types the
 * instance id. `open(key)` validates the key and derives the full stream path
 * `${basePath}/${encode(key)}` — one stream per key (for the workflow engine,
 * per execution). The path is always derived from the schema, never hand-built.
 * `key` defaults to `Schema.String`, so `open("exec-1")` works unrefined.
 */
export const StreamDb =
  <Self = never>(basePath: string) =>
  <const T extends Tables, Key extends KeySchema = typeof Schema.String>(
    tables: T,
    key?: Key,
  ): StreamDbClass<T, Key> => {
    const keySchema = (key ?? Schema.String) as Schema.Codec<unknown, string>
    const encodeKey = (value: unknown) =>
      Schema.encodeUnknownEffect(keySchema)(value) as Effect.Effect<string, Schema.SchemaError>
    class StreamDbImpl {
      static readonly basePath = basePath
      static readonly key = keySchema
      static readonly tables = tables
      static readonly open = (value: Key["Type"]): Effect.Effect<StreamDbInstance<T>, S2StreamDbError, S2Client> =>
        encodeKey(value).pipe(
          Effect.mapError(toError("open")),
          Effect.flatMap((segment) => openStream(`${basePath}/${segment}`, tables)),
        )
    }
    return StreamDbImpl as unknown as StreamDbClass<T, Key>
  }

// ── runtime ──────────────────────────────────────────────────────────────────

const toError = (operation: string) => (cause: unknown): S2StreamDbError =>
  new S2StreamDbError({
    operation,
    message: cause instanceof Error ? cause.message : String(cause),
    cause,
  })

/** Command records (fence/trim) carry an empty-name header; data records do not. */
const isCommandRecord = (headers: ReadonlyArray<readonly [string, string]>): boolean =>
  headers.some(([name]) => name === "")

const change = (
  operation: "insert" | "update" | "delete",
  type: string,
  key: string,
  value?: unknown,
): ChangeMessage.Message =>
  operation === "delete"
    ? { type, key, headers: { operation } }
    : { type, key, value, headers: { operation } }

const control = (signal: "snapshot-start" | "snapshot-end", offset: number): ChangeMessage.Message => ({
  headers: { control: signal, offset: String(offset) },
})

/** Max records in one S2 append batch (the snapshot must fit one atomic batch). */
const MAX_BATCH_RECORDS = 1000

type Intent =
  | { readonly _tag: "write"; readonly table: string; readonly row: unknown }
  | { readonly _tag: "delete"; readonly table: string; readonly key: string }

interface TableMeta {
  readonly type: string
  readonly schema: Schema.Struct<Schema.Struct.Fields>
  readonly pkField: string
}

const openStream = <T extends Tables>(
  stream: string,
  tables: T,
): Effect.Effect<StreamDbInstance<T>, S2StreamDbError, S2Client> =>
  Effect.gen(function*() {
    const client = yield* S2Client
    const state = MaterializedState.empty()
    const tailRef = yield* Ref.make(0)
    const lock = yield* Semaphore.make(1)

    // record key → table metadata (the `type` is the table's `tableName`).
    const meta = new Map<string, TableMeta>(
      Object.entries(tables).map(([key, table]): readonly [string, TableMeta] => [
        key,
        { type: table.tableName, schema: table.schema, pkField: table.pkField },
      ]),
    )
    const metaFor = (table: string): Effect.Effect<TableMeta, S2StreamDbError> => {
      const m = meta.get(table)
      return m === undefined
        ? Effect.fail(toError("table")(new Error(`unknown table: ${table}`)))
        : Effect.succeed(m)
    }

    // Concrete schemas carry no encode/decode services; pin the generic to `never`.
    const encodeSchema = (schema: TableMeta["schema"], row: unknown) =>
      Schema.encodeUnknownEffect(schema)(row) as Effect.Effect<unknown, Schema.SchemaError>
    const decodeSchema = (schema: TableMeta["schema"], encoded: unknown) =>
      Schema.decodeUnknownEffect(schema)(encoded) as Effect.Effect<unknown, Schema.SchemaError>

    const encodeRow = (table: string, row: unknown) =>
      metaFor(table).pipe(
        Effect.flatMap((m) =>
          encodeSchema(m.schema, row).pipe(
            Effect.map((encoded) => ({
              type: m.type,
              key: String((encoded as Record<string, unknown>)[m.pkField]),
              encoded,
            })),
          )
        ),
      )

    const decodeRow = (table: string, encoded: unknown) =>
      metaFor(table).pipe(Effect.flatMap((m) => decodeSchema(m.schema, encoded)))

    // preload: paginate from head, fold data records, skip command records.
    const foldRecord = (
      record: { readonly seqNum: number; readonly body: string; readonly headers: ReadonlyArray<readonly [string, string]> },
    ) =>
      isCommandRecord(record.headers)
        ? Effect.void
        : ChangeMessage.decode(record.body).pipe(Effect.map((message) => state.apply(message)))

    const preloadFrom = (fromSeq: number): Effect.Effect<void, unknown> =>
      client.readBatch(stream, { start: { from: { seqNum: fromSeq }, clamp: true } }).pipe(
        Effect.flatMap((batch) =>
          Effect.forEach(batch.records, foldRecord, { discard: true }).pipe(
            Effect.flatMap(() => {
              const last = batch.records.at(-1)
              const tailSeq = batch.tail?.seqNum ?? (last === undefined ? fromSeq : last.seqNum + 1)
              return last !== undefined && last.seqNum + 1 < tailSeq
                ? preloadFrom(last.seqNum + 1)
                : Ref.set(tailRef, tailSeq)
            }),
          )
        ),
      )

    yield* client.createStream({ stream }).pipe(
      Effect.catch((cause) => (cause instanceof S2Conflict ? Effect.void : Effect.fail(cause))),
      Effect.mapError(toError("createStream")),
    )
    yield* preloadFrom(0).pipe(
      Effect.catch((cause) => (cause instanceof S2NotFound ? Ref.set(tailRef, 0) : Effect.fail(cause))),
      Effect.mapError(toError("preload")),
    )

    // commit a batch of messages atomically: CAS-append, advance tail, then apply.
    // Caller must hold `lock`. Raw error channel; the public op maps to S2StreamDbError.
    const commitLocked = (messages: ReadonlyArray<ChangeMessage.Message>) =>
      Effect.gen(function*() {
        if (messages.length === 0) {
          return
        }
        const bodies = yield* Effect.forEach(messages, ChangeMessage.encode)
        const records = bodies.map((body) => AppendRecord.string({ body }))
        const matchSeqNum = yield* Ref.get(tailRef)
        const ack = yield* client.append(stream, AppendInput.create(records, { matchSeqNum }))
        yield* Ref.set(tailRef, ack.tail.seqNum)
        messages.forEach((message) => state.apply(message))
      })

    const makeFacade = (table: string): CollectionFacade<unknown> => ({
      insert: (row) =>
        lock.withPermits(1)(
          encodeRow(table, row).pipe(
            Effect.flatMap(({ encoded, key, type }) => commitLocked([change("insert", type, key, encoded)])),
          ),
        ).pipe(Effect.mapError(toError("insert"))),
      upsert: (row) =>
        lock.withPermits(1)(
          encodeRow(table, row).pipe(
            Effect.flatMap(({ encoded, key, type }) => {
              const op = Option.isSome(state.get(type, key)) ? "update" : "insert"
              return commitLocked([change(op, type, key, encoded)])
            }),
          ),
        ).pipe(Effect.mapError(toError("upsert"))),
      delete: (key) =>
        lock.withPermits(1)(
          metaFor(table).pipe(Effect.flatMap((m) => commitLocked([change("delete", m.type, key)]))),
        ).pipe(Effect.mapError(toError("delete"))),
      insertOrGet: (row) =>
        lock.withPermits(1)(
          Effect.gen(function*() {
            const { encoded, key, type } = yield* encodeRow(table, row)
            const existing = state.get(type, key)
            if (Option.isSome(existing)) {
              const decoded = yield* decodeRow(table, existing.value)
              return { _tag: "Found", row: decoded } satisfies InsertOrGetResult<unknown>
            }
            yield* commitLocked([change("insert", type, key, encoded)])
            return { _tag: "Inserted" } satisfies InsertOrGetResult<unknown>
          }),
        ).pipe(Effect.mapError(toError("insertOrGet"))),
      get: (key) =>
        metaFor(table).pipe(
          Effect.flatMap((m) =>
            Option.match(state.get(m.type, key), {
              onNone: () => Effect.succeedNone,
              onSome: (encoded) => decodeRow(table, encoded).pipe(Effect.map(Option.some)),
            })
          ),
          Effect.mapError(toError("get")),
        ),
      query: (build) =>
        metaFor(table).pipe(
          Effect.flatMap((m) => Effect.forEach(state.values(m.type), (encoded) => decodeRow(table, encoded))),
          Effect.map(build),
          Effect.mapError(toError("query")),
        ),
    })

    const facades = Object.fromEntries(Object.keys(tables).map((name) => [name, makeFacade(name)] as const))

    const transact = (body: (tx: Transaction<T>) => unknown) =>
      lock.withPermits(1)(
        Effect.suspend(() => {
          const intents: Array<Intent> = []
          const tx = {
            insert: (t: string, row: unknown) => intents.push({ _tag: "write", table: t, row }),
            upsert: (t: string, row: unknown) => intents.push({ _tag: "write", table: t, row }),
            delete: (t: string, key: string) => intents.push({ _tag: "delete", table: t, key }),
          }
          const result = body(tx as Transaction<T>)
          return Effect.forEach(intents, (intent) =>
            intent._tag === "delete"
              ? metaFor(intent.table).pipe(Effect.map((m) => change("delete", m.type, intent.key)))
              : encodeRow(intent.table, intent.row).pipe(
                Effect.map(({ encoded, key, type }) => {
                  const op = Option.isSome(state.get(type, key)) ? "update" : "insert"
                  return change(op, type, key, encoded)
                }),
              )).pipe(
                Effect.flatMap((messages) => commitLocked(messages)),
                Effect.as(result),
              )
        }),
      ).pipe(Effect.mapError(toError("transact")))

    const compact = lock.withPermits(1)(
      Effect.gen(function*() {
        const cursor = yield* Ref.get(tailRef)
        const entries = state.entries()
        if (entries.length + 3 > MAX_BATCH_RECORDS) {
          return yield* Effect.fail(
            new S2StreamDbError({
              operation: "compact",
              message: `snapshot of ${entries.length} live keys exceeds one atomic batch (${MAX_BATCH_RECORDS})`,
              cause: undefined,
            }),
          )
        }
        const startBody = yield* ChangeMessage.encode(control("snapshot-start", cursor))
        const endBody = yield* ChangeMessage.encode(control("snapshot-end", cursor))
        const entryBodies = yield* Effect.forEach(entries, (entry) =>
          ChangeMessage.encode(change("insert", entry.type, entry.key, entry.value)),
        )
        const records = [
          AppendRecord.string({ body: startBody }),
          ...entryBodies.map((body) => AppendRecord.string({ body })),
          AppendRecord.string({ body: endBody }),
          AppendRecord.trim(cursor),
        ]
        const ack = yield* client.append(stream, AppendInput.create(records, { matchSeqNum: cursor }))
        yield* Ref.set(tailRef, ack.tail.seqNum)
      }).pipe(Effect.mapError(toError("compact"))),
    )

    const drop = client.deleteStream({ stream }).pipe(Effect.mapError(toError("drop")))

    return { ...facades, transact, compact, drop } as unknown as StreamDbInstance<T>
  })
