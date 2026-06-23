import { AppendInput, AppendRecord, S2Client, S2Conflict, S2NotFound, type StreamConfig } from "effect-s2"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as SchemaAST from "effect/SchemaAST"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as ChangeMessage from "./ChangeMessage.ts"
import { S2StreamDbError } from "./errors.ts"
import { MaterializedState } from "./MaterializedState.ts"

const isS2Conflict = Schema.is(S2Conflict)
const isS2NotFound = Schema.is(S2NotFound)
const isS2StreamDbError = Schema.is(S2StreamDbError)

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
export const primaryKey = <S extends Schema.Top>(schema: S): S => schema.annotate({ [PrimaryKeyAnnotation]: true }) as S

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
  new(): object
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
  <_Self = never>(name: string) => <const Fields extends Schema.Struct.Fields>(fields: Fields): TableClass<Fields> => {
    const schema = Schema.Struct(fields)
    const pkField = findPrimaryKey(schema)
    // oxlint-disable-next-line typescript/no-extraneous-class -- factory returns a nominal static table surface
    class TableImpl {
      static readonly tableName = name
      static readonly schema = schema
      static readonly pkField = pkField
    }
    // Intentional class-factory cast: the static shape plus phantom Row cannot be expressed structurally on a class declaration.
    return TableImpl as unknown as TableClass<Fields>
  }

// ── facade types ─────────────────────────────────────────────────────────────

export interface TableFacade<Row, Key extends string = string> {
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

/**
 * Buffers writes to commit them as one atomic batch. Table-keyed: each write
 * names its `Table` class (self-describing — carries its `type` and primary key),
 * so any table — declared on the db or accessed via `db.table(...)` — can join.
 */
export interface Transaction {
  readonly insert: <T extends AnyTable>(table: T, row: RowOf<T>) => void
  readonly upsert: <T extends AnyTable>(table: T, row: RowOf<T>) => void
  readonly delete: <T extends AnyTable>(table: T, key: string) => void
}

/** The opened db: its declared tables (by name) plus db-wide operations. */
export type StreamDbInstance<T extends Tables> =
  & { readonly [K in keyof T & string]: TableFacade<RowOf<T[K]>> }
  & {
    /**
     * The typed facade for any `Table` over this db's stream — the runtime
     * parallel to the declared `db.<name>` accessors. Pure: the facade is derived
     * from the self-describing `Table` class; nothing is registered or mutated.
     */
    readonly table: <Tbl extends AnyTable>(table: Tbl) => TableFacade<RowOf<Tbl>>
    /** Commit writes across tables atomically (one S2 batch). */
    readonly transact: <A>(body: (tx: Transaction) => A) => Effect.Effect<A, S2StreamDbError>
    /**
     * Append a snapshot of the live set at the current tail, then trim records
     * before it — the first-class checkpoint (today's `compact`, surfaced). This is
     * latest-value projection GC, not an event-log read.
     */
    readonly checkpoint: Effect.Effect<void, S2StreamDbError>
    /** Explicitly trim records before `cursor` (an S2 `seq_num`). */
    readonly trim: (cursor: number) => Effect.Effect<void, S2StreamDbError>
    /** In-stream snapshot + trim; preload cost becomes bounded by live-key count. */
    readonly compact: Effect.Effect<void, S2StreamDbError>
    /** Drop the stream entirely. */
    readonly drop: Effect.Effect<void, S2StreamDbError>
  }

/** Options for `StreamDb.open` — an optional per-stream config for create-if-absent. */
export interface OpenOptions {
  readonly config?: StreamConfig
}

// ── StreamDb — `class X extends StreamDb<X>("base")({ tables }) {}` ───────────

/** A key schema: decodes the db's instance key and encodes it to a path segment. */
export type KeySchema = Schema.Codec<unknown, string>

/** The static shape of a StreamDb class. */
export interface StreamDbClass<T extends Tables, Key extends KeySchema> {
  new(): object
  readonly basePath: string
  readonly key: Key
  readonly tables: T
  /**
   * Open the db over the stream `${basePath}/${encode(key)}` (one S2 stream).
   * The key is validated + encoded to its path segment through `key`'s schema.
   * `options.config` is applied to the create-if-absent (inherits basin defaults
   * when omitted).
   */
  readonly open: (
    key: Key["Type"],
    options?: OpenOptions
  ) => Effect.Effect<StreamDbInstance<T>, S2StreamDbError, S2Client>
  /**
   * The typed instance keys that currently exist as streams under `basePath`:
   * enumerate by db base path, strip `basePath`, decode each suffix through the
   * `key` schema — discovery that returns typed keys (only this layer can invert
   * stream-name → key), not name construction. Streams being deleted are excluded.
   * No `keyPrefix`: an encoded-prefix filter bypasses the key schema, so
   * schema-owned prefix enumeration is a follow-up.
   */
  readonly list: () => Effect.Effect<ReadonlyArray<Key["Type"]>, S2StreamDbError, S2Client>
  /**
   * Open `key` only if its stream already exists; never creates (unlike `open`,
   * which creates-if-absent). Existence is decided by the open itself (the
   * preload's non-creating path), so there is no separate public probe to race
   * against — a missing stream resolves to `None`.
   */
  readonly openExisting: (
    key: Key["Type"]
  ) => Effect.Effect<Option.Option<StreamDbInstance<T>>, S2StreamDbError, S2Client>
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
  <_Self = never>(basePath: string) =>
  <const T extends Tables, Key extends KeySchema = typeof Schema.String>(
    tables: T,
    key?: Key
  ): StreamDbClass<T, Key> => {
    const keySchema = (key ?? Schema.String) as Schema.Codec<unknown, string>
    const encodeKey = (value: unknown) => Schema.encodeUnknownEffect(keySchema)(value)
    const decodeKey = (segment: string) =>
      Schema.decodeUnknownEffect(keySchema)(segment) as Effect.Effect<Key["Type"], Schema.SchemaError>
    const prefix = `${basePath}/`
    // oxlint-disable-next-line typescript/no-extraneous-class -- factory returns a nominal static StreamDb surface
    class StreamDbImpl {
      static readonly basePath = basePath
      static readonly key = keySchema
      static readonly tables = tables
      static readonly open = (
        value: Key["Type"],
        options?: OpenOptions
      ): Effect.Effect<StreamDbInstance<T>, S2StreamDbError, S2Client> =>
        encodeKey(value).pipe(
          Effect.mapError(toError("open")),
          Effect.flatMap((segment) =>
            openStream(
              `${basePath}/${segment}`,
              tables,
              options?.config === undefined ? {} : { config: options.config }
            )
          )
        )
      static readonly list = (): Effect.Effect<ReadonlyArray<Key["Type"]>, S2StreamDbError, S2Client> =>
        // listAllStreams paginates the SDK's stream list for us (includeDeleted
        // defaults false); we strip basePath and decode each suffix to a typed key.
        S2Client.listAllStreams({ prefix }).pipe(
          Stream.filter((info) => info.deletedAt == null),
          Stream.mapEffect((info) => decodeKey(info.name.slice(prefix.length))),
          Stream.runCollect,
          Effect.map((keys) => Array.from(keys)),
          withStreamDbSpan("list", { basePath }),
          Effect.mapError(toError("list"))
        )
      // Race-free: the open's non-creating preload is authoritative —
      // `create: false` propagates `S2NotFound`, which maps to `None`. No
      // separate public `exists` probe races against a concurrent drop.
      static readonly openExisting = (
        value: Key["Type"]
      ): Effect.Effect<Option.Option<StreamDbInstance<T>>, S2StreamDbError, S2Client> =>
        encodeKey(value).pipe(
          Effect.mapError(toError("openExisting")),
          Effect.flatMap((segment) => openStream(`${basePath}/${segment}`, tables, { create: false })),
          Effect.map(Option.some<StreamDbInstance<T>>),
          Effect.catch((error) =>
            isS2StreamDbError(error) && isS2NotFound(error.cause)
              ? Effect.succeedNone
              : Effect.fail(error)
          ),
          withStreamDbSpan("openExisting", { basePath })
        )
    }
    // Intentional class-factory cast: the static shape plus constructor surface cannot be expressed structurally on a class declaration.
    return StreamDbImpl as unknown as StreamDbClass<T, Key>
  }

// ── runtime ──────────────────────────────────────────────────────────────────

const toError = (operation: string) => (cause: unknown): S2StreamDbError =>
  new S2StreamDbError({
    operation,
    message: cause instanceof Error ? cause.message : String(cause),
    cause
  })

type TraceAttributeValue = string | number | boolean
type TraceAttributes = Record<string, TraceAttributeValue>

const withStreamDbSpan =
  (operation: string, attributes: TraceAttributes = {}) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    effect.pipe(Effect.withSpan(`effect-s2-stream-db.${operation}`, { attributes }))

/** Command records (fence/trim) carry an empty-name header; data records do not. */
const isCommandRecord = (headers: ReadonlyArray<readonly [string, string]>): boolean =>
  headers.some(([name]) => name === "")

const change = (
  operation: "insert" | "update" | "delete",
  type: string,
  key: string,
  value?: unknown
): ChangeMessage.Message =>
  operation === "delete"
    ? { type, key, headers: { operation } }
    : { type, key, value, headers: { operation } }

const control = (signal: "snapshot-start" | "snapshot-end", offset: number): ChangeMessage.Message => ({
  headers: { control: signal, offset: String(offset) }
})

/** Max records in one S2 append batch (the snapshot must fit one atomic batch). */
const MAX_BATCH_RECORDS = 1000

class SnapshotBatchTooLarge extends Data.TaggedError("SnapshotBatchTooLarge")<{
  readonly entries: number
  readonly maxRecords: number
}> {
  get message(): string {
    return `snapshot of ${this.entries} live keys exceeds one atomic batch (${this.maxRecords})`
  }
}

class StreamDbSchemaCodecError extends Data.TaggedError("StreamDbSchemaCodecError")<{
  readonly cause: unknown
}> {}

const asServiceFreeEncoder = (schema: TableMeta["schema"]): Schema.Encoder<unknown, never> => schema as never

const asServiceFreeDecoder = (schema: TableMeta["schema"]): Schema.Decoder<unknown, never> => schema as never

type Intent =
  | { readonly _tag: "write"; readonly meta: TableMeta; readonly row: unknown }
  | { readonly _tag: "delete"; readonly meta: TableMeta; readonly key: string }

interface TableMeta {
  readonly type: string
  readonly schema: Schema.Struct<Schema.Struct.Fields>
  readonly pkField: string
}

/** A `Table` class is self-describing — derive its metadata directly, no registry. */
const metaOf = (table: AnyTable): TableMeta => ({
  type: table.tableName,
  schema: table.schema,
  pkField: table.pkField
})

const openStream = <T extends Tables>(
  stream: string,
  tables: T,
  options?: { readonly create?: boolean; readonly config?: StreamConfig }
): Effect.Effect<StreamDbInstance<T>, S2StreamDbError, S2Client> =>
  Effect.gen(function*() {
    const client = yield* S2Client
    const state = MaterializedState.empty()
    const tailRef = yield* Ref.make(0)
    const lock = yield* Semaphore.make(1)

    const encodeSchema = (schema: TableMeta["schema"], row: unknown) =>
      Effect.try({
        try: () => Schema.encodeUnknownSync(asServiceFreeEncoder(schema))(row),
        catch: (cause) => new StreamDbSchemaCodecError({ cause })
      })
    const decodeSchema = (schema: TableMeta["schema"], encoded: unknown) =>
      Effect.try({
        try: () => Schema.decodeUnknownSync(asServiceFreeDecoder(schema))(encoded),
        catch: (cause) => new StreamDbSchemaCodecError({ cause })
      })

    const encodeRow = (meta: TableMeta, row: unknown) =>
      encodeSchema(meta.schema, row).pipe(
        Effect.map((encoded) => ({
          type: meta.type,
          key: String((encoded as Record<string, unknown>)[meta.pkField]),
          encoded
        }))
      )

    const decodeRow = (meta: TableMeta, encoded: unknown) => decodeSchema(meta.schema, encoded)

    // preload: paginate from head, fold data records, skip command records.
    const foldRecord = (
      record: {
        readonly seqNum: number
        readonly body: string
        readonly headers: ReadonlyArray<readonly [string, string]>
      }
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
            })
          )
        )
      )

    // `create: false` (openExisting) never provisions; default ensures-if-absent,
    // applying `config` when given (basin defaults otherwise).
    if (options?.create !== false) {
      yield* client.createStream({ stream, ...(options?.config === undefined ? {} : { config: options.config }) }).pipe(
        Effect.catch((cause) => (isS2Conflict(cause) ? Effect.void : Effect.fail(cause))),
        Effect.mapError(toError("createStream"))
      )
    }
    // An empty stream (tail 0) is a 416 on read and a never-appended one is a 404 —
    // both mean "nothing to fold". `checkTail` distinguishes empty from non-empty.
    // When `create: false` (openExisting), a missing stream (404) is NOT swallowed:
    // the checkTail is the authoritative existence check, so `S2NotFound` propagates
    // and the caller maps it to `None`.
    const swallowMissing = options?.create !== false
    yield* client.checkTail(stream).pipe(
      Effect.flatMap((tail) => (tail.tail.seqNum === 0 ? Ref.set(tailRef, 0) : preloadFrom(0))),
      Effect.catch((cause) => isS2NotFound(cause) && swallowMissing ? Ref.set(tailRef, 0) : Effect.fail(cause)),
      Effect.mapError(toError("preload"))
    )

    // commit a batch of messages atomically: CAS-append, advance tail, then apply.
    // Caller must hold `lock`. Raw error channel; the public op maps to S2StreamDbError.
    const commitLocked = (messages: ReadonlyArray<ChangeMessage.Message>) =>
      Effect.gen(function*() {
        if (messages.length === 0) {
          return
        }
        yield* Effect.annotateCurrentSpan({
          stream,
          messageCount: messages.length
        })
        const bodies = yield* Effect.forEach(messages, ChangeMessage.encode)
        const records = bodies.map((body) => AppendRecord.string({ body }))
        const matchSeqNum = yield* Ref.get(tailRef)
        const ack = yield* client.append(stream, AppendInput.create(records, { matchSeqNum }))
        yield* Ref.set(tailRef, ack.tail.seqNum)
        messages.forEach((message) => state.apply(message))
      }).pipe(withStreamDbSpan("commit", { stream }))

    const makeFacade = (meta: TableMeta): TableFacade<unknown> => ({
      insert: (row) =>
        lock.withPermits(1)(
          encodeRow(meta, row).pipe(
            Effect.flatMap(({ encoded, key, type }) => commitLocked([change("insert", type, key, encoded)]))
          )
        ).pipe(
          withStreamDbSpan("table.insert", { stream, table: meta.type }),
          Effect.mapError(toError("insert"))
        ),
      upsert: (row) =>
        lock.withPermits(1)(
          encodeRow(meta, row).pipe(
            Effect.flatMap(({ encoded, key, type }) => {
              const op = Option.isSome(state.get(type, key)) ? "update" : "insert"
              return commitLocked([change(op, type, key, encoded)])
            })
          )
        ).pipe(
          withStreamDbSpan("table.upsert", { stream, table: meta.type }),
          Effect.mapError(toError("upsert"))
        ),
      delete: (key) =>
        lock.withPermits(1)(commitLocked([change("delete", meta.type, key)])).pipe(
          withStreamDbSpan("table.delete", { stream, table: meta.type }),
          Effect.mapError(toError("delete"))
        ),
      insertOrGet: (row) =>
        lock.withPermits(1)(
          Effect.gen(function*() {
            const { encoded, key, type } = yield* encodeRow(meta, row)
            const existing = state.get(type, key)
            if (Option.isSome(existing)) {
              const decoded = yield* decodeRow(meta, existing.value)
              return { _tag: "Found", row: decoded } satisfies InsertOrGetResult<unknown>
            }
            yield* commitLocked([change("insert", type, key, encoded)])
            return { _tag: "Inserted" } satisfies InsertOrGetResult<unknown>
          })
        ).pipe(
          withStreamDbSpan("table.insertOrGet", { stream, table: meta.type }),
          Effect.mapError(toError("insertOrGet"))
        ),
      // `Effect.suspend` so the read observes `MaterializedState` when the Effect
      // RUNS, not when `get`/`query` is called — otherwise a reused read Effect
      // would freeze the value captured at construction time.
      get: (key) =>
        Effect.suspend(() =>
          Option.match(state.get(meta.type, key), {
            onNone: () => Effect.succeedNone,
            onSome: (encoded) => decodeRow(meta, encoded).pipe(Effect.map(Option.some))
          })
        ).pipe(
          withStreamDbSpan("table.get", { stream, table: meta.type }),
          Effect.mapError(toError("get"))
        ),
      query: (build) =>
        Effect.suspend(() => Effect.forEach(state.values(meta.type), (encoded) => decodeRow(meta, encoded))).pipe(
          withStreamDbSpan("table.query", { stream, table: meta.type }),
          Effect.map(build),
          Effect.mapError(toError("query"))
        )
    })

    const facades = Object.fromEntries(
      Object.entries(tables).map(([name, table]) => [name, makeFacade(metaOf(table))] as const)
    )
    const table = (tbl: AnyTable): TableFacade<unknown> => makeFacade(metaOf(tbl))

    const transact = (body: (tx: Transaction) => unknown) =>
      lock.withPermits(1)(
        Effect.suspend(() => {
          const intents: Array<Intent> = []
          const tx: Transaction = {
            insert: (t, row) => intents.push({ _tag: "write", meta: metaOf(t), row }),
            upsert: (t, row) => intents.push({ _tag: "write", meta: metaOf(t), row }),
            delete: (t, key) => intents.push({ _tag: "delete", meta: metaOf(t), key })
          }
          const result = body(tx)
          return Effect.forEach(intents, (intent) =>
            intent._tag === "delete"
              ? Effect.succeed(change("delete", intent.meta.type, intent.key))
              : encodeRow(intent.meta, intent.row).pipe(
                Effect.map(({ encoded, key, type }) => {
                  const op = Option.isSome(state.get(type, key)) ? "update" : "insert"
                  return change(op, type, key, encoded)
                })
              )).pipe(
              Effect.flatMap((messages) => commitLocked(messages)),
              Effect.as(result)
            )
        })
      ).pipe(
        withStreamDbSpan("transact", { stream }),
        Effect.mapError(toError("transact"))
      )

    // snapshot the live set at the tail + trim before it, as one atomic batch
    // (the trim only lands if the snapshot does — durable-before-trim).
    const runCheckpoint = lock.withPermits(1)(
      Effect.gen(function*() {
        const cursor = yield* Ref.get(tailRef)
        const entries = state.entries()
        yield* Effect.annotateCurrentSpan({ stream, cursor, liveEntries: entries.length })
        if (entries.length + 3 > MAX_BATCH_RECORDS) {
          return yield* new SnapshotBatchTooLarge({ entries: entries.length, maxRecords: MAX_BATCH_RECORDS })
        }
        const startBody = yield* ChangeMessage.encode(control("snapshot-start", cursor))
        const endBody = yield* ChangeMessage.encode(control("snapshot-end", cursor))
        const entryBodies = yield* Effect.forEach(entries, (entry) =>
          ChangeMessage.encode(change("insert", entry.type, entry.key, entry.value)))
        const records = [
          AppendRecord.string({ body: startBody }),
          ...entryBodies.map((body) =>
            AppendRecord.string({ body })
          ),
          AppendRecord.string({ body: endBody }),
          AppendRecord.trim(cursor)
        ]
        const ack = yield* client.append(stream, AppendInput.create(records, { matchSeqNum: cursor }))
        yield* Ref.set(tailRef, ack.tail.seqNum)
      })
    )

    const checkpoint = runCheckpoint.pipe(
      withStreamDbSpan("checkpoint", { stream }),
      Effect.mapError(toError("checkpoint"))
    )

    // `compact` is the original name for `checkpoint`, kept for backward compat.
    const compact = runCheckpoint.pipe(
      withStreamDbSpan("compact", { stream }),
      Effect.mapError(toError("compact"))
    )

    const trim = (cursor: number) =>
      lock.withPermits(1)(
        Effect.gen(function*() {
          const tail = yield* Ref.get(tailRef)
          yield* Effect.annotateCurrentSpan({ stream, cursor })
          const ack = yield* client.append(
            stream,
            AppendInput.create([AppendRecord.trim(cursor)], { matchSeqNum: tail })
          )
          yield* Ref.set(tailRef, ack.tail.seqNum)
        })
      ).pipe(
        withStreamDbSpan("trim", { stream }),
        Effect.mapError(toError("trim"))
      )

    const drop = client.deleteStream({ stream }).pipe(
      withStreamDbSpan("drop", { stream }),
      Effect.mapError(toError("drop"))
    )

    // Intentional dynamic facade cast: the per-table mapped type cannot be proven structurally here.
    return { ...facades, table, transact, checkpoint, trim, compact, drop } as unknown as StreamDbInstance<T>
  }).pipe(withStreamDbSpan("open", { stream }))
