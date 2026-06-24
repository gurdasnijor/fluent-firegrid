/**
 * chDB (embedded ClickHouse) driver for Effect SQL, backed by chdb-node's
 * synchronous `Session`. Structure mirrors `@effect/sql`'s ClickhouseClient so
 * the `sql` tag, SqlSchema, Model and migrations work unchanged.
 *
 * Two encodings, kept separate:
 *   - WIRE VALUE (JSONEachRow): a real, round-trippable Schema codec. Results
 *     decode through it; insert rows encode through it. No SQL literals.
 *   - SQL LITERAL (query params only, since chDB has no bind channel): one-way
 *     code-gen, NOT a Schema (it can't round-trip). Composition is function
 *     composition via the `Ch` combinators below — no regex over type strings.
 *
 * Other chDB realities: in-process + SYNCHRONOUS (Effect.try, not tryPromise);
 * no pool / AbortController / KILL QUERY / server query_id; results are format
 * strings we parse; errors are thrown, with the ClickHouse code recovered from
 * the "Code: NNN. DB::Exception" message.
 *
 * VERIFY against effect/unstable/sql: Connection method set, Client.make
 * options, and Statement.makeCompiler/custom config are taken from the official
 * driver source; generics may need minor tweaks against the actual type defs.
 */
import {
  type ChdbQueryStream,
  type ChdbResult,
  type InsertParams,
  type InsertSummary,
  type QueryOptions,
  Session,
  type StreamOptions
} from "chdb"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as Client from "effect/unstable/sql/SqlClient"
import type { Connection, Row } from "effect/unstable/sql/SqlConnection"
import {
  ConnectionError,
  SqlError,
  SqlSyntaxError,
  StatementTimeoutError,
  UnknownError
} from "effect/unstable/sql/SqlError"
import * as Statement from "effect/unstable/sql/Statement"

const ATTR_DB_SYSTEM_NAME = "db.system.name"
const ATTR_DB_NAMESPACE = "db.namespace"

const escapeString = (s: string): string => s.replace(/\\/g, "\\\\").replace(/'/g, "\\'")

// ── error classification (chDB throws; code is in the message) ───────────────

const codeFromMessage = (cause: unknown): number | undefined => {
  const msg = cause instanceof Error ? cause.message : String(cause)
  const m = /Code:\s*(\d+)/.exec(msg)
  return m !== null ? Number(m[1]) : undefined
}

const SYNTAX_CODES = new Set([36, 47, 60, 62, 81, 242])
const classifyError = (
  cause: unknown,
  message: string,
  operation: string,
  fallback: "connection" | "unknown" = "unknown"
) => {
  const props = { cause, message, operation }
  const code = codeFromMessage(cause)
  if (code !== undefined) {
    if (SYNTAX_CODES.has(code)) return new SqlSyntaxError(props)
    if (code === 159 || code === 160 || code === 209) return new StatementTimeoutError(props)
  }
  return fallback === "connection" ? new ConnectionError(props) : new UnknownError(props)
}

// ── Ch: typed SQL-literal builders (for query params; one-way by design) ─────
// Each value knows its ClickHouse type and how to render a JS value as a
// literal. `array`/`nullable`/`map` compose by function composition, so
// `Ch.array(Ch.nullable(Ch.Int64))` builds both the renderer and the type with
// no regex and no stringly-typed dispatch.

export interface Ch<A> {
  readonly type: string
  readonly lit: (value: A) => string
}

const leaf = <A>(type: string, lit: (value: A) => string): Ch<A> => ({ type, lit })

export const Ch = {
  String: leaf<string>("String", (v) => `'${escapeString(v)}'`),
  Int64: leaf<number | bigint>("Int64", (v) => (typeof v === "bigint" ? v.toString() : String(Math.trunc(v)))),
  UInt64: leaf<number | bigint>("UInt64", (v) => (typeof v === "bigint" ? v.toString() : String(Math.trunc(v)))),
  Float64: leaf<number>("Float64", (v) => String(v)),
  Bool: leaf<boolean>("Bool", (v) => (v ? "true" : "false")),
  /** Pass epoch nanoseconds; rendered losslessly and timezone-independently. */
  DateTime64Nanos: leaf<number | bigint>(
    "DateTime64(9)",
    (v) => `fromUnixTimestamp64Nano(${typeof v === "bigint" ? v.toString() : String(v)})`
  ),
  UUID: leaf<string>("UUID", (v) => `'${escapeString(v)}'`),
  array<A>(inner: Ch<A>): Ch<ReadonlyArray<A>> {
    return leaf(`Array(${inner.type})`, (xs) => `[${xs.map(inner.lit).join(", ")}]`)
  },
  nullable<A>(inner: Ch<A>): Ch<A | null | undefined> {
    return leaf(`Nullable(${inner.type})`, (v) => (v === null || v === undefined ? "NULL" : inner.lit(v)))
  },
  map<K, V>(k: Ch<K>, v: Ch<V>): Ch<ReadonlyMap<K, V> | Record<string, V>> {
    return leaf(`Map(${k.type}, ${v.type})`, (m) => {
      const entries = m instanceof Map ? [...m.entries()] : Object.entries(m as Record<string, V>)
      return `map(${entries.flatMap(([mk, mv]) => [k.lit(mk as K), v.lit(mv as V)]).join(", ")})`
    })
  }
} as const

/** Default literal for untyped `sql` interpolation: dispatch on the JS runtime
 *  value (not on a parsed type string). For anything precise, use Ch + param. */
const defaultLiteral = (value: unknown): string => {
  if (value === null || value === undefined) return "NULL"
  if (Array.isArray(value)) return `[${value.map(defaultLiteral).join(", ")}]`
  switch (typeof value) {
    case "number":
      return (Number.isInteger(value) ? Ch.Int64 : Ch.Float64).lit(value)
    case "bigint":
      return Ch.Int64.lit(value)
    case "boolean":
      return Ch.Bool.lit(value)
    case "string":
      return Ch.String.lit(value)
    case "symbol":
      return Ch.String.lit(value.description ?? value.toString())
    case "function":
      return Ch.String.lit(value.name)
    case "undefined":
      return "NULL"
    case "object":
      if (value instanceof Date) return `fromUnixTimestamp64Milli(${value.getTime()})`
      if (value instanceof Map) {
        return `map(${
          Array.from(value.entries(), ([key, item]) => `${defaultLiteral(key)}, ${defaultLiteral(item)}`).join(", ")
        })`
      }
      return Ch.String.lit(JSON.stringify(value) ?? Object.prototype.toString.call(value))
  }
  return "NULL"
}

// ── compiler: the official ClickHouse one, but it emits final literals ───────
// (chDB can't bind params, so there is nothing to substitute later)

type ChdbLit = Statement.Custom<"ChdbLit", string, undefined> // paramA = pre-rendered literal
export type ChdbCustom = ChdbLit
const chdbLit = Statement.custom<ChdbLit>("ChdbLit")

const escape = Statement.defaultEscape("\"")

export const makeCompiler = (transform?: (_: string) => string) =>
  Statement.makeCompiler<ChdbCustom>({
    dialect: "sqlite",
    placeholder(_i, u) {
      return defaultLiteral(u) // render the literal in place; no {pN: Type}
    },
    onIdentifier: transform !== undefined
      ? (value, withoutTransform) => (withoutTransform ? escape(value) : escape(transform(value)))
      : escape,
    onRecordUpdate() {
      return ["", []]
    },
    onCustom(type) {
      return [type.paramA, []] // emit the pre-rendered literal, bind nothing
    }
  })

// ── fiber refs ────────────────────────────────────────────────────────────────

export const ClientMethod = Context.Reference<"query" | "command">("@chdb/ChdbClient/ClientMethod", {
  defaultValue: () => "query" as const
})

// ── config / model ───────────────────────────────────────────────────────────

export interface ChdbClientConfig {
  /** Session directory. A temp dir is created and removed on scope close if omitted. */
  readonly path?: string
  readonly database?: string
  /** Session-scoped settings, applied via SET (sticky for the Session's lifetime). */
  readonly settings?: Record<string, string | number | boolean>
  readonly spanAttributes?: Record<string, unknown>
  readonly transformResultNames?: (str: string) => string
  readonly transformQueryNames?: (str: string) => string
}

export class ChdbSession extends Context.Service<ChdbSession, Session>()(
  "@firegrid/observability/ChdbClient/ChdbSession"
) {}

export interface ChdbNative {
  readonly query: (sql: string, format?: string) => Effect.Effect<string, SqlError>
  readonly queryBind: (sql: string, args: object, format?: string) => Effect.Effect<string, SqlError>
  readonly queryAsync: (sql: string, options?: QueryOptions) => Effect.Effect<ChdbResult, SqlError>
  readonly queryBindAsync: (sql: string, params: object, options?: QueryOptions) => Effect.Effect<ChdbResult, SqlError>
  readonly insert: (params: InsertParams) => Effect.Effect<InsertSummary, SqlError>
  readonly queryStream: (sql: string, options?: StreamOptions) => Effect.Effect<ChdbQueryStream, SqlError>
}

export interface ChdbClientApi extends Client.SqlClient {
  readonly config: ChdbClientConfig
  readonly native: ChdbNative
  /** Typed literal fragment for a query parameter, e.g. param(Ch.Int64, 5n). */
  readonly param: <A>(ch: Ch<A>, value: A) => Statement.Fragment
  /** Run a statement and decode each row through a Schema (the typed-result path). */
  readonly query: <A, I>(
    schema: Schema.Codec<A, I>,
    statement: Effect.Effect<ReadonlyArray<unknown>, SqlError>
  ) => Effect.Effect<ReadonlyArray<A>, SqlError | Schema.SchemaError>
  /** INSERT row objects, encoded through a Schema to their JSONEachRow form. */
  readonly insertQuery: <A, I>(options: {
    readonly table: string
    readonly values: ReadonlyArray<A>
    readonly schema?: Schema.Codec<A, I>
    readonly format?: string
  }) => Effect.Effect<{ readonly written: number }, SqlError | Schema.SchemaError>
  readonly asCommand: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>
}

export class ChdbClient extends Context.Service<ChdbClient, ChdbClientApi>()(
  "@firegrid/observability/ChdbClient"
) {}

const renderSetting = (v: string | number | boolean): string =>
  typeof v === "string" ? `'${escapeString(v)}'` : typeof v === "boolean" ? (v ? "1" : "0") : String(v)

// ── construction ─────────────────────────────────────────────────────────────

const openSession = (options: ChdbClientConfig): Effect.Effect<Session, SqlError, Scope.Scope> =>
  Effect.acquireRelease(
    Effect.try({
      // With no path, chdb-node opens its own temp directory and removes it on
      // cleanup(); with a path, the on-disk session persists across opens.
      try: () => (options.path === undefined ? new Session() : new Session(options.path)),
      catch: (cause) =>
        new SqlError({ reason: classifyError(cause, "ChdbClient: failed to open session", "connect", "connection") })
    }),
    // cleanup() is idempotent, never throws, and drops the temp dir when ephemeral.
    (s) => Effect.sync(() => s.cleanup())
  )

const bootstrapSession = (session: Session, options: ChdbClientConfig): Effect.Effect<void, SqlError> =>
  Effect.try({
    try: () => {
      if (options.database !== undefined) {
        session.query(`CREATE DATABASE IF NOT EXISTS ${options.database}; USE ${options.database}`)
      }
      if (options.settings !== undefined) {
        Object.entries(options.settings).forEach(([k, v]) => session.query(`SET ${k} = ${renderSetting(v)}`))
      }
      session.query("SELECT 1")
    },
    catch: (cause) =>
      new SqlError({ reason: classifyError(cause, "ChdbClient: bootstrap failed", "connect", "connection") })
  })

export const makeWithSession = (
  session: Session,
  options: ChdbClientConfig
): Effect.Effect<ChdbClientApi, SqlError, Reactivity.Reactivity> =>
  Effect.gen(function*() {
    const compiler = makeCompiler(options.transformQueryNames)
    const transformRows = options.transformResultNames !== undefined
      ? Statement.defaultTransforms(options.transformResultNames).array
      : undefined

    const runString = (sql: string, format: string): Effect.Effect<string, SqlError> =>
      Effect.try({
        try: () => String(session.query(sql, format) ?? ""),
        catch: (cause) => new SqlError({ reason: classifyError(cause, "Failed to execute statement", "execute") })
      })

    const native: ChdbNative = {
      query(sql, format = "CSV") {
        return Effect.try({
          try: () => String(session.query(sql, format) ?? ""),
          catch: (cause) => new SqlError({ reason: classifyError(cause, "Failed to execute native query", "execute") })
        })
      },
      queryBind(sql, args, format = "CSV") {
        return Effect.try({
          try: () => String(session.queryBind(sql, args, format) ?? ""),
          catch: (cause) =>
            new SqlError({ reason: classifyError(cause, "Failed to execute native bound query", "execute") })
        })
      },
      queryAsync(sql, queryOptions) {
        return Effect.tryPromise({
          try: () => session.queryAsync(sql, queryOptions),
          catch: (cause) =>
            new SqlError({ reason: classifyError(cause, "Failed to execute native async query", "execute") })
        })
      },
      queryBindAsync(sql, params, queryOptions) {
        return Effect.tryPromise({
          try: () => session.queryBindAsync(sql, params, queryOptions),
          catch: (cause) =>
            new SqlError({ reason: classifyError(cause, "Failed to execute native async bound query", "execute") })
        })
      },
      insert(params) {
        return Effect.tryPromise({
          try: () => session.insert(params),
          catch: (cause) => new SqlError({ reason: classifyError(cause, "Failed to execute native insert", "insert") })
        })
      },
      queryStream(sql, streamOptions) {
        return Effect.try({
          try: () => session.queryStream(sql, streamOptions),
          catch: (cause) =>
            new SqlError({ reason: classifyError(cause, "Failed to open native query stream", "execute") })
        })
      }
    }

    type TransformRows = NonNullable<Parameters<Connection["execute"]>[2]>

    const parseJson = (s: string): unknown => JSON.parse(s) as unknown
    const parseEachRow = (s: string): ReadonlyArray<Row> =>
      s.trim() !== ""
        ? s.trim().split("\n").map((line) => parseJson(line) as Row)
        : []
    const parseCompactEachRow = (s: string): ReadonlyArray<ReadonlyArray<unknown>> =>
      s.trim() !== ""
        ? s.trim().split("\n").map((line) => {
          const row = parseJson(line)
          return Array.isArray(row) ? row as ReadonlyArray<unknown> : [row]
        })
        : []
    const applyTransformRows = (
      rows: ReadonlyArray<Row>,
      transformRows: TransformRows | undefined
    ): ReadonlyArray<Row> => transformRows !== undefined ? transformRows(rows) : rows

    class ConnectionImpl implements Connection {
      // params are already inlined as literals by the compiler, so they're unused here
      private run(sql: string, _params: ReadonlyArray<unknown>, format = "JSONEachRow") {
        return Effect.withFiber<ReadonlyArray<Row>, SqlError>((fiber) => {
          if (fiber.getRef(ClientMethod) === "command") {
            return Effect.as(runString(sql, "Null"), [])
          }
          return Effect.map(runString(sql, format), parseEachRow)
        })
      }
      execute(sql: string, params: ReadonlyArray<unknown>, transformRows: TransformRows | undefined) {
        return Effect.map(this.run(sql, params), (rows) => applyTransformRows(rows, transformRows))
      }
      executeRaw(sql: string, _params: ReadonlyArray<unknown>) {
        return Effect.map(runString(sql, "JSON"), (s) => (s.trim() !== "" ? parseJson(s) : { data: [] }))
      }
      executeValues(sql: string, _params: ReadonlyArray<unknown>) {
        return Effect.map(runString(sql, "JSONCompactEachRow"), parseCompactEachRow)
      }
      executeValuesUnprepared(sql: string, params: ReadonlyArray<unknown>) {
        return this.executeValues(sql, params)
      }
      executeUnprepared(sql: string, params: ReadonlyArray<unknown>, transformRows: TransformRows | undefined) {
        return this.execute(sql, params, transformRows)
      }
      executeStream(sql: string, params: ReadonlyArray<unknown>, transformRows: TransformRows | undefined) {
        // chdb-node's Session is buffered, so this materializes then chunks.
        return this.run(sql, params).pipe(
          Effect.map((rows) => applyTransformRows(rows, transformRows)),
          Effect.map(Stream.fromIterable),
          Stream.unwrap
        )
      }
    }

    const connection = new ConnectionImpl()

    return Object.assign(
      yield* Client.make({
        acquirer: Effect.succeed(connection),
        compiler,
        spanAttributes: [
          ...(options.spanAttributes !== undefined ? Object.entries(options.spanAttributes) : []),
          [ATTR_DB_SYSTEM_NAME, "clickhouse"],
          [ATTR_DB_NAMESPACE, options.database ?? "default"]
        ],
        transformRows
      }),
      {
        config: options,
        native,
        param<A>(ch: Ch<A>, value: A) {
          return Statement.fragment([chdbLit(ch.lit(value), undefined)])
        },
        query<A, I>(schema: Schema.Codec<A, I>, statement: Effect.Effect<ReadonlyArray<unknown>, SqlError>) {
          const decode = Schema.decodeUnknownEffect(Schema.Array(schema))
          return Effect.flatMap(statement, (rows) => decode(rows))
        },
        insertQuery<A, I>(
          opts: {
            readonly table: string
            readonly values: ReadonlyArray<A>
            readonly schema?: Schema.Codec<A, I>
            readonly format?: string
          }
        ) {
          return Effect.gen(function*() {
            const rows: ReadonlyArray<unknown> = opts.schema !== undefined
              ? yield* Schema.encodeUnknownEffect(Schema.Array(opts.schema))(opts.values)
              : opts.values
            return yield* Effect.try({
              try: () => {
                const format = opts.format ?? "JSONEachRow"
                const data = rows.map((v) => JSON.stringify(v)).join("\n")
                session.query(`INSERT INTO ${opts.table} FORMAT ${format}\n${data}`)
                return { written: opts.values.length }
              },
              catch: (cause) => new SqlError({ reason: classifyError(cause, "Failed to insert data", "insert") })
            })
          })
        },
        asCommand<A, E, R>(effect: Effect.Effect<A, E, R>) {
          return Effect.provideService(effect, ClientMethod, "command")
        }
      }
    ) as ChdbClientApi
  })

export const make = (
  options: ChdbClientConfig
): Effect.Effect<ChdbClientApi, SqlError, Scope.Scope | Reactivity.Reactivity> =>
  Effect.gen(function*() {
    const session = yield* openSession(options)
    yield* bootstrapSession(session, options)
    return yield* makeWithSession(session, options)
  })

// ── layers ───────────────────────────────────────────────────────────────────

export const sessionLayer = (config: ChdbClientConfig): Layer.Layer<ChdbSession, SqlError> =>
  Layer.effectContext(
    Effect.gen(function*() {
      const session = yield* openSession(config)
      yield* bootstrapSession(session, config)
      return Context.make(ChdbSession, session)
    })
  )

export const layerFromSession = (
  config: ChdbClientConfig
): Layer.Layer<ChdbClient | Client.SqlClient, SqlError, ChdbSession> =>
  Layer.effectContext(
    Effect.gen(function*() {
      const session = yield* ChdbSession
      const client = yield* makeWithSession(session, config)
      return Context.make(ChdbClient, client).pipe(Context.add(Client.SqlClient, client))
    })
  ).pipe(Layer.provide(Reactivity.layer))

export const layer = (config: ChdbClientConfig): Layer.Layer<ChdbSession | ChdbClient | Client.SqlClient, SqlError> =>
  layerFromSession(config).pipe(Layer.provideMerge(sessionLayer(config)))
