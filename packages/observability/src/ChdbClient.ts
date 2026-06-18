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
import { Session } from "chdb"
import { Schema } from "effect"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as Client from "effect/unstable/sql/SqlClient"
import type { Connection } from "effect/unstable/sql/SqlConnection"
import { ConnectionError, SqlError, SqlSyntaxError, StatementTimeoutError, UnknownError } from "effect/unstable/sql/SqlError"
import * as Statement from "effect/unstable/sql/Statement"

const ATTR_DB_SYSTEM_NAME = "db.system.name"
const ATTR_DB_NAMESPACE = "db.namespace"

const escapeString = (s: string): string => s.replace(/\\/g, "\\\\").replace(/'/g, "\\'")

// ── error classification (chDB throws; code is in the message) ───────────────

const codeFromMessage = (cause: unknown): number | undefined => {
  const msg = cause instanceof Error ? cause.message : String(cause)
  const m = /Code:\s*(\d+)/.exec(msg)
  return m ? Number(m[1]) : undefined
}

const SYNTAX_CODES = new Set([36, 47, 60, 62, 81, 242])
const classifyError = (cause: unknown, message: string, operation: string, fallback: "connection" | "unknown" = "unknown") => {
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
  DateTime64Nanos: leaf<number | bigint>("DateTime64(9)", (v) => `fromUnixTimestamp64Nano(${typeof v === "bigint" ? v.toString() : String(v)})`),
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
const defaultCh = (value: unknown): Ch<any> => {
  if (value === null || value === undefined) return leaf("Nullable(String)", () => "NULL")
  if (Array.isArray(value)) return Ch.array(value.length ? defaultCh(value[0]) : Ch.String)
  switch (typeof value) {
    case "number":
      return Number.isInteger(value) ? Ch.Int64 : Ch.Float64
    case "bigint":
      return Ch.Int64
    case "boolean":
      return Ch.Bool
    case "object":
      if (value instanceof Date) return leaf<Date>("DateTime64(3)", (d) => `fromUnixTimestamp64Milli(${d.getTime()})`)
      if (value instanceof Map) return Ch.map(Ch.String, Ch.String)
      return Ch.String
    default:
      return Ch.String
  }
}

// ── compiler: the official ClickHouse one, but it emits final literals ───────
// (chDB can't bind params, so there is nothing to substitute later)

export type ChdbCustom = ChdbLit
interface ChdbLit extends Statement.Custom<"ChdbLit", string, undefined> {} // paramA = pre-rendered literal
const chdbLit = Statement.custom<ChdbLit>("ChdbLit")

const escape = Statement.defaultEscape("\"")

export const makeCompiler = (transform?: (_: string) => string) =>
  Statement.makeCompiler<ChdbCustom>({
    dialect: "sqlite",
    placeholder(_i, u) {
      return defaultCh(u).lit(u) // render the literal in place; no {pN: Type}
    },
    onIdentifier: transform
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

export interface ChdbClient extends Client.SqlClient {
  readonly config: ChdbClientConfig
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

export const ChdbClient = Context.Service<ChdbClient>("@chdb/ChdbClient")

const renderSetting = (v: string | number | boolean): string =>
  typeof v === "string" ? `'${escapeString(v)}'` : typeof v === "boolean" ? (v ? "1" : "0") : String(v)

// ── construction ─────────────────────────────────────────────────────────────

export const make = (
  options: ChdbClientConfig
): Effect.Effect<ChdbClient, SqlError, Scope.Scope | Reactivity.Reactivity> =>
  Effect.gen(function*() {
    const compiler = makeCompiler(options.transformQueryNames)
    const transformRows = options.transformResultNames
      ? Statement.defaultTransforms(options.transformResultNames).array
      : undefined

    const session = yield* Effect.acquireRelease(
      Effect.try({
        // With no path, chdb-node opens its own temp directory and removes it on
        // cleanup(); with a path, the on-disk session persists across opens.
        try: () => (options.path === undefined ? new Session() : new Session(options.path)),
        catch: (cause) => new SqlError({ reason: classifyError(cause, "ChdbClient: failed to open session", "connect", "connection") })
      }),
      // cleanup() is idempotent, never throws, and drops the temp dir when ephemeral.
      (s) => Effect.sync(() => s.cleanup())
    )

    yield* Effect.try({
      try: () => {
        if (options.database) session.query(`CREATE DATABASE IF NOT EXISTS ${options.database}; USE ${options.database}`)
        if (options.settings) {
          for (const [k, v] of Object.entries(options.settings)) session.query(`SET ${k} = ${renderSetting(v)}`)
        }
        session.query("SELECT 1")
      },
      catch: (cause) => new SqlError({ reason: classifyError(cause, "ChdbClient: bootstrap failed", "connect", "connection") })
    })

    const runString = (sql: string, format: string): Effect.Effect<string, SqlError> =>
      Effect.try({
        try: () => String(session.query(sql, format) ?? ""),
        catch: (cause) => new SqlError({ reason: classifyError(cause, "Failed to execute statement", "execute") })
      })

    const parseEachRow = (s: string): ReadonlyArray<any> => (s.trim() ? s.trim().split("\n").map((line) => JSON.parse(line)) : [])

    class ConnectionImpl implements Connection {
      // params are already inlined as literals by the compiler, so they're unused here
      private run(sql: string, _params: ReadonlyArray<unknown>, format = "JSONEachRow") {
        return Effect.withFiber<ReadonlyArray<any>, SqlError>((fiber) => {
          if (fiber.getRef(ClientMethod) === "command") {
            return Effect.as(runString(sql, "Null"), [])
          }
          return Effect.map(runString(sql, format), parseEachRow)
        })
      }
      execute(sql: string, params: ReadonlyArray<unknown>, transformRows: (<A extends object>(r: ReadonlyArray<A>) => ReadonlyArray<A>) | undefined) {
        return transformRows ? Effect.map(this.run(sql, params), transformRows) : this.run(sql, params)
      }
      executeRaw(sql: string, _params: ReadonlyArray<unknown>) {
        return Effect.map(runString(sql, "JSON"), (s) => (s.trim() ? JSON.parse(s) : { data: [] }))
      }
      executeValues(sql: string, params: ReadonlyArray<unknown>) {
        return this.run(sql, params, "JSONCompactEachRow")
      }
      executeUnprepared(sql: string, params: ReadonlyArray<unknown>, transformRows?: any) {
        return this.execute(sql, params, transformRows)
      }
      executeStream(sql: string, params: ReadonlyArray<unknown>, transformRows: (<A extends object>(r: ReadonlyArray<A>) => ReadonlyArray<A>) | undefined) {
        // chdb-node's Session is buffered, so this materializes then chunks.
        return this.run(sql, params).pipe(
          Effect.map((rows) => (transformRows ? transformRows(rows) : rows)),
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
          ...(options.spanAttributes ? Object.entries(options.spanAttributes) : []),
          [ATTR_DB_SYSTEM_NAME, "clickhouse"],
          [ATTR_DB_NAMESPACE, options.database ?? "default"]
        ],
        transformRows
      }),
      {
        config: options,
        param<A>(ch: Ch<A>, value: A) {
          return Statement.fragment([chdbLit(ch.lit(value), undefined)])
        },
        query<A, I>(schema: Schema.Codec<A, I>, statement: Effect.Effect<ReadonlyArray<unknown>, SqlError>) {
          const decode = Schema.decodeUnknownEffect(Schema.Array(schema))
          return Effect.flatMap(statement, (rows) => decode(rows))
        },
        insertQuery<A, I>(opts: { readonly table: string; readonly values: ReadonlyArray<A>; readonly schema?: Schema.Codec<A, I>; readonly format?: string }) {
          const encode = opts.schema
            ? Schema.encodeUnknownEffect(Schema.Array(opts.schema))
            : (xs: ReadonlyArray<A>) => Effect.succeed(xs as ReadonlyArray<unknown>)
          return Effect.flatMap(encode(opts.values), (rows) =>
            Effect.try({
              try: () => {
                const format = opts.format ?? "JSONEachRow"
                const data = (rows as ReadonlyArray<unknown>).map((v) => JSON.stringify(v)).join("\n")
                session.query(`INSERT INTO ${opts.table} FORMAT ${format}\n${data}`)
                return { written: opts.values.length }
              },
              catch: (cause) => new SqlError({ reason: classifyError(cause, "Failed to insert data", "insert") })
            }))
        },
        asCommand<A, E, R>(effect: Effect.Effect<A, E, R>) {
          return Effect.provideService(effect, ClientMethod, "command")
        }
      }
    ) as ChdbClient
  })

// ── layers ───────────────────────────────────────────────────────────────────

export const layer = (config: ChdbClientConfig): Layer.Layer<ChdbClient | Client.SqlClient, SqlError> =>
  Layer.effectContext(
    Effect.map(make(config), (client) => Context.make(ChdbClient, client).pipe(Context.add(Client.SqlClient, client)))
  ).pipe(Layer.provide(Reactivity.layer))