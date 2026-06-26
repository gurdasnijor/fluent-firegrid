# Firegrid.Trace

Fable-native chDB (embedded ClickHouse) OpenTelemetry span exporter for Firegrid,
ported from the TypeScript `@firegrid/trace` package.

Exposes `ChdbSpanExporter` / `RemoteChdbSpanExporter` (the OTel `SpanExporter`
shape), the `otel_traces` table helpers (`ensureOtelTracesTable`,
`insertChdbSpanRows`, `spanToChdbRow`), the pure `Ch` SQL-literal builders, and a
reduced `ChdbClient` (native chDB query surface only — the effect/unstable/sql
SqlClient / Statement compiler is intentionally not ported).

```sh
NUGET_PACKAGES=$PWD/.nuget/packages dotnet build src/Firegrid.Trace/Firegrid.Trace.fsproj -v:q
```
