# @durable-streams/conformance-tests

Conformance test suites for Durable Streams **client** and **server** implementations, in one package behind a single `conformance` CLI.

This package was formed by merging the former `@durable-streams/client-conformance-tests` and `@durable-streams/server-conformance-tests` packages. The two suites verify implementations against the [Durable Streams protocol](../../PROTOCOL.md) but use different harnesses, so they remain distinct engines under a unified entrypoint.

## CLI

```
conformance <client|server> [options]
```

The first argument selects the engine; everything after it is passed to that engine's CLI unchanged.

### Client conformance

Verifies that a client implementation (in any language) correctly implements the protocol, via a stdin/stdout JSON-line adapter protocol and an embedded reference server.

```bash
# Test the built-in TypeScript client
conformance client --run ts

# Test a client adapter in another language
conformance client --run ./adapters/python_adapter.py --suite producer

# Benchmark
conformance client --bench ts --category latency

conformance client --help
```

Test cases are declarative YAML files under [`test-cases/`](./test-cases) (`producer/`, `consumer/`, `lifecycle/`, `validation/`).

### Server conformance

Verifies that a running server correctly implements the protocol, by driving HTTP requests against it with a vitest-based DSL (L1 named consumers, L2 pull-wake, webhook subscriptions, property-based fuzzing).

```bash
# Run once (CI)
conformance server --run http://localhost:4473

# Watch source paths and rerun on change (development)
conformance server --watch src http://localhost:4473

conformance server --help
```

## Programmatic use

The server engine is consumed as a library by in-repo vitest suites:

```ts
import { runConformanceTests } from "@durable-streams/conformance-tests/server"

runConformanceTests({ baseUrl })
```

Subpath exports:

- `@durable-streams/conformance-tests/server` — server conformance harness (`runConformanceTests`, DSLs)
- `@durable-streams/conformance-tests/client` — client runner, benchmark harness, loaders
- `@durable-streams/conformance-tests/protocol` — client adapter protocol types
- `@durable-streams/conformance-tests` — both of the above as `client` / `server` namespaces

## Layout

```
src/
  cli.ts            unified dispatcher (client|server)
  index.ts          root barrel (client/server namespaces)
  client/           client conformance engine (YAML runner + adapter protocol)
  server/           server conformance engine (vitest DSLs)
test-cases/         declarative client test cases (YAML)
bin/
  conformance-dev.mjs   tsx dev wrapper for the dispatcher
```
