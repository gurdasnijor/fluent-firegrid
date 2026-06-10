# @durable-streams/server-conformance-tests

Protocol compliance test suite for Durable Streams server implementations.

This package provides a comprehensive test suite to verify that a server correctly implements the [Durable Streams protocol](../../PROTOCOL.md).

## Installation

```bash
npm install @durable-streams/server-conformance-tests
# or
pnpm add @durable-streams/server-conformance-tests
```

## CLI Usage

The easiest way to run conformance tests against your server:

### Run Once (CI)

```bash
npx @durable-streams/server-conformance-tests --run http://localhost:4437
```

### Watch Mode (Development)

Watch source files and automatically rerun tests when changes are detected:

```bash
npx @durable-streams/server-conformance-tests --watch src http://localhost:4437

# Watch multiple directories
npx @durable-streams/server-conformance-tests --watch src lib http://localhost:4437
```

### CLI Options

```
Usage:
  npx @durable-streams/server-conformance-tests --run <url>
  npx @durable-streams/server-conformance-tests --watch <path> [path...] <url>

Options:
  --run              Run tests once and exit (for CI)
  --watch <paths>    Watch source paths and rerun tests on changes (for development)
  --help, -h         Show help message

Arguments:
  <url>              Base URL of the Durable Streams server to test against
```

## Programmatic Usage

You can also run the conformance tests programmatically within your own test suite:

```typescript
import { runConformanceTests } from "@durable-streams/server-conformance-tests"

// In your test file (e.g., with vitest)
describe("My Server Implementation", () => {
  const config = { baseUrl: "" }

  beforeAll(async () => {
    // Start your server
    const server = await startMyServer({ port: 0 })
    config.baseUrl = server.url
  })

  afterAll(async () => {
    await server.stop()
  })

  // Run all conformance tests
  runConformanceTests(config)
})
```

## CI Integration

Add conformance tests to your CI pipeline:

```yaml
# GitHub Actions example
jobs:
  conformance:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: "20"

      - name: Install dependencies
        run: npm install

      - name: Start server
        run: npm run start:server &

      - name: Wait for server
        run: npx wait-on http://localhost:4437

      - name: Run conformance tests
        run: npx @durable-streams/server-conformance-tests --run http://localhost:4437
```

## Test Coverage

The conformance test suite covers:

- **Basic Stream Operations** - Create, delete, idempotent operations
- **Append Operations** - String data, chunking, sequence ordering
- **Read Operations** - Empty/full streams, offset reads
- **Long-Poll Operations** - Data waiting, immediate returns
- **HTTP Protocol** - Headers, status codes, content negotiation
- **TTL and Expiry** - TTL/Expires-At handling
- **Case-Insensitivity** - Content-type, header casing
- **Content-Type Validation** - Match enforcement
- **HEAD Metadata** - Metadata-only responses
- **Offset Validation** - Malformed offsets, resumable reads
- **Protocol Edge Cases** - Empty bodies, binary data, monotonic progression
- **Byte-Exactness** - Data integrity guarantees
- **Caching and ETag** - ETag and 304 responses
- **Chunking and Large Payloads** - Pagination, large files
- **Property-Based Fuzzing** - Random append/read sequences
- **Malformed Input Fuzzing** - Security-focused tests
- **Read-Your-Writes Consistency** - Immediate visibility after writes
- **SSE Mode** - Server-sent events streaming
- **JSON Mode** - JSON serialization and batching

## Draft Coordination Coverage

`PROTOCOL.md` Section 7.4 defines draft coordination substrate extensions. A
server should not advertise one of these capabilities until this package
contains passing conformance coverage for it.

### Filtered Subscriptions

Required coverage:

- Creating a webhook and pull-wake subscription with a valid filter succeeds,
  and idempotent re-confirmation includes the normalized filter in the config
  hash.
- Re-confirming the same subscription ID with a different filter returns
  `409 Conflict`.
- Invalid filter syntax returns `400 Bad Request` and does not create the
  subscription.
- A non-matching JSON append does not create a webhook delivery or pull-wake
  event.
- A matching JSON append creates exactly the normal L2 wake shape, including
  generation fencing and stream cursor metadata.
- Non-matching events before a later matching event do not get re-scanned into
  duplicate wakes.
- Acking after a filtered wake advances the public `acked_offset` and prevents
  redelivery after release or lease expiry.
- Incompatible content-type handling follows the protocol: registration rejects
  known incompatible streams, and future incompatible streams do not crash the
  subscription worker.

### Scheduled Append

Required coverage:

- `PUT /__ds/schedules/:id` creates a pending schedule and `GET` returns its
  status.
- Repeating `PUT` with the same normalized schedule is idempotent; changing the
  normalized body returns `409 Conflict`.
- A schedule never appends before its `at` timestamp.
- A due schedule appends through the normal stream write path and wakes matching
  subscriptions.
- A scheduled append carrying a producer tuple is deduplicated if the scheduler
  retries the fire operation.
- `DELETE` cancels a pending schedule and prevents the target append.
- A schedule survives server restart or Durable Object eviction before firing.
- Failed target appends transition the schedule to `failed` with a protocol
  error instead of retrying forever.

### Commit-once Append

Required coverage:

- Existing producer tuple conformance is exercised through immediate appends,
  scheduled appends, and any helper APIs that append on behalf of a caller.
- Duplicate producer tuples return the duplicate classification without
  mutating stream contents.
- Producer fencing runs before stream-closed conflict classification, matching
  the base producer conformance tests.
- Distinct producer IDs and epochs remain independent on the same stream.

### Child and Attachment Composition

Required coverage:

- A parent stream can create a child stream, append an invocation fact, and wait
  for a terminal child fact through a filtered subscription.
- Progress facts wake attachment subscribers without closing or otherwise
  mutating the child stream.
- Terminal facts written with a producer tuple are idempotent under retry.
- Parent recovery from a saved subscription cursor does not miss a terminal fact
  written while the parent was offline.

### Effect Client Witness

The `effect-durable-streams` package should have package-level witness tests for
each exposed coordination helper:

- registering filtered subscriptions;
- running a pull-wake claim/ack/release loop;
- creating, cancelling, and observing scheduled appends;
- using producer tuples through scheduled and immediate append helpers; and
- composing child streams without importing server internals.

## License

Apache 2.0
