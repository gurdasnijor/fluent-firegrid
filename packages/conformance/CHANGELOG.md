# @durable-streams/conformance-tests

## 0.4.0

### Major Changes

- Merged `@durable-streams/client-conformance-tests` (0.2.11) and
  `@durable-streams/server-conformance-tests` (0.3.5) into a single
  `@durable-streams/conformance-tests` package.

  - Client sources now live under `src/client/`, server sources under
    `src/server/`; the client YAML test cases remain at `test-cases/`.
  - A unified `conformance <client|server>` CLI dispatches to each engine.
    The previous per-engine flags are unchanged after the subcommand.
  - The server library entry is now imported from
    `@durable-streams/conformance-tests/server` (was the root export of
    `@durable-streams/server-conformance-tests`).
  - Engine behavior is otherwise unchanged.
