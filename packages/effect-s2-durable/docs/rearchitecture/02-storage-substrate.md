# Storage Substrate Axis

## Decision

Use `effect-s2-stream-db` as the shared S2 storage substrate, but keep the
stream/table split clear:

| Durable shape | Example | Correct abstraction |
| --- | --- | --- |
| ordered facts | object `Accepted`, `StateChanged`, `Journaled`, `SignalResolved`, `Completed` | typed event stream |
| latest value by key | service `steps`, `clockWakeups`, `deferreds`, user tables | table / materialized view |

The existing `effect-s2-stream-db` API is a good fit for latest-value tables. It
is not yet the right API for object owner logs because object owner logs need
ordered replay, seq-num cursors, and event records, not a fake table keyed by
sequence number.

## Target Stack

```text
effect-s2
  raw S2 transport, basin/stream admin, append sessions, producers, reads

effect-s2-stream-db
  EventStream<A>: schema/path adapter over ordered S2 records
  Table<A>: latest-state projection by key
  materializers, checkpoints, snapshots, trim helpers

effect-s2-durable
  ActorEvent vocabulary and projection
  object admission/drain/completion semantics
  service/object/shared invocation semantics
  public durable primitives and engine facade
```

## EventStream

Add a generic typed stream abstraction beside `Table` / `StreamDb`, not as a
replacement for them:

- schema-owned event encode/decode;
- schema/key-derived stream paths where useful;
- typed event records carrying seq num and metadata;
- finite reads and live tails;
- append / guarded append / append-session support delegated to `effect-s2`;
- snapshot cursor helpers;
- trim helpers;
- materialization hooks from stream facts into latest-value tables.

Do not duplicate raw S2 client features. `effect-s2` still owns basin/stream
admin, transport retries, producers, append sessions, and low-level reads.

## Object Owner Logs

After `EventStream<A>` exists, change `object/log.ts` to be a thin durable
adapter over `EventStream<ActorEvent>`.

Keep in `effect-s2-durable`:

- `ActorEvent` schema and projection;
- object call-id routing;
- admission idempotency semantics;
- owner FIFO drain semantics;
- fence / owner-drive session semantics;
- durable error mapping meaningful to callers.

Move or reuse from `effect-s2-stream-db`:

- decoded ordered record reads;
- typed append batching/session mechanics;
- checkpoint / snapshot cursor mechanics;
- trim mechanics;
- generic materialization helpers.

## Snapshots And Trimming

Default recovery should be:

1. S2 owner stream is the source of truth.
2. Object projection is rebuilt from snapshot plus tail.
3. Trimming compacts history already covered by snapshots.
4. Restart-based boot recovery re-drives pending heads.
5. Fencing protects against stale cross-host writers.

Use S2 snapshots/trimming through the stream-db substrate:

- write `ActorSnapshot` at a known owner-stream cursor;
- on open/recovery, load snapshot then read only the tail;
- trim records already covered by the snapshot after correctness is tested.

Lease, heartbeat, and claim-sweep are not the default next step. They are only
for prompt peer takeover without waiting for process restart.

## `object/snapshots.ts`

Future recovery-cost boundary:

- write projection snapshot at a cursor;
- load snapshot and read tail;
- trim records already covered by the snapshot.

This should be implemented on top of generic stream-db snapshot/trim helpers
where possible.

## References

- [`docs/sdds/effect-s2-stream-db-relational-ivm-sdd.md`](../../../../docs/sdds/effect-s2-stream-db-relational-ivm-sdd.md)
- [`docs/sdds/effect-s2-stream-db-processor-architecture-api-sdd.md`](../../../../docs/sdds/effect-s2-stream-db-processor-architecture-api-sdd.md)
- https://s2.dev/docs/concepts/snapshots
- https://s2.dev/docs/concepts/trimming
