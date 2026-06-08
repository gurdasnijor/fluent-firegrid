# fluent-worker-redrive

Firelab witness for `features/fluent/substrate/fluent-worker-redrive.feature`.

This simulation runs over Firelab's real `DurableStreamTestServer` and the
pinned Durable Streams named-consumer/pull-wake substrate. It does not mock
claim, ack, release, wake delivery, cursor storage, or pull queues.

Coverage:

- DS grants a pull-wake claim and rejects a competing worker with `EPOCH_HELD`.
- Fluent materializes committed session facts through `FluentStore.collectSession`
  while holding the DS claim, independent of the subscription ack offset.
- First drive records a side-effect result and an L2 product outcome; second
  drive replays the journaled result and does not record a second side-effect
  execution.
- `ackAfterDurableProductOutcome` sequences the durable product append before
  the DS ack; an injected product append failure leaves the DS consumer offset
  unchanged.
- Work appended while a claim is held is delivered by DS re-wake after ack and
  release, without a fluent-runtime pending-work queue.

Focused verification:

```bash
pnpm --filter firelab simulate:run fluent-worker-redrive --timeout-ms 120000
```

Latest local run in this lane:
`2026-06-06T01-48-34-032Z__fluent-worker-redrive`, verdict
`production-path-covered`.
