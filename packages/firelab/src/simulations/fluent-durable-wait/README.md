# fluent-durable-wait

Firelab witness for `features/fluent/coordination/fluent-durable-wait.feature`
over the merged ConsumerSubstrate/post-claim redrive path.

The host models the architecture README flow:

1. A fluent session records correlation data.
2. The session workload records `wait_for` intent before park.
3. Provider ingress appends queryable state-change facts to the session stream.
4. Durable Streams grants pull-wake claims.
5. A post-claim session authority handles the wake: materialize session facts,
   evaluate CEL through `FluentSources`, append L2 wake outcome, then ack/release
   through `DurableConsumerClient`.
6. Replay/redrive returns the journaled match instead of selecting a newer event.

Durable Streams owns wake delivery, claim contention, cursor offsets, ack, and
release. Fluent owns only product-level wait intent and match journaling.
The Firelab verdict is computed from coverage gates over host-side spans. The
driver only waits for the product-visible completion fact and annotates durable
row counts as corroboration.

Covered feature scenarios:

- Wait intent is recorded before parking.
- Catch-up scan prevents lost wakeups.
- Non-matching wake re-suspends the handler.
- Matching wake resolves from CEL predicate.
- Match shorthand is represented by the desugared CEL predicate persisted in
  the wait intent.
- self binding is a recorded correlation snapshot.
- Replay does not re-evaluate against the live world.

Focused verification:

```bash
pnpm --filter firelab simulate:run fluent-durable-wait --timeout-ms 120000
```

Latest local run in this lane after the methodology rework:
`2026-06-06T02-28-23-145Z__fluent-durable-wait`, verdict
`production-path-covered`.
