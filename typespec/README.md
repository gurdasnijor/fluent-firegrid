# Durable Streams TypeSpec

The TypeSpec source is organized by `PROTOCOL.md` section and protocol
responsibility:

- `main.tsp` defines the service root and imports the protocol modules.
- `common.tsp` contains shared scalars, headers, response envelopes, and
  protocol-wide error shapes from Sections 8-13.
- `streams.tsp` models Section 5 stream URL operations: create, append, close,
  delete, metadata, catch-up reads, long-poll reads, SSE reads, forks, and
  idempotent producer headers.
- `subscriptions.tsp` models Section 6 reserved subscription management:
  subscription configuration, addressing, explicit stream membership, and JWKS
  discovery. Filtered subscription fields live here because they are part of the
  normalized subscription configuration even though the filter semantics are
  introduced in Section 7.4.1.
- `subscription-delivery.tsp` models Sections 7.1-7.3 delivery mechanics:
  webhook callbacks, pull-wake claim/ack/release, generation fencing, and lease
  token flows.
- `schedules.tsp` models the Section 7.4.2 scheduled append extension.

The split is intentionally not a simple control-plane/data-plane split:
subscriptions have both management and delivery concerns, and scheduled append
is a draft coordination extension rather than subscription CRUD.
