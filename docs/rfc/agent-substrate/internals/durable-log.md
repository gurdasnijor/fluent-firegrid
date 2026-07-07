# 7. Durable Log Requirements

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

A durable log implementation **MUST** provide:

```txt
append
read from beginning or offset
ordered records
durable acknowledgement semantics
```

It **SHOULD** provide:

```txt
live tailing
opaque offsets
idempotent producer semantics
stream creation/ensure
stream closure / EOF
content type or schema metadata
```

A durable log implementation **MAY** provide:

```txt
forking
retention policies
compression
caching
partitioning
multi-stream namespaces
```

A lower-level durable byte-stream protocol is a suitable log implementation when it specifies append-only writes, catch-up reads, live reads, offsets, closure, and optional idempotent producer headers.

This RFC does not require any specific byte-stream protocol.

The concrete reference shape for this section is: append facts, save the next cursor, replay from that cursor, project facts into read models, and resume work after restart. The requirements below do not require any specific protocol headers, crate names, database tables, or stream paths.

## 7.1 Cursor and Offset Semantics

A cursor is an implementation-defined position token. It MAY be an integer offset, byte offset, record id, vector cursor, or opaque token. A conforming cursor model **MUST** satisfy these requirements:

```txt
cursor_before_first(stream) identifies the start of replay
cursor_after(record) identifies a position after that record
read(stream, cursor_after(record)) never returns that record again
read(stream, cursor_before_first) returns the first retained record, if any
```

Offsets **MUST** be stable for the lifetime of retained records. If retention removes a record needed by a cursor, reads from that cursor **MUST** fail with a retention-gap error rather than silently starting later.

`EOF` means no more records are currently available at or after the requested cursor. It **MUST NOT** mean the stream is permanently closed unless the log also returns an explicit closed marker. A live-tail reader at EOF **MAY** wait for new records. A catch-up reader at EOF **MUST** expose the cursor at which live tailing can resume.

Offsets and cursors **MUST NOT** be used as business identifiers. They identify stream coordinates for replay, catch-up, and proof of observation. Sessions, prompts, tool calls, permissions, and resources use semantic ids from their domain or adapter.

## 7.2 Append Acknowledgement and Ordering

An append is durable only after the log acknowledges it. Acknowledgement **MUST** include enough information to resume reading after the appended records.

If a batch append is accepted atomically, either every record in the batch **MUST** become visible in order or none may become visible. If a log supports partial batch acceptance, the append result **MUST** identify the accepted prefix and the error for the remainder.

For a single stream, readers **MUST** observe records in append order. Across multiple streams, this RFC does not require a global order unless the implementation explicitly defines one. If cross-stream ordering is exposed, the implementation **MUST** document whether it is causal, timestamp-based, transaction-based, or best-effort.

Append order is authoritative within the ordering boundary. Timestamps are useful for UI and diagnostics, but consumers that need agreement on order **MUST** use stream position or the implementation's documented ordering cursor rather than local wall-clock time.

## 7.3 Replay Determinism

Given the same retained stream contents and the same projection/operator version, replay **MUST** produce the same durable outputs that are defined as deterministic. Implementations **MUST NOT** derive replay results from wall-clock time, random values, local process ids, or live resource handles unless those values were already captured in durable records.

Replay consumers **MUST** be able to distinguish catch-up from live processing. Claimed-work operators use that distinction to avoid side effects during replay.

## 7.4 Idempotent Producer Protocol

The log **SHOULD** support append idempotency. If it does, the producer protocol **MUST** define:

```txt
producer identity
idempotency key or sequence number
dedupe scope
dedupe retention window
conflict comparison rule
```

For duplicate append attempts with the same producer and idempotency key:

```txt
same record content -> return original AppendResult
conflicting record content -> return IdempotencyConflict
expired dedupe window -> either reject as expired or accept as new, as documented
```

Transport-level append idempotency is not a replacement for domain idempotency. Launch, prompt, approval, timer, and provider operations still need domain-level dedupe semantics when retries can cross producers or streams.

---
