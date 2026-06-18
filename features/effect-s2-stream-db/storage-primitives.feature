Feature: Storage primitives

  @sql:checkpoint_trace_shape
  Scenario: checkpoint snapshots the live set and reopens from the compacted stream
    Given an open storage db with infinite retention at key "cart"
    When I insert item "a" value 1
    And I upsert item "a" value 2
    And I insert item "b" value 3
    And I delete item "a"
    And I checkpoint
    Then reopening, item "a" is absent
    And reopening, item "b" is 3

  @unverified
  Rule: Schema-owned table definitions

    Scenario: Table(name)(fields) derives tableName, row schema, and primary-key field from the Effect Schema definition
      Then Table(name)(fields) derives tableName, row schema, and primary-key field from the Effect Schema definition.

    Scenario: A primary key field is declared by annotating a field schema with primaryKey; defining a table without a primary key fails at definition time
      Then A primary key field is declared by annotating a field schema with primaryKey; defining a table without a primary key fails at definition time.

    Scenario: Row input/output crosses the storage boundary through the table schema; invalid rows fail before appending and stored rows are decoded before get/query returns them
      Then Row input/output crosses the storage boundary through the table schema; invalid rows fail before appending and stored rows are decoded before get/query returns them.

    Scenario: A Table class is self-describing, so db.table(TableClass) can produce a typed facade without mutating a registry
      Then A Table class is self-describing, so db.table(TableClass) can produce a typed facade without mutating a registry.

  @unverified
  Rule: Schema-owned instance keys

    Scenario: StreamDb(basePath)(tables, keySchema?) derives the full stream name by encoding the instance key through the key schema
      Then StreamDb(basePath)(tables, keySchema?) derives the full stream name by encoding the instance key through the key schema.

    Scenario: The key schema defaults to Schema.String, but branded/refined key schemas are preserved through open/list/openExisting
      Then The key schema defaults to Schema.String, but branded/refined key schemas are preserved through open/list/openExisting.

    Scenario: Instance enumeration decodes stream-name suffixes back through the key schema before returning keys
      Then Instance enumeration decodes stream-name suffixes back through the key schema before returning keys.

  @unverified
  Rule: Per-stream config on open

    Scenario: StreamDb.open accepts an optional StreamConfig (retentionPolicy / deleteOnEmpty / storageClass) applied to the create-if-absent
      Then StreamDb.open accepts an optional StreamConfig (retentionPolicy / deleteOnEmpty / storageClass) applied to the create-if-absent.

    Scenario: Omitting config preserves current behaviour (create-if-absent, inherit basin defaults) — fully backward compatible
      Then Omitting config preserves current behaviour (create-if-absent, inherit basin defaults) — fully backward compatible.

    Scenario: Age/size retention is for ephemeral streams only; a stream that mixes permanent state with transient records must not be GC'd by age retention (it uses CHECKPOINT instead)
      Then Age/size retention is for ephemeral streams only; a stream that mixes permanent state with transient records must not be GC'd by age retention (it uses CHECKPOINT instead).

  @unverified
  Rule: Enumerate instances

    Scenario: StreamDb.list() returns typed instance keys for existing streams under the db base path, decoded through the key schema. The operation is discovery, not name construction; raw encoded-prefix filtering is intentionally out of scope
      Then StreamDb.list() returns typed instance keys for existing streams under the db base path, decoded through the key schema. The operation is discovery, not name construction; raw encoded-prefix filtering is intentionally out of scope.

    Scenario: Existence equals liveness only for streams dropped on completion; for persistent streams, existence means the instance exists, and a separate work check is required. includeDeleted defaults false — a dropped instance no longer enumerates
      Then Existence equals liveness only for streams dropped on completion; for persistent streams, existence means the instance exists, and a separate work check is required. includeDeleted defaults false — a dropped instance no longer enumerates.

  @unverified
  Rule: Non-creating open

    Scenario: openExisting(key) returns None for a missing stream and never creates it; the non-creating open path is authoritative, with no separate public exists/probe API that can race with deletion
      Then openExisting(key) returns None for a missing stream and never creates it; the non-creating open path is authoritative, with no separate public exists/probe API that can race with deletion.

  @unverified
  Rule: Latest-value table projection

    Scenario: The latest-value materialized fold is the read lens for user state and materialized views — get/query read the current value per (table, key)
      Then The latest-value materialized fold is the read lens for user state and materialized views — get/query read the current value per (table, key).

    Scenario: The same MaterializedState fold is used for cold preload and live apply-after-ack, so replay and live state cannot diverge by using different materializers
      Then The same MaterializedState fold is used for cold preload and live apply-after-ack, so replay and live state cannot diverge by using different materializers.

    Scenario: Control messages are fold controls: snapshot-start/reset clear the materialized view, snapshot-end is a boundary marker with no table effect
      Then Control messages are fold controls: snapshot-start/reset clear the materialized view, snapshot-end is a boundary marker with no table effect.

    Scenario: S2 command records such as trim/fence are skipped by the ChangeMessage fold and are never decoded as table rows
      Then S2 command records such as trim/fence are skipped by the ChangeMessage fold and are never decoded as table rows.

  @unverified
  Rule: Table writes and read-after-ack visibility

    Scenario: insert appends an insert ChangeMessage and does not perform a first-writer check
      Then insert appends an insert ChangeMessage and does not perform a first-writer check.

    Scenario: insertOrGet is first-writer-wins within the opened single-writer instance: it returns Found with the decoded existing row when the key is already live, otherwise appends the insert and returns Inserted
      Then insertOrGet is first-writer-wins within the opened single-writer instance: it returns Found with the decoded existing row when the key is already live, otherwise appends the insert and returns Inserted.

    Scenario: upsert appends insert for an absent key and update for a live key
      Then upsert appends insert for an absent key and update for a live key.

    Scenario: delete appends a delete ChangeMessage and removes the key from subsequent get/query projection reads
      Then delete appends a delete ChangeMessage and removes the key from subsequent get/query projection reads.

    Scenario: Writes become visible to get/query only after the S2 append acknowledgement and local projection application
      Then Writes become visible to get/query only after the S2 append acknowledgement and local projection application.

    Scenario: Reusing a get/query Effect observes state at run time, not at Effect construction time
      Then Reusing a get/query Effect observes state at run time, not at Effect construction time.

  @unverified
  Rule: Atomic cross-table commits

    Scenario: transact buffers insert/upsert/delete intents across tables and commits them as one conditional S2 append batch
      Then transact buffers insert/upsert/delete intents across tables and commits them as one conditional S2 append batch.

    Scenario: A successful transaction applies every buffered mutation to the local projection after the append acknowledgement
      Then A successful transaction applies every buffered mutation to the local projection after the append acknowledgement.

    Scenario: A failed transaction does not partially apply buffered mutations to the local projection
      Then A failed transaction does not partially apply buffered mutations to the local projection.

    Scenario: Transaction writes are keyed by self-describing Table classes, so declared db tables and db.table(TableClass) participate in the same atomic commit
      Then Transaction writes are keyed by self-describing Table classes, so declared db tables and db.table(TableClass) participate in the same atomic commit.

  @unverified
  Rule: Caller-driven checkpoint + trim

    Scenario: An opened StreamDb instance exposes checkpoint, which appends a snapshot of its live set at a cursor, then trims records before that cursor (surfacing today's internal compact as a first-class operation)
      Then An opened StreamDb instance exposes checkpoint, which appends a snapshot of its live set at a cursor, then trims records before that cursor (surfacing today's internal compact as a first-class operation).

    Scenario: An opened StreamDb instance exposes trim(cursor), which issues an explicit trim before cursor
      Then An opened StreamDb instance exposes trim(cursor), which issues an explicit trim before cursor.

    Scenario: A snapshot must be durable before any trim that depends on it
      Then A snapshot must be durable before any trim that depends on it.

    Scenario: A checkpoint snapshot must fit one S2 batch (MAX_BATCH_RECORDS); larger snapshots require framed/chunked snapshots (a follow-up)
      Then A checkpoint snapshot must fit one S2 batch (MAX_BATCH_RECORDS); larger snapshots require framed/chunked snapshots (a follow-up).

  @unverified
  Rule: Stream lifecycle

    Scenario: compact remains a compatibility alias for checkpoint and preserves its snapshot+trim semantics
      Then compact remains a compatibility alias for checkpoint and preserves its snapshot+trim semantics.

    Scenario: drop deletes the underlying S2 stream; after drop, default enumeration excludes the instance
      Then drop deletes the underlying S2 stream; after drop, default enumeration excludes the instance.

    Scenario: openExisting is the only public non-creating existence path; there is no separate exists probe
      Then openExisting is the only public non-creating existence path; there is no separate exists probe.

  @unverified
  Rule: Latest-value storage, not generic event log

    Scenario: Stream-db exposes a latest-value ChangeMessage projection over S2; it is not a generic ordered event-log abstraction
      Then Stream-db exposes a latest-value ChangeMessage projection over S2; it is not a generic ordered event-log abstraction.

    Scenario: Ordered ActorEvent replay by S2 seq_num is a schema-owned actor log over effect-s2.readDecoded (stateful-execution LAYERING.6), not a StreamDb primitive
      Then Ordered ActorEvent replay by S2 seq_num is a schema-owned actor log over effect-s2.readDecoded (stateful-execution LAYERING.6), not a StreamDb primitive.

    Scenario: Stream-db must not add readLog/collectLog-style event-log APIs as a convenience over this latest-value abstraction
      Then Stream-db must not add readLog/collectLog-style event-log APIs as a convenience over this latest-value abstraction.

  @unverified
  Rule: Schema-derived identity only

    Scenario: Public callers open/list by typed keys, not by hand-built stream paths
      Then Public callers open/list by typed keys, not by hand-built stream paths.

    Scenario: StreamDb.list decodes existing stream names through the key schema; raw encoded-prefix filtering is intentionally out of scope
      Then StreamDb.list decodes existing stream names through the key schema; raw encoded-prefix filtering is intentionally out of scope.

    Scenario: Table identity is the schema-owned tableName plus schema-owned primary key, not caller-composed delimiter strings
      Then Table identity is the schema-owned tableName plus schema-owned primary key, not caller-composed delimiter strings.

  @unverified
  Rule: Opened instance ownership

    Scenario: One opened StreamDb instance serializes writes through its local lock and CAS tail
      Then One opened StreamDb instance serializes writes through its local lock and CAS tail.

    Scenario: Cross-process or multi-owner writes to the same stream are outside the StreamDb guarantee unless a higher-level owner/fencing protocol serializes them
      Then Cross-process or multi-owner writes to the same stream are outside the StreamDb guarantee unless a higher-level owner/fencing protocol serializes them.

  @unverified
  Rule: Production tracing

    Scenario: Public operations emit effect-s2-stream-db spans with enough attributes to identify the operation, stream, and table where applicable
      Then Public operations emit effect-s2-stream-db spans with enough attributes to identify the operation, stream, and table where applicable.

    Scenario: S2-backed behavior is proven by executable Cucumber specs over production spans; package-local tests remain pure and type-level
      Then S2-backed behavior is proven by executable Cucumber specs over production spans; package-local tests remain pure and type-level.

  @unverified
  Rule: COMPAT

    Scenario: All additions are backward compatible — existing open/get/query/transact/compact/drop behaviour is unchanged when the new options/primitives are not used
      Then All additions are backward compatible — existing open/get/query/transact/compact/drop behaviour is unchanged when the new options/primitives are not used.
